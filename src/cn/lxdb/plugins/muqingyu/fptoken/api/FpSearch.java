package cn.lxdb.plugins.muqingyu.fptoken.api;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.index.TermsEnum$SeekStatus;
import org.apache.lucene.search.ConjunctionDISI;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.offheap.OffheapPoolName;
import org.slf4j.Logger;

import cn.lucene.lxdb.params.LxdbLogerEncrypt;
import cn.lxdb.plugins.muqingyu.fptoken.config.FpTokenBlockLevelPolicy;
import cn.lxdb.plugins.muqingyu.fptoken.config.Lucene80FPSearchConfig;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.block.FpGroupHotNgramBitIndex;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpBlockInfo;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpSearchStat;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpTokenTermLayout;

/**
 * 查询侧：按 ngram {@link BytesRef} 切片在组内 hot/common 位图中定位候选 term 序号，再
 * {@link Terms#iterator_fp()} seek 倒排并合并 doc，多切片 AND、同切片多组 OR。
 * <p>
 * 每个查询 slice 会对自身及若干更短连续子串（见 {@link Lucene80FPSearchConfig#SEARCH_BITSET_PROBE_LAYERS_DOWN}）
 * 对应位图做 AND，再 seek，以降低单桶误命中率。
 */
public class FpSearch {

	public FpSearchStat stat;
	public FpSearch(FpSearchStat stat) {
		this.stat = stat;
	}

	public static final Logger LOG = LxdbLogerEncrypt.getLogger("mqy.fptoken");
	public String DEBUG_UUID=Lucene80FPSearchConfig.PRINT_DEBUG?java.util.UUID.randomUUID().toString():"";

	private void print_debug(Terms terms) throws IOException {
		if (!Lucene80FPSearchConfig.PRINT_DEBUG) {
			return;
		}
		final AtomicReference<PostingsEnum> docsEnum = new AtomicReference<PostingsEnum>(null);

		TermsEnum termsEnum = terms.iterator();
		BytesRef term = termsEnum.term();
		int termIndex = 1;
		while (term != null) {
			try {
			
				PostingsEnum pe = termsEnum.postings(docsEnum.get(), PostingsEnum.NONE);

				LOG.info(DEBUG_UUID+" termIndex:" + termIndex + " cost:" + pe.cost() + " info:" +FpTokenTermLayout.toReadableString(term));
			} finally {

				termIndex++;
				term = termsEnum.next();
			}
		}
	}

	/**
	 * @param fpblock_list 段内 group_id → 位图区元数据
	 * @param slices       查询串滑窗切片（长度须在 {@link Lucene80FPSearchConfig#NGRAM_MIN}..{@link Lucene80FPSearchConfig#NGRAM_MAX}）
	 * @return 同时命中全部 slice 的 doc 集合（AND）；无切片或任一切片无命中则返回空集
	 */
	public FixedBitSet search( TreeMap<Integer, FpBlockInfo> fpblock_list, Terms terms, int maxDoc, BytesRef columnName,
			BytesRef[] slices) throws IOException {

		print_debug(terms);
		stat.doccount+=maxDoc;
		final boolean[][] choose = new boolean[Lucene80FPSearchConfig.NGRAM_MAX][Lucene80FPSearchConfig.BUCKETS];
		for (int i = 0; i < choose.length; i++) {
			Arrays.fill(choose[i], false);
		}
		if (slices != null) {
			for (BytesRef slice : slices) {
				markChooseForSliceAndProbes(choose, slice);
			}
		}

		final FixedBitSet rtn = new FixedBitSet(OffheapPoolName.fptokenbitset, maxDoc);
		if (slices == null || slices.length == 0) {
			return rtn;
		}

		final FixedBitSet[] collect = new FixedBitSet[slices.length];

		int maxGroupId = -1;

		for (Entry<Integer, FpBlockInfo> e : fpblock_list.entrySet()) {
			final FpBlockInfo blkinfo = e.getValue();
			if (!fieldInfoMatchesColumn(blkinfo, columnName)) {
				continue;
			}
			
			stat.blkCount[blkinfo.targetLevel]+=slices.length;
			final int groupId = e.getKey().intValue();
			if (groupId > maxGroupId) {
				maxGroupId = groupId;
			}

			final FpGroupHotNgramBitIndex bitsetIndex = terms.fpBits(Lucene80FPSearchConfig.DEFAULT_INDEX_ID, groupId,
					choose, choose);
			if (bitsetIndex == null) {
				continue;
			}

			for (int i = 0; i < slices.length; i++) {
				final BytesRef slice = slices[i];
//				final BytesRef[] probes = buildProbeSlices(slice);
				final FixedBitSet[] hotBanks = resolveBanks(bitsetIndex.banksHot, slice);
				final FixedBitSet[] commonBanks = resolveBanks(bitsetIndex.banksCommon, slice);
				
				searchSliceInGroup(hotBanks, commonBanks, columnName, slice, blkinfo, groupId, terms, maxDoc, collect,i);
			}
		}

		searchSparseNoBitIndexTerms(terms, columnName, maxDoc, slices, collect, maxGroupId);

		boolean merged = false;
		for (int i = 0; i < slices.length; i++) {
			if (collect[i] == null) {
				rtn.clear(0, maxDoc);
				return rtn;
			}
			if (!merged) {
				rtn.or(collect[i]);
				merged = true;
			} else {
				rtn.and(collect[i]);
			}
		}
		return rtn;
	}

	/**
	 * 为 slice 及其向下 {@link Lucene80FPSearchConfig#SEARCH_BITSET_PROBE_LAYERS_DOWN} 档的连续子串标记需加载的位图桶。
	 * 例：{@code abcd} → {@code abcd}、{@code abc}、{@code bcd}。
	 */
	public static void markChooseForSliceAndProbes(boolean[][] choose, BytesRef slice) {

		final int lenIdx = ngramLengthIndex(slice.length);
		final int buckets[] = FpGroupHotNgramBitIndex.bucketIndex(slice.bytes, slice.offset, slice.length);
		for(int bucket:buckets)
		{
			choose[lenIdx][bucket] = true;

		}
	
	}

//	/**
//	 * 探针序列：锚点 slice + 各档更短连续子串（去重，保持锚点在前）。
//	 */
//	public static BytesRef[] buildProbeSlices(BytesRef slice) {
//		final int layersDown = Lucene80FPSearchConfig.SEARCH_BITSET_PROBE_LAYERS_DOWN;
//		final List<BytesRef> probes = new ArrayList<>(Math.max(2, slice.length * layersDown));
//		probes.add(slice);
//		for (int down = 1; down <= layersDown; down++) {
//			final int len = slice.length - down;
//			if (len < Lucene80FPSearchConfig.NGRAM_MIN) {
//				break;
//			}
//			for (int start = 0; start + len <= slice.length; start++) {
//				if (len == slice.length && start == 0) {
//					continue;
//				}
//				final BytesRef sub = new BytesRef(slice.bytes, slice.offset + start, len);
//				if (!containsProbe(probes, sub)) {
//					probes.add(sub);
//				}
//			}
//		}
//		return probes.toArray(new BytesRef[0]);
//	}

	private static boolean containsProbe(List<BytesRef> probes, BytesRef candidate) {
		for (BytesRef p : probes) {
			if (p.equals(candidate)) {
				return true;
			}
		}
		return false;
	}

	private static boolean fieldInfoMatchesColumn(FpBlockInfo blkinfo, BytesRef columnName) {
		if (blkinfo == null || blkinfo.fieldInfo == null) {
			return false;
		}
		return blkinfo.fieldInfo.equals(columnName);
	}

	/**
	 * 稀疏列（写段时 common 词数 ≤ {@link FpTokenBlockLevelPolicy#NO_INDEX_THRESHOLD}、无 ngram 位图）：
	 * 从 {@code (column, index_id, group_id, level=0, …)} seek 起扫描，校验头字段后按 payload 子串命中 OR 进各 slice 的 doc 集。
	 */
	private void searchSparseNoBitIndexTerms(Terms terms, BytesRef columnName, int maxDoc, BytesRef[] slices,
			FixedBitSet[] collect, int maxGroupId) throws IOException {
		final short indexId = Lucene80FPSearchConfig.DEFAULT_INDEX_ID;
		final AtomicReference<PostingsEnum> reusePosting = new AtomicReference<PostingsEnum>(null);
		final TermsEnum termsEnum = terms.iterator();
		final BytesRef reuse = new BytesRef(new byte[512]);

		// 稀疏列 term 的 group_level=0，按列+index_id+group_id 字典序分散；从 group_id=0 起扫，勿用 maxGroupId 作下界。
		FpTokenTermLayout.make_fp_search_prefix(reuse, columnName, indexId, maxGroupId+1,
				(byte) FpTokenBlockLevelPolicy.BLOCK_LEVEL_NOGROUP, false, 0);
		if (termsEnum.seekCeil(reuse) == TermsEnum$SeekStatus.END) {
			stat.termMiss0++;
			return;
		}

		if (Lucene80FPSearchConfig.PRINT_DEBUG) {
			LOG.info(DEBUG_UUID+"sparse scan column=" + columnName.utf8ToString() + " maxGroupId=" + maxGroupId);
		}

		for (BytesRef found = termsEnum.term(); found != null; found = termsEnum.next()) {
			if (!FpTokenTermLayout.columnNameEquals(found, columnName)) {
				
				break;
			}
			
			final int level = FpTokenTermLayout.readLevel(found);
			if (level != FpTokenBlockLevelPolicy.BLOCK_LEVEL_NOGROUP) {
				continue;
			}
			
			

			final BytesRef payload = FpTokenTermLayout.removeColumnAndHeaderBytes(found);
			for (int i = 0; i < slices.length; i++) {
				if (!payloadContainsSlice(payload, slices[i])) {
					stat.termMiss0++;
					continue;
				}
				stat.termHit0++;
				final FixedBitSet acc = ensureCollect(collect, i, maxDoc);
				orPostingsInto(reusePosting, termsEnum, maxDoc, acc,false);
			}
		}
	}

	/** payload 是否包含 slice 连续子串（indexOf 语义）。 */
	private static boolean payloadContainsSlice(BytesRef payload, BytesRef slice) {
		return payloadMatchesSlice(false, payload, slice);
	}

	private static FixedBitSet[] resolveBanks(FixedBitSet[][] banksGrid, BytesRef probes) {
		final int[] buckets = FpGroupHotNgramBitIndex.bucketIndex(probes.bytes, probes.offset, probes.length);
		final FixedBitSet[] banks = new FixedBitSet[buckets.length];
		final int lenIdx = ngramLengthIndex(probes.length);

		for(int i=0;i<banks.length;i++)
		{
			banks[i] = banksGrid[lenIdx][buckets[i]];

		}
		
		return banks;
	}

	/** 单 group、单 slice：先 hot 位图候选，整库 hot 无 seek 命中再 common。 */
	private void searchSliceInGroup(FixedBitSet[] hotBanks, FixedBitSet[] commonBanks, BytesRef columnName,
			BytesRef anchorSlice, FpBlockInfo blkinfo, int groupid, Terms terms, int maxDoc, FixedBitSet[] collect,
			int sliceIndex) throws IOException {
		final FixedBitSet acc = ensureCollect(collect, sliceIndex, maxDoc);

		final boolean hotHit = searchBankHot(hotBanks, true, columnName, anchorSlice, blkinfo, groupid, terms, maxDoc,acc);
		if(Lucene80FPSearchConfig.PRINT_DEBUG)
		{
			LOG.info(DEBUG_UUID+" searchBankHot " +hotHit+" "+anchorSlice.utf8ToString());

		}

		if (!hotHit) {
			boolean common_hit=searchBankCommon(commonBanks, false, columnName, anchorSlice, blkinfo, groupid, terms, maxDoc, acc);
			if(Lucene80FPSearchConfig.PRINT_DEBUG)
			{
				LOG.info(DEBUG_UUID+" searchBankCommon " +common_hit+" "+anchorSlice.utf8ToString());
			}

		}
	}

	private static FixedBitSet ensureCollect(FixedBitSet[] collect, int sliceIndex, int maxDoc) {
		if (collect[sliceIndex] == null) {
			collect[sliceIndex] = new FixedBitSet(OffheapPoolName.fptokenbitset, maxDoc);
		}
		return collect[sliceIndex];
	}

	/**
	 * 遍历多桶位图 AND 后的候选 bit（bit = termOrder - 1），构造 FP 词项 seek；命中则 OR 进 {@code collect}。
	 *
	 * @return 是否至少一次 seek 命中
	 */
	private boolean searchBankHot(FixedBitSet[] banks, boolean hotMark, BytesRef columnName, BytesRef anchorSlice,
			FpBlockInfo blkinfo, int groupid, Terms terms, int maxDoc, FixedBitSet collect) throws IOException {
		final DocIdSetIterator bits = intersectBankIterators(banks);
		if (bits == null) {
			return false;
		}

		final AtomicReference<PostingsEnum> reusePosting = new AtomicReference<PostingsEnum>(null);
		final TermsEnum termsEnum = terms.iterator();
		final BytesRef reuse = new BytesRef(new byte[512]);
		boolean anyHit = false;
		final AtomicInteger maxHotPayloadLen = new AtomicInteger(0);
		boolean payloadLenCapSet = false;

		boolean first=false;
		for (int bit = bits.nextDoc(); bit != DocIdSetIterator.NO_MORE_DOCS; bit = bits.nextDoc()) {
			//遍历每个term
			final int termIndex = bit;
			stat.bitHitHot[blkinfo.targetLevel]++;
			if(!first)
			{
				stat.blkHitHot[blkinfo.targetLevel]++;
				first=true;
			}
			
			if (!payloadLenCapSet) {
				int status = seekTermAndOrDocs(reusePosting, maxHotPayloadLen, true, termsEnum, reuse,
						Lucene80FPSearchConfig.DEFAULT_INDEX_ID, groupid, (byte) blkinfo.targetLevel, hotMark,
						termIndex, columnName, anchorSlice, maxDoc, collect);
				if (status == SEEK_OK) {
					anyHit = true;
					payloadLenCapSet = true;
				}
				continue;
			}
			final int status = seekTermAndOrDocs(reusePosting, maxHotPayloadLen, false, termsEnum, reuse,
					Lucene80FPSearchConfig.DEFAULT_INDEX_ID, groupid, (byte) blkinfo.targetLevel, hotMark, termIndex,
					columnName, anchorSlice, maxDoc, collect);
			if (status == SEEK_OK) {
				anyHit = true;
			}
			if (status == SEEK_BREAK_PAYLOAD_LEN) {
				break;
			}
		}
		return anyHit;
	}

	private boolean searchBankCommon(FixedBitSet[] banks, boolean hotMark, BytesRef columnName, BytesRef anchorSlice,
			FpBlockInfo blkinfo, int groupid, Terms terms, int maxDoc, FixedBitSet collect) throws IOException {
		final DocIdSetIterator bits = intersectBankIterators(banks);
		if (bits == null) {
			if(Lucene80FPSearchConfig.PRINT_DEBUG)
			{
			LOG.info(DEBUG_UUID+" searchBankCommon bits == null "+anchorSlice.utf8ToString());
			}
			return false;
		}

		final AtomicReference<PostingsEnum> reusePosting = new AtomicReference<PostingsEnum>(null);
		final TermsEnum termsEnum = terms.iterator();
		final BytesRef reuse = new BytesRef(new byte[512]);
		boolean anyHit = false;
		boolean first=false;

		for (int bit = bits.nextDoc(); bit != DocIdSetIterator.NO_MORE_DOCS; bit = bits.nextDoc()) {
			final int termIndex = bit;
			
			stat.bitHitCommon[blkinfo.targetLevel]++;
			if(!first)
			{
				stat.blkHitCommon[blkinfo.targetLevel]++;
				first=true;
			}

			final int status = seekTermAndOrDocs(reusePosting, null, false, termsEnum, reuse,
					Lucene80FPSearchConfig.DEFAULT_INDEX_ID, groupid, (byte) blkinfo.targetLevel, hotMark, termIndex,
					columnName, anchorSlice, maxDoc, collect);
			if(Lucene80FPSearchConfig.PRINT_DEBUG)
			{
				LOG.info(DEBUG_UUID+" searchBankCommon termIndex = "+termIndex+" status="+status+" "+anchorSlice.utf8ToString());
			}
			if (status == SEEK_OK) {
				anyHit = true;
			}
		}
		return anyHit;
	}

	/** 各探针桶位图 term 序号 AND；任一桶缺失则无可选候选。 */
	private static DocIdSetIterator intersectBankIterators(FixedBitSet[] banks) {
		if (banks == null || banks.length == 0) {
			return null;
		}
		if (banks.length == 1) {
			return banks[0] == null ? null : new BitSetIterator(banks[0], 0L);
		}
		final List<DocIdSetIterator> iters = new ArrayList<>(banks.length);
		for (FixedBitSet bank : banks) {
			if (bank == null) {
				return null;
			}
			iters.add(new BitSetIterator(bank, 0L));
		}
		return ConjunctionDISI.intersectIterators(iters);
	}

	private static final int SEEK_OK = 0;
	private static final int SEEK_BREAK_PAYLOAD_LEN = 1;
	private static final int SEEK_MISS = 2;

	private  int seekTermAndOrDocs(AtomicReference<PostingsEnum> reusePosting,
			AtomicInteger maxHotPayloadLenOrNull, boolean requireExactPayloadMatch, TermsEnum termsEnum, BytesRef reuse,
			short indexId, int groupid, byte groupLevel, boolean hotMark, int termIndex, BytesRef columnName,
			BytesRef slice, int maxDoc, FixedBitSet collect) throws IOException {
		FpTokenTermLayout.make_fp_search_prefix(reuse, columnName, indexId, groupid, groupLevel, hotMark, termIndex);

		if (Lucene80FPSearchConfig.PRINT_DEBUG) {
			LOG.info(DEBUG_UUID+" seekCeil indexId:" + indexId + " " + groupid + " " + groupLevel + " " + hotMark + " " + termIndex
					+ " " + slice.utf8ToString());
		}
		if (termsEnum.seekCeil(reuse) == TermsEnum$SeekStatus.END) {
			if(hotMark)
			{
				stat.termMissHot1[groupLevel]++;
			}else {
				stat.termMissCommon1[groupLevel]++;
			}
			if (Lucene80FPSearchConfig.PRINT_DEBUG) {
				LOG.info(DEBUG_UUID+" seekCeil SEEK_MISS indexId:" + indexId + " " + groupid + " " + groupLevel + " " + hotMark + " " + termIndex
						+ " " + slice.utf8ToString());
			}
			return SEEK_MISS;
		}
		final BytesRef found = termsEnum.term();
		boolean isDelTerm = FpTokenTermLayout.readIsDelTerm(found);
		if (Lucene80FPSearchConfig.PRINT_DEBUG) {
			LOG.info(DEBUG_UUID+"found indexId:" + indexId + " " + groupid + " " + groupLevel + " " + hotMark + " " + termIndex
					+ " " + slice.utf8ToString()+" info:"+FpTokenTermLayout.toReadableString(found));
		}
		if (!termHeaderMatches(found, columnName, groupid, groupLevel, hotMark, termIndex)) {
			if(hotMark)
			{
				stat.termMissHot2[groupLevel]++;
			}else {
				stat.termMissCommon2[groupLevel]++;
			}
			return SEEK_MISS;
		}
		final BytesRef payload = FpTokenTermLayout.removeColumnAndHeaderBytes(found);
		if (!payloadMatchesSlice(requireExactPayloadMatch, payload, slice)) {
			if(hotMark)
			{
				stat.termMissHot3[groupLevel]++;
			}else {
				stat.termMissCommon3[groupLevel]++;
			}
			return SEEK_MISS;
		}

		if (hotMark && maxHotPayloadLenOrNull != null) {
			if (requireExactPayloadMatch) {
				maxHotPayloadLenOrNull.set(FpTokenTermLayout.maxHotPayloadLenFromHeader(found, payload.length));
			}
			if (maxHotPayloadLenOrNull.get() < payload.length) {
				return SEEK_BREAK_PAYLOAD_LEN;
			}
		}

		if (!isDelTerm) {
			orPostingsInto(reusePosting, termsEnum, maxDoc, collect,hotMark);
		}
		
		if(hotMark)
		{
			stat.termHitHot[groupLevel]++;
		}else {
			stat.termHitCommon[groupLevel]++;
		}
		return SEEK_OK;
	}

	private static boolean termHeaderMatches(BytesRef found, BytesRef columnName, int groupid, byte groupLevel,
			boolean hotMark, int termIndex) {
		if (found.length <= 0) {
			return false;
		}
		return FpTokenTermLayout.columnNameEquals(found, columnName)
				&& FpTokenTermLayout.read_group_id(found) == groupid
				&& FpTokenTermLayout.readLevel(found) == (groupLevel & 0xFF)
				&& FpTokenTermLayout.isHotTerm(found) == hotMark
				&& FpTokenTermLayout.readTermIndex(found) == termIndex;
	}

	private static boolean payloadMatchesSlice(boolean requireExactPayloadMatch, BytesRef payload, BytesRef slice) {
		final int plen = payload.length;
		final int slen = slice.length;
		if (plen < slen) {
			return false;
		}
		if (requireExactPayloadMatch || plen == slen) {
			return payload.equals(slice);
		}

		final byte[] pb = payload.bytes;
		final int po = payload.offset;
		final byte[] sb = slice.bytes;
		final int so = slice.offset;

		for (int i = 0; i <= plen - slen; i++) {
			boolean match = true;
			for (int j = 0; j < slen; j++) {
				if (pb[po + i + j] != sb[so + j]) {
					match = false;
					break;
				}
			}
			if (match) {
				return true;
			}
		}
		return false;
	}

	private  void orPostingsInto(AtomicReference<PostingsEnum> reuse, TermsEnum termsEnum, int maxDoc,
			FixedBitSet collect,boolean hotMark) throws IOException {
		final PostingsEnum pe = termsEnum.postings(reuse.get(), PostingsEnum.NONE);
		if (pe == null) {
			return;
		}

		reuse.set(pe);
		int doc;
		while ((doc = pe.nextDoc()) != PostingsEnum.NO_MORE_DOCS) {
			if (doc >= 0 && doc < maxDoc) {
				collect.set(doc);
				if(hotMark)
				{
					stat.hothit++;
				}else {
					stat.commonhit++;
				}
			}
		}
	}

	private static int ngramLengthIndex(int sliceLength) {
		if (sliceLength < Lucene80FPSearchConfig.NGRAM_MIN || sliceLength > Lucene80FPSearchConfig.NGRAM_MAX) {
			throw new IllegalArgumentException("slice length out of range: " + sliceLength);
		}
		return sliceLength - 1;
	}
}
