package cn.lxdb.plugins.muqingyu.fptoken.api;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.index.TermsEnum$SeekStatus;
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
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.Utils;

/**
 * 查询侧：按 ngram {@link BytesRef} 切片在组内 hot/common 精确 bucketIndex 倒排中定位 order 列表，再
 * {@link Terms#iterator_fp()} seek 倒排并合并 doc，多切片 AND、同切片多组 OR。
 */
public class FpSearch {

	public FpSearchStat stat;
	public FpSearch(FpSearchStat stat) {
		this.stat = stat;
	}

	public static final Logger LOG = LxdbLogerEncrypt.getLogger("mqy.fptoken");
	public String DEBUG_UUID=Lucene80FPSearchConfig.PRINT_DEBUG?java.util.UUID.randomUUID().toString():"";



	/**
	 * @param fpblock_list 段内 group_id → 位图区元数据
	 * @param slices       查询串滑窗切片（长度须在 {@link Lucene80FPSearchConfig#NGRAM_MIN}..{@link Lucene80FPSearchConfig#NGRAM_MAX}）
	 * @return 同时命中全部 slice 的 doc 集合（AND）；无切片或任一切片无命中则返回空集
	 */
	public FixedBitSet search( TreeMap<Integer, FpBlockInfo> fpblock_list, Terms terms, int maxDoc, BytesRef columnName,
			BytesRef[] slices) throws IOException {

		stat.doccount+=maxDoc;
		final FixedBitSet rtn = new FixedBitSet(OffheapPoolName.fptokenbitset, maxDoc);
		if (slices == null || slices.length == 0) {
			return rtn;
		}

		final FixedBitSet[] collect = new FixedBitSet[slices.length];
		int maxGroupId = -1;
		final long[] bucketKeys = FpGroupHotNgramBitIndex.selectiveKeysForSlices(slices);

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
					bucketKeys, bucketKeys);
			if (bitsetIndex == null) {
				continue;
			}

			for (int i = 0; i < slices.length; i++) {
				searchSliceInGroup(bitsetIndex, columnName, slices[i], blkinfo, groupId, terms, maxDoc, collect, i);
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

	private void searchSliceInGroup(FpGroupHotNgramBitIndex bitIndex, BytesRef columnName, BytesRef anchorSlice,
			FpBlockInfo blkinfo, int groupid, Terms terms, int maxDoc, FixedBitSet[] collect, int sliceIndex)
			throws IOException {
		final FixedBitSet acc = ensureCollect(collect, sliceIndex, maxDoc);
		final boolean hotHit = searchOrdersHot(bitIndex, columnName, anchorSlice, blkinfo, groupid, terms, maxDoc, acc);
		if (Lucene80FPSearchConfig.PRINT_DEBUG) {
			LOG.info(DEBUG_UUID + "[fp_search] searchOrdersHot hit=" + hotHit + " slice="
					+ Utils.BytesReftoString(anchorSlice));
		}
		if (!hotHit) {
			final boolean commonHit = searchOrdersCommon(bitIndex, columnName, anchorSlice, blkinfo, groupid, terms,
					maxDoc, acc);
			if (Lucene80FPSearchConfig.PRINT_DEBUG) {
				LOG.info(DEBUG_UUID + "[fp_search] searchOrdersCommon hit=" + commonHit + " slice="
						+ Utils.BytesReftoString(anchorSlice));
			}
		}
	}

	private static FixedBitSet ensureCollect(FixedBitSet[] collect, int sliceIndex, int maxDoc) {
		if (collect[sliceIndex] == null) {
			collect[sliceIndex] = new FixedBitSet(OffheapPoolName.fptokenbitset, maxDoc);
		}
		return collect[sliceIndex];
	}

	private boolean searchOrdersHot(FpGroupHotNgramBitIndex bitIndex, BytesRef columnName, BytesRef anchorSlice,
			FpBlockInfo blkinfo, int groupid, Terms terms, int maxDoc, FixedBitSet collect) throws IOException {
		return seekOrderList(bitIndex.lookupHotOrders(anchorSlice), true, columnName, anchorSlice, blkinfo, groupid,
				terms, maxDoc, collect);
	}

	private boolean searchOrdersCommon(FpGroupHotNgramBitIndex bitIndex, BytesRef columnName, BytesRef anchorSlice,
			FpBlockInfo blkinfo, int groupid, Terms terms, int maxDoc, FixedBitSet collect) throws IOException {
		return seekOrderList(bitIndex.lookupCommonOrders(anchorSlice), false, columnName, anchorSlice, blkinfo,
				groupid, terms, maxDoc, collect);
	}

	private boolean seekOrderList(int[] orders, boolean hotMark, BytesRef columnName, BytesRef anchorSlice,
			FpBlockInfo blkinfo, int groupid, Terms terms, int maxDoc, FixedBitSet collect) throws IOException {
		if (orders == null || orders.length == 0) {
			return false;
		}
		final AtomicReference<PostingsEnum> reusePosting = new AtomicReference<PostingsEnum>(null);
		final TermsEnum termsEnum = terms.iterator();
		final BytesRef reuse = new BytesRef(new byte[512]);
		boolean anyHit = false;
		final AtomicInteger maxHotPayloadLen = new AtomicInteger(0);
		boolean payloadLenCapSet = false;
		boolean first = false;
		for (int termIndex : orders) {
			if (hotMark) {
				stat.bitHitHot[blkinfo.targetLevel]++;
			} else {
				stat.bitHitCommon[blkinfo.targetLevel]++;
			}
			if (!first) {
				if (hotMark) {
					stat.blkHitHot[blkinfo.targetLevel]++;
				} else {
					stat.blkHitCommon[blkinfo.targetLevel]++;
				}
				first = true;
			}
			if (hotMark && !payloadLenCapSet) {
				final int status = seekTermAndOrDocs(reusePosting, maxHotPayloadLen, true, termsEnum, reuse,
						Lucene80FPSearchConfig.DEFAULT_INDEX_ID, groupid, (byte) blkinfo.targetLevel, hotMark,
						termIndex, columnName, anchorSlice, maxDoc, collect);
				if (status == SEEK_OK) {
					anyHit = true;
					payloadLenCapSet = true;
				}
				continue;
			}
			final int status = seekTermAndOrDocs(reusePosting, hotMark ? maxHotPayloadLen : null, false, termsEnum,
					reuse, Lucene80FPSearchConfig.DEFAULT_INDEX_ID, groupid, (byte) blkinfo.targetLevel, hotMark,
					termIndex, columnName, anchorSlice, maxDoc, collect);
			if (status == SEEK_OK) {
				anyHit = true;
			}
			if (status == SEEK_BREAK_PAYLOAD_LEN) {
				break;
			}
		}
		return anyHit;
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
			LOG.info(DEBUG_UUID + "[fp_search] sparseScan column=" + Utils.BytesReftoString(columnName) + " maxGroupId="
					+ maxGroupId);
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

	private static final int SEEK_OK = 0;
	private static final int SEEK_BREAK_PAYLOAD_LEN = 1;
	private static final int SEEK_MISS = 2;

	private static String seekStatusLabel(int status) {
		if (status == SEEK_OK) {
			return "OK";
		}
		if (status == SEEK_BREAK_PAYLOAD_LEN) {
			return "BREAK_PAYLOAD_LEN";
		}
		if (status == SEEK_MISS) {
			return "MISS";
		}
		return String.valueOf(status);
	}

	private  int seekTermAndOrDocs(AtomicReference<PostingsEnum> reusePosting,
			AtomicInteger maxHotPayloadLenOrNull, boolean requireExactPayloadMatch, TermsEnum termsEnum, BytesRef reuse,
			short indexId, int groupid, byte groupLevel, boolean hotMark, int termIndex, BytesRef columnName,
			BytesRef slice, int maxDoc, FixedBitSet collect) throws IOException {
		FpTokenTermLayout.make_fp_search_prefix(reuse, columnName, indexId, groupid, groupLevel, hotMark, termIndex);

		if (Lucene80FPSearchConfig.PRINT_DEBUG) {
			LOG.info(DEBUG_UUID + "[fp_search] seekCeil prefix indexId=" + indexId + " groupId=" + groupid
					+ " groupLevel=L" + groupLevel + " hot=" + hotMark + " termIndex=" + termIndex + " slice="
					+ Utils.BytesReftoString(slice));
		}
		if (termsEnum.seekCeil(reuse) == TermsEnum$SeekStatus.END) {
			if(hotMark)
			{
				stat.termMissHot1[groupLevel]++;
			}else {
				stat.termMissCommon1[groupLevel]++;
			}
			if (Lucene80FPSearchConfig.PRINT_DEBUG) {
				LOG.info(DEBUG_UUID + "[fp_search] seekCeil miss indexId=" + indexId + " groupId=" + groupid
						+ " groupLevel=L" + groupLevel + " hot=" + hotMark + " termIndex=" + termIndex + " slice="
						+ Utils.BytesReftoString(slice));
			}
			return SEEK_MISS;
		}
		final BytesRef found = termsEnum.term();
		boolean isDelTerm = FpTokenTermLayout.readIsDelTerm(found);
		if (Lucene80FPSearchConfig.PRINT_DEBUG) {
			LOG.info(DEBUG_UUID + "[fp_search] seekCeil found indexId=" + indexId + " groupId=" + groupid
					+ " groupLevel=L" + groupLevel + " hot=" + hotMark + " termIndex=" + termIndex + " slice="
					+ Utils.BytesReftoString(slice) + " term=" + FpTokenTermLayout.toReadableString(found));
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
