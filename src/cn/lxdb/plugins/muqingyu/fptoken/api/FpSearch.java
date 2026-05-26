package cn.lxdb.plugins.muqingyu.fptoken.api;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.index.TermsEnum$SeekStatus;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.offheap.OffheapPoolName;

import cn.lxdb.plugins.muqingyu.fptoken.config.Lucene80FPSearchConfig;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.block.FpGroupHotNgramBitIndex;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpBlockInfo;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpTokenTermLayout;

/**
 * 查询侧：按 ngram {@link BytesRef} 切片在组内 hot/common 位图中定位候选 term 序号，再
 * {@link Terms#iterator_fp()} seek 倒排并合并 doc，多切片 AND、同切片多组 OR。
 */
public class FpSearch {

	/**
	 * @param fpblock_list 段内 group_id → 位图区元数据
	 * @param slices       查询串滑窗切片（长度须在 {@link Lucene80FPSearchConfig#NGRAM_MIN}..{@link Lucene80FPSearchConfig#NGRAM_MAX}）
	 * @return 同时命中全部 slice 的 doc 集合（AND）；无切片或任一切片无命中则返回空集
	 */
	public FixedBitSet search(TreeMap<Integer, FpBlockInfo> fpblock_list, Terms terms, int maxDoc, BytesRef[] slices)
			throws IOException {
		final boolean[][] choose = new boolean[Lucene80FPSearchConfig.NGRAM_MAX][Lucene80FPSearchConfig.BUCKETS];
		for (int i = 0; i < choose.length; i++) {
			Arrays.fill(choose[i], false);
		}
		for (BytesRef slice : slices) {
			final int lenIdx = ngramLengthIndex(slice.length);
			final int bucket = FpGroupHotNgramBitIndex.bucketIndex(slice.bytes, slice.offset, slice.length);
			choose[lenIdx][bucket] = true;
		}

		final FixedBitSet rtn = new FixedBitSet(OffheapPoolName.fptokenbitset, maxDoc);
		if (slices == null || slices.length == 0) {
			return rtn;
		}

		final FixedBitSet[] collect = new FixedBitSet[slices.length];

		for (Entry<Integer, FpBlockInfo> e : fpblock_list.entrySet()) {
			final FpGroupHotNgramBitIndex bitsetIndex = terms.fpBits(Lucene80FPSearchConfig.DEFAULT_INDEX_ID, e.getKey(), choose, choose);
			for (int i = 0; i < slices.length; i++) {
				final BytesRef slice = slices[i];
				final int lenIdx = ngramLengthIndex(slice.length);
				final int bucket = FpGroupHotNgramBitIndex.bucketIndex(slice.bytes, slice.offset, slice.length);
				final FixedBitSet hot = bitsetIndex.banksHot[lenIdx][bucket];
				final FixedBitSet common = bitsetIndex.banksCommon[lenIdx][bucket];
				searchSliceInGroup(hot, common, slice, e.getValue(), e.getKey(), terms, maxDoc, collect, i);
			}
		}

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

	/** 单 group、单 slice：先 hot 位图候选，整库 hot 无 seek 命中再 common。 */
	private void searchSliceInGroup(FixedBitSet hot, FixedBitSet common, BytesRef slice, FpBlockInfo blkinfo, int groupid,
			Terms terms, int maxDoc, FixedBitSet[] collect, int sliceIndex) throws IOException {
		final FixedBitSet acc = ensureCollect(collect, sliceIndex, maxDoc);
		
		final boolean hotHit = searchBank_hot(hot, true, slice, blkinfo, groupid, terms, maxDoc, acc);
		if (!hotHit) {//如果词已经在热词里面了，就没必要在扫描common了
			searchBank_common(common, false, slice, blkinfo, groupid, terms, maxDoc, acc);
		}
	}

	private static FixedBitSet ensureCollect(FixedBitSet[] collect, int sliceIndex, int maxDoc) {
		if (collect[sliceIndex] == null) {
			collect[sliceIndex] = new FixedBitSet(OffheapPoolName.fptokenbitset, maxDoc);
		}
		return collect[sliceIndex];
	}

	/**
	 * 遍历 bank 中置位 bit（bit = termOrder - 1），构造 FP 词项 seek；命中则 OR 进 {@code collect}。
	 *
	 * @return 是否至少一次 seek 命中
	 */
	private boolean searchBank_hot(FixedBitSet bank, boolean hotMark, BytesRef slice, FpBlockInfo blkinfo, int groupid,
			Terms terms, int maxDoc, FixedBitSet collect) throws IOException {
		if (bank == null) {
			return false;
		}
		
		AtomicReference<PostingsEnum> reusePosting=new AtomicReference<PostingsEnum>(null);
		final TermsEnum termsEnum = terms.iterator();//这个检索就没必要用iterator_fp了
		final BytesRef reuse = new BytesRef(new byte[512]);
		boolean anyHit = false;
		
		int first=bank.nextSetBit(0);
		
		if(first==DocIdSetIterator.NO_MORE_DOCS)
		{
			return false;
		}
		AtomicInteger merger_level=new AtomicInteger(0);
		{//先读默认，必须完全相等

			final int termIndex = first + 1;
			
			int status=seekTermAndOrDocs(reusePosting,merger_level,true,termsEnum, reuse, Lucene80FPSearchConfig.DEFAULT_INDEX_ID, groupid, (byte) blkinfo.targetLevel, hotMark, termIndex,
					false, slice, maxDoc, collect);
			if (seek_status_ok==status) {
				anyHit = true;
			}
			
			status=seekTermAndOrDocs(reusePosting,merger_level,true,termsEnum, reuse, Lucene80FPSearchConfig.DEFAULT_INDEX_ID, groupid, (byte) blkinfo.targetLevel, hotMark, termIndex,
					true, slice, maxDoc, collect);
			if (seek_status_ok==status) {
				anyHit = true;
			}
		
		}
		
		//在读其他level，必须是不能超过max_level
		for (int bit = bank.nextSetBit(first+1); ; bit = bank.nextSetBit(bit + 1)) {
			if(bit==DocIdSetIterator.NO_MORE_DOCS)
			{
				break;
			}
			final int termIndex = bit + 1;
			int status=seekTermAndOrDocs(reusePosting,merger_level,false,termsEnum, reuse, Lucene80FPSearchConfig.DEFAULT_INDEX_ID, groupid, (byte) blkinfo.targetLevel, hotMark, termIndex,
					false, slice, maxDoc, collect);
			
			if (seek_status_break==status) {
				break;
			}
		}
		return anyHit;
	}
	
	
	private boolean searchBank_common(FixedBitSet bank, boolean hotMark, BytesRef slice, FpBlockInfo blkinfo, int groupid,
			Terms terms, int maxDoc, FixedBitSet collect) throws IOException {
		if (bank == null) {
			return false;
		}
		
		AtomicReference<PostingsEnum> reusePosting=new AtomicReference<PostingsEnum>(null);

		final TermsEnum termsEnum = terms.iterator();//这个检索就没必要用iterator_fp了
		final BytesRef reuse = new BytesRef(new byte[512]);
		boolean anyHit = false;
		AtomicInteger merger_level=new AtomicInteger(0);

		//在读其他level，必须是不能超过max_level
		for (int bit = bank.nextSetBit(0); ; bit = bank.nextSetBit(bit + 1)) {
			if(bit==DocIdSetIterator.NO_MORE_DOCS)
			{
				break;
			}
			final int termIndex = bit + 1;
			int status=seekTermAndOrDocs(reusePosting,merger_level,false,termsEnum, reuse, Lucene80FPSearchConfig.DEFAULT_INDEX_ID, groupid, (byte) blkinfo.targetLevel, hotMark, termIndex,
					false, slice, maxDoc, collect);
			if (seek_status_ok==status) {
				anyHit = true;
			}
		}
		return anyHit;
	}
	private static int seek_status_miss=2;
	private static int seek_status_break=1;
	private static int seek_status_ok=0;

	private static int seekTermAndOrDocs(AtomicReference<PostingsEnum> reuseposting,AtomicInteger merger_level,boolean must_equals,TermsEnum termsEnum, BytesRef reuse, short indexId, int groupid, byte groupLevel,
			boolean hotMark, int termIndex, boolean isDelTerm, BytesRef slice, int maxDoc, FixedBitSet collect)
			throws IOException {
		FpTokenTermLayout.make_fp_search_prefix(reuse, indexId, groupid, groupLevel, hotMark, termIndex, isDelTerm);
		if (termsEnum.seekCeil(reuse) == TermsEnum$SeekStatus.END) {
			return seek_status_miss;
		}
		final BytesRef found = termsEnum.term();
		
		//先匹配是否是满足条件的termIndex，如果不存在这个termIndex是有问题的
		if (!termHeaderMatches(found, indexId, groupid, groupLevel, hotMark, termIndex, isDelTerm)) {
			return seek_status_miss;
		}
		BytesRef rawTerms=FpTokenTermLayout.removeHeaderBytes(found);
		if (!payloadContains(must_equals,rawTerms, slice)) {
			return seek_status_miss;
		}
		
		if(hotMark)
		{
			if(must_equals)
			{
				merger_level.set(FpTokenTermLayout.readHotTermScanLevel(found)+rawTerms.length);;
			}
			
			if(merger_level.get()<rawTerms.length)
			{
				return seek_status_break;
			}
		}
		
		orPostingsInto(reuseposting,termsEnum, maxDoc, collect);
		return seek_status_ok;
	}

	

	private static boolean termHeaderMatches(BytesRef found, short indexId, int groupid, byte groupLevel, boolean hotMark,
			int termIndex, boolean isDelTerm) {
		if (found.length < FpTokenTermLayout.FP_HEADER_BYTES) {
			return false;
		}
		return  FpTokenTermLayout.read_group_id(found) == groupid
				&& FpTokenTermLayout.readLevel(found) == (groupLevel & 0xFF)
				&& FpTokenTermLayout.isHotTerm(found) == hotMark
				&& FpTokenTermLayout.readTermIndex(found) == termIndex
				&& FpTokenTermLayout.readIsDelTerm(found) == isDelTerm;
	}

	private static boolean payloadContains(boolean must_equals,BytesRef payload, BytesRef slice) {
		
		final int plen = payload.length;
		final int slen = slice.length;
		if (plen < slen) {
			return false;
		}
		if(must_equals||plen == slen)
		{
			return payload.equals(slice);
		}
		
		
		//这里是contains的实现
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

	private static void orPostingsInto(AtomicReference<PostingsEnum> reuse,  TermsEnum termsEnum, int maxDoc, FixedBitSet collect) throws IOException {
		final PostingsEnum pe = termsEnum.postings(reuse.get(), PostingsEnum.NONE);
		if (pe == null) {
			return;
		}
		
		reuse.set(pe);
		int doc;
		while ((doc = pe.nextDoc()) != PostingsEnum.NO_MORE_DOCS) {
			if (doc >= 0 && doc < maxDoc) {
				collect.set(doc);
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
