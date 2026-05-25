package cn.lxdb.plugins.muqingyu.fptoken.api;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.NumericUtils;
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

	private static final short DEFAULT_INDEX_ID = 0;

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
			final FpGroupHotNgramBitIndex bitsetIndex = terms.fpBits(DEFAULT_INDEX_ID, e.getKey(), choose, choose);
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
		final boolean hotHit = searchBank(hot, true, slice, blkinfo, groupid, terms, maxDoc, acc);
		if (!hotHit) {
			searchBank(common, false, slice, blkinfo, groupid, terms, maxDoc, acc);
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
	private boolean searchBank(FixedBitSet bank, boolean hotMark, BytesRef slice, FpBlockInfo blkinfo, int groupid,
			Terms terms, int maxDoc, FixedBitSet collect) throws IOException {
		if (bank == null) {
			return false;
		}
		final TermsEnum termsEnum = terms.iterator_fp();
		final BytesRef reuse = new BytesRef(new byte[512]);
		boolean anyHit = false;
		for (int bit = bank.nextSetBit(0); bit >= 0; bit = bank.nextSetBit(bit + 1)) {
			final int termIndex = bit + 1;
			if (seekTermAndOrDocs(termsEnum, reuse, DEFAULT_INDEX_ID, groupid, (byte) blkinfo.targetLevel, hotMark, termIndex,
					false, slice, maxDoc, collect)) {
				anyHit = true;
			}
			if (seekTermAndOrDocs(termsEnum, reuse, DEFAULT_INDEX_ID, groupid, (byte) blkinfo.targetLevel, hotMark, termIndex,
					true, slice, maxDoc, collect)) {
				anyHit = true;
			}
		}
		return anyHit;
	}

	private static boolean seekTermAndOrDocs(TermsEnum termsEnum, BytesRef reuse, short indexId, int groupid, byte groupLevel,
			boolean hotMark, int termIndex, boolean isDelTerm, BytesRef slice, int maxDoc, FixedBitSet collect)
			throws IOException {
		FpTokenTermLayout.make_fp_search_prefix(reuse, indexId, groupid, groupLevel, hotMark, termIndex, isDelTerm);
		appendSlicePayload(reuse, slice);
		if (!termsEnum.seekExact(reuse)) {
			return false;
		}
		final BytesRef found = termsEnum.term();
		if (!termHeaderMatches(found, indexId, groupid, groupLevel, hotMark, termIndex, isDelTerm)) {
			return false;
		}
		if (!payloadContains(FpTokenTermLayout.removeHeaderBytes(found), slice)) {
			return false;
		}
		orPostingsInto(termsEnum, maxDoc, collect);
		return true;
	}

	/** 与写段 {@link FpTokenTermLayout#make_fp_term} 一致：第 13 字节 scanlevel=0，其后为载荷。 */
	private static void appendSlicePayload(BytesRef reuse, BytesRef slice) {
		final int off = reuse.offset;
		reuse.bytes[off + FpTokenTermLayout.HOT_TERM_SCANLEVEL_OFFSET] = 0;
		System.arraycopy(slice.bytes, slice.offset, reuse.bytes, off + FpTokenTermLayout.FP_HEADER_BYTES, slice.length);
		reuse.length = FpTokenTermLayout.FP_HEADER_BYTES + slice.length;
	}

	private static boolean termHeaderMatches(BytesRef term, short indexId, int groupid, byte groupLevel, boolean hotMark,
			int termIndex, boolean isDelTerm) {
		if (term.length < FpTokenTermLayout.FP_HEADER_BYTES) {
			return false;
		}
		return NumericUtils.sortableBytesToShort(term.bytes, term.offset + FpTokenTermLayout.INDEX_ID_OFFSET) == indexId
				&& NumericUtils.sortableBytesToInt(term.bytes, term.offset + FpTokenTermLayout.GROUP_ID_OFFSET) == groupid
				&& FpTokenTermLayout.readLevel(term) == (groupLevel & 0xFF)
				&& FpTokenTermLayout.isHotTerm(term) == hotMark
				&& FpTokenTermLayout.readTermIndex(term) == termIndex
				&& FpTokenTermLayout.readIsDelTerm(term) == isDelTerm;
	}

	private static boolean payloadContains(BytesRef payload, BytesRef slice) {
		final int plen = payload.length;
		final int slen = slice.length;
		if (plen < slen) {
			return false;
		}
		final byte[] pb = payload.bytes;
		final int po = payload.offset;
		final byte[] sb = slice.bytes;
		final int so = slice.offset;
		if (plen == slen) {
			for (int j = 0; j < slen; j++) {
				if (pb[po + j] != sb[so + j]) {
					return false;
				}
			}
			return true;
		}
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

	private static void orPostingsInto(TermsEnum termsEnum, int maxDoc, FixedBitSet collect) throws IOException {
		final PostingsEnum pe = termsEnum.postings(null, PostingsEnum.NONE);
		if (pe == null) {
			return;
		}
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
