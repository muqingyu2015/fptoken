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
import org.slf4j.Logger;

import cn.lucene.lxdb.params.LxdbLogerEncrypt;
import cn.lxdb.plugins.muqingyu.fptoken.config.Lucene80FPSearchConfig;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.block.FpGroupHotNgramBitIndex;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpBlockInfo;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpTokenTermLayout;

/**
 * 查询侧：按 ngram {@link BytesRef} 切片在组内 hot/common 位图中定位候选 term 序号，再
 * {@link Terms#iterator_fp()} seek 倒排并合并 doc，多切片 AND、同切片多组 OR。
 */
public class FpSearch {
	
	public static final Logger LOG = LxdbLogerEncrypt.getLogger("mqy.fptoken");

	private void print_debug(Terms terms) throws IOException
	{
		if(!Lucene80FPSearchConfig.PRINT_DEBUG)
		{
			return ;
		}
		final AtomicReference<PostingsEnum> docsEnum = new AtomicReference<PostingsEnum>(null);

		TermsEnum termsEnum = terms.iterator();
		BytesRef term = termsEnum.term();
		int termIndex=1;
		while (term != null) {
			try {
				if(term.length<FpTokenTermLayout.FP_HEADER_BYTES)
				{
					
					LOG.info("debug termIndex:"+termIndex+" term:"+terms.getClass().getName()+" len:"+term.length+"  data:"+term.utf8ToString());
					continue;
				}
			
				short read_index_id=FpTokenTermLayout.read_index_id(term);
				int group_id=FpTokenTermLayout.read_group_id(term);
				int level=FpTokenTermLayout.readLevel(term);
				boolean ishot=FpTokenTermLayout.isHotTerm(term);
				boolean isdel=FpTokenTermLayout.readIsDelTerm(term);
				int termindex=FpTokenTermLayout.readTermIndex(term);
				int hot_down_tier=FpTokenTermLayout.readHotDownTierBudget(term);
				BytesRef ref=FpTokenTermLayout.removeHeaderBytes(term);
				
				
				PostingsEnum pe = termsEnum.postings(docsEnum.get(), PostingsEnum.NONE);
	
				LOG.info("debug termIndex:"+termIndex+" index_id:"+read_index_id+" group_id:"+group_id+" level:"+level+" hot:"+ishot+" isdel:"+isdel+" termindex:"+termindex+" hot_down_tier:"+hot_down_tier+" freq:"+pe.freq()+" data:"+ref.utf8ToString());
			}finally {

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
	public FixedBitSet search(TreeMap<Integer, FpBlockInfo> fpblock_list, Terms terms, int maxDoc, BytesRef[] slices)
			throws IOException {
		
		print_debug(terms);
		final boolean[][] choose = new boolean[Lucene80FPSearchConfig.NGRAM_MAX][Lucene80FPSearchConfig.BUCKETS];
		for (int i = 0; i < choose.length; i++) {
			Arrays.fill(choose[i], false);
		}
		for (BytesRef slice : slices) {//TODO:这里仅仅根据当前长度判断误差太大，需要往上多判断几个bitset的and，不然在遍历的时候可能会有太多的误命中,但是也没必要每层都要判断，因此向上判断1~2层我觉得是可以的
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
				if(Lucene80FPSearchConfig.PRINT_DEBUG)
				{  
					LOG.info("bitset "+hot.cardinality()+" "+hot.nextSetBit(0)+" "+common.cardinality()+" "+common.nextSetBit(0)+" "+lenIdx+" " +bucket +" "+slice.length);;
				}
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

		final boolean hotHit = searchBankHot(hot, true, slice, blkinfo, groupid, terms, maxDoc, acc);
		if (!hotHit) {
			searchBankCommon(common, false, slice, blkinfo, groupid, terms, maxDoc, acc);
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
	private boolean searchBankHot(FixedBitSet bank, boolean hotMark, BytesRef slice, FpBlockInfo blkinfo, int groupid,
			Terms terms, int maxDoc, FixedBitSet collect) throws IOException {
		if (bank == null) {
			return false;
		}

		final AtomicReference<PostingsEnum> reusePosting = new AtomicReference<PostingsEnum>(null);
		final TermsEnum termsEnum = terms.iterator();
		final BytesRef reuse = new BytesRef(new byte[512]);
		boolean anyHit = false;
		final AtomicInteger maxHotPayloadLen = new AtomicInteger(0);
		boolean payloadLenCapSet = false;

		for (int bit = bank.nextSetBit(0); bit != DocIdSetIterator.NO_MORE_DOCS; bit = bank.nextSetBit(bit + 1)) {
			final int termIndex = bit;
			if (!payloadLenCapSet) {
				int status = seekTermAndOrDocs(reusePosting, maxHotPayloadLen, true, termsEnum, reuse,
						Lucene80FPSearchConfig.DEFAULT_INDEX_ID, groupid, (byte) blkinfo.targetLevel, hotMark, termIndex,
						slice, maxDoc, collect);
				if (status == SEEK_OK) {
					anyHit = true;
					payloadLenCapSet = true;
					continue;
				}else {
					break;
				}
			}
			final int status = seekTermAndOrDocs(reusePosting, maxHotPayloadLen, false, termsEnum, reuse,
					Lucene80FPSearchConfig.DEFAULT_INDEX_ID, groupid, (byte) blkinfo.targetLevel, hotMark, termIndex,
					 slice, maxDoc, collect);
			if (status == SEEK_OK) {
				anyHit = true;
			}
			if (status == SEEK_BREAK_PAYLOAD_LEN) {
				break;
			}
		}
		return anyHit;
	}

	private boolean searchBankCommon(FixedBitSet bank, boolean hotMark, BytesRef slice, FpBlockInfo blkinfo, int groupid,
			Terms terms, int maxDoc, FixedBitSet collect) throws IOException {
		if (bank == null) {
			return false;
		}

		final AtomicReference<PostingsEnum> reusePosting = new AtomicReference<PostingsEnum>(null);
		final TermsEnum termsEnum = terms.iterator();
		final BytesRef reuse = new BytesRef(new byte[512]);
		boolean anyHit = false;

		for (int bit = bank.nextSetBit(0); bit != DocIdSetIterator.NO_MORE_DOCS; bit = bank.nextSetBit(bit + 1)) {
			final int termIndex = bit;
			final int status = seekTermAndOrDocs(reusePosting, null, false, termsEnum, reuse,
					Lucene80FPSearchConfig.DEFAULT_INDEX_ID, groupid, (byte) blkinfo.targetLevel, hotMark, termIndex,
					 slice, maxDoc, collect);
			if (status == SEEK_OK) {
				anyHit = true;
			}
		}
		return anyHit;
	}

	private static final int SEEK_OK = 0;
	private static final int SEEK_BREAK_PAYLOAD_LEN = 1;
	private static final int SEEK_MISS = 2;

	private static int seekTermAndOrDocs(AtomicReference<PostingsEnum> reusePosting,
			AtomicInteger maxHotPayloadLenOrNull, boolean requireExactPayloadMatch, TermsEnum termsEnum, BytesRef reuse,
			short indexId, int groupid, byte groupLevel, boolean hotMark, int termIndex,
			BytesRef slice, int maxDoc, FixedBitSet collect) throws IOException {
		FpTokenTermLayout.make_fp_search_prefix(reuse, indexId, groupid, groupLevel, hotMark, termIndex);
		
		if(Lucene80FPSearchConfig.PRINT_DEBUG)
		{  
			LOG.info("seekCeil indexId:"+indexId+" "+groupid+" "+groupLevel+" " +hotMark +" "+termIndex+" "+slice.length);;
		}
		if (termsEnum.seekCeil(reuse) == TermsEnum$SeekStatus.END) {
			return SEEK_MISS;
		}
		final BytesRef found = termsEnum.term();
		boolean isDelTerm=FpTokenTermLayout.readIsDelTerm(found);
		if(Lucene80FPSearchConfig.PRINT_DEBUG)
		{  
			LOG.info("found indexId:"+indexId+" "+groupid+" "+groupLevel+" " +hotMark +" "+termIndex+" "+slice.length);;
		}
		if (!termHeaderMatches(found, groupid, groupLevel, hotMark, termIndex)) {
			return SEEK_MISS;
		}
		final BytesRef payload = FpTokenTermLayout.removeHeaderBytes(found);
		if (!payloadMatchesSlice(requireExactPayloadMatch, payload, slice)) {
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
			orPostingsInto(reusePosting, termsEnum, maxDoc, collect);
		}
		return SEEK_OK;
	}

	private static boolean termHeaderMatches(BytesRef found, int groupid, byte groupLevel, boolean hotMark,
			int termIndex) {
		if (found.length < FpTokenTermLayout.FP_HEADER_BYTES) {
			return false;
		}
		return FpTokenTermLayout.read_group_id(found) == groupid
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

	private static void orPostingsInto(AtomicReference<PostingsEnum> reuse, TermsEnum termsEnum, int maxDoc,
			FixedBitSet collect) throws IOException {
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
