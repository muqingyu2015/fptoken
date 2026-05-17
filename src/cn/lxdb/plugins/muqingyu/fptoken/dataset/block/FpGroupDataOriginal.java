package cn.lxdb.plugins.muqingyu.fptoken.dataset.block;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.lucene.codecs.BlockTermState;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.SparseFixedBitSet;
import org.apache.lucene.util.offheap.OffheapPoolName;

import cn.lxdb.plugins.muqingyu.fptoken.api.FpTokenBlockOrchestrator;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FPDocList;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpBlockInfo;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpTermKey;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpTokenTermLayout;

/**
 * 单组内：组级 doc 并集用 {@link SparseFixedBitSet}；每个 term 对应 {@link FPDocList}，
 * 每个 term 的 {@link FPDocList} 默认用严格递增的 {@code int[]} 存 doc；若某次添加非递增或达到上限则升为
 * {@link SparseFixedBitSet}（见 {@link FPDocList#addDoc(int)}）。
 * 词项按 {@link FpTokenTermLayout#isHotTerm(BytesRef)} 分入热词 / 普通词两个 {@link TreeMap}；键为 {@link FpTermKey}（缓存 hash）。
 */
public final class FpGroupDataOriginal {

	private final int maxDoc;
	/** 组内去重 doc 并集（闭块判定） */
	public final SparseFixedBitSet distinctDocUnion;
	/** 热词表，键为 {@link FpTermKey}（独立字节拷贝 + 预计算 hash） */
	private final TreeMap<FpTermKey, FPDocList> hotTermToDocs = new TreeMap<>();

	/** 普通词表 */
	private final TreeMap<FpTermKey, FPDocList> commonTermToDocs = new TreeMap<>();
	

	public FpGroupDataOriginal(int maxDoc) {
		this.maxDoc = maxDoc;
		this.distinctDocUnion = new SparseFixedBitSet(OffheapPoolName.fptoken, maxDoc);
	}

	public int termCount() {
		return commonTermToDocs.size();
	}

	/** 供 {@link FpGroupHotNgramRebuild} 使用。 */
	public TreeMap<FpTermKey, FPDocList> hotTermMapInternal() {
		return hotTermToDocs;
	}

	public TreeMap<FpTermKey, FPDocList> commonTermMapInternal() {
		return commonTermToDocs;
	}




	public int maxDocInternal() {
		return maxDoc;
	}


	

	public void flushto(FpTokenBlockOrchestrator parentItem,FpGroupHotNgramBitIndex bitinfo) throws IOException {

		int del_term_docid=distinctDocUnion.nextSetBit(0);


		final byte[] reuse_bytes = new byte[1024];
		BytesRef reuse_term=new BytesRef(reuse_bytes);
		int group_id=parentItem.groupIndex.incrementAndGet();
		for(Entry<FpTermKey, FPDocList> e:hotTermToDocs.entrySet())
		{
			FpTermKey key=e.getKey();
			FPDocList val=e.getValue();
			boolean isDelTerm=FpTokenTermLayout.readIsDelTerm(key.bytesRef())||val.docsize()<=0;
;
			int index=FpTokenTermLayout.readTermIndex(key.bytesRef());
			if(isDelTerm&&val.docsize()<=0) {//仅仅占位用
				val.addDoc(del_term_docid);
			}
			
			FpTokenTermLayout.make_fp_term(reuse_term, (short)0, group_id, (byte)parentItem.targetLevel, FpTokenTermLayout.TERM_MARK_HOT, index, isDelTerm, key.bytesRef());
			BlockTermState stat = parentItem.termsWriter.writefp(parentItem.blockTreeWriter.state,parentItem.pool,parentItem.debugList,reuse_term, val, parentItem.norms);

		}
		
		
		for(Entry<FpTermKey, FPDocList> e:commonTermToDocs.entrySet())
		{
			FpTermKey key=e.getKey();
			FPDocList val=e.getValue();
			boolean isDelTerm=FpTokenTermLayout.readIsDelTerm(key.bytesRef())||val.docsize()<=0;
			if(isDelTerm)
			{
				continue;
			}
			int index=FpTokenTermLayout.readTermIndex(key.bytesRef());

			FpTokenTermLayout.make_fp_term(reuse_term, (short)0, group_id, (byte)parentItem.targetLevel, FpTokenTermLayout.TERM_MARK_COMMON, index, false, FpTokenTermLayout.removeHeaderBytes(key.bytesRef()));
			BlockTermState stat = parentItem.termsWriter.writefp(parentItem.blockTreeWriter.state,parentItem.pool,parentItem.debugList,reuse_term, val, parentItem.norms);

		}
		

		FpBlockInfo blkinfo=bitinfo.flushto(parentItem.blockTreeWriter.bitOut);
		parentItem.fpblock_list.put(group_id, blkinfo);
	

	
		this.resetAfterFlush();
	}

	public void ingestTermPostings(BytesRef term_withheader, TermsEnum termsEnum, int maxDocExclusive) throws IOException {
		BytesRef term_noheader=FpTokenTermLayout.clearAndCopyGroupBytes(term_withheader);
		final PostingsEnum pe = termsEnum.postings(null, PostingsEnum.NONE);
		final TreeMap<FpTermKey, FPDocList> bucket = FpTokenTermLayout.isHotTerm(term_withheader) ? hotTermToDocs : commonTermToDocs;
		final FpTermKey probe = FpTermKey.viewOf(term_noheader);
		FPDocList acc = bucket.get(probe);
		if (acc == null) {//没意义 大部分都是新的
			acc = new FPDocList(maxDocExclusive);
			bucket.put(FpTermKey.viewOf(term_noheader), acc);
		}
		if (pe == null) {
			return;
		}
		int doc;
		while ((doc = pe.nextDoc()) != PostingsEnum.NO_MORE_DOCS) {
			if (doc >= 0 && doc < maxDocExclusive) {
				distinctDocUnion.set(doc);
				acc.addDoc(doc);
			}
		}
	}

	/** 将本组 doc/term 累加到 target（降级时并入可合并组）。 */
	public void mergeInto(FpGroupDataRebuild target) throws IOException {
		for (int d = distinctDocUnion.nextSetBit(0); d >= 0 && d < maxDoc; d = distinctDocUnion.nextSetBit(d + 1)) {
			target.distinctDocUnion.set(d);
		}
		mergeTermBucketInto(hotTermToDocs, target.hotTermToDocs, target);
		mergeTermBucketInto(commonTermToDocs, target.commonTermToDocs, target);
		this.resetAfterFlush();
	}

	private static void mergeTermBucketInto(TreeMap<FpTermKey, FPDocList> from, TreeMap<FpTermKey, FPDocList> into,
			FpGroupDataRebuild target) throws IOException {
		for (Map.Entry<FpTermKey, FPDocList> e : from.entrySet()) {
			FPDocList tgt = into.get(e.getKey());
			if (tgt == null) {
				tgt = new FPDocList(target.maxDoc);
				BytesRef key=FpTokenTermLayout.removeHeaderBytes(e.getKey().bytesRef());
				into.put(FpTermKey.viewOf(key), tgt);
			}
			tgt.addAllDocsFrom(e.getValue());
		}
	}

	public void resetAfterFlush() {
		hotTermToDocs.clear();
		commonTermToDocs.clear();
		distinctDocUnion.clear(0, maxDoc);
	}
}
