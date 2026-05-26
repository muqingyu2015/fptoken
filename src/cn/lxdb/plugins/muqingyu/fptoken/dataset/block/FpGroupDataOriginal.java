package cn.lxdb.plugins.muqingyu.fptoken.dataset.block;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicReference;
import java.util.TreeMap;

import org.apache.lucene.codecs.BlockTermState;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.SparseFixedBitSet;
import org.apache.lucene.util.offheap.OffheapPoolName;
import org.slf4j.Logger;

import cn.lucene.lxdb.params.LxdbLogerEncrypt;
import cn.lucene.proguard.keep.lxdb.common.CLMillisecondClock;
import cn.lxdb.plugins.muqingyu.fptoken.api.FpTokenBlockOrchestrator;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FPDocList;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpBlockInfo;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpTermKey;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpTokenTermLayout;
import cn.lxdb.stl.v25.common.MillisecondClock;

/**
 * 单组内：组级 doc 并集用 {@link SparseFixedBitSet}；每个 term 对应 {@link FPDocList}，
 * 每个 term 的 {@link FPDocList} 默认用严格递增的 {@code int[]} 存 doc；若某次添加非递增或达到上限则升为
 * {@link SparseFixedBitSet}（见 {@link FPDocList#addDoc(int)}）。
 * 词项按 {@link FpTokenTermLayout#isHotTerm(BytesRef)} 分入热词 / 普通词两个 {@link TreeMap}；键为 {@link FpTermKey}（缓存 hash）。
 */
public final class FpGroupDataOriginal {
    public static final Logger LOG = LxdbLogerEncrypt.getLogger("mqy.fptoken");

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


	

	/**
	 * 透传写出：位图由调用方按<strong>合并前逻辑组</strong>读出；{@code newGroupId} 为本段新分配的落盘组号
	 * （倒排头、{@link cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpBlockInfo}、查询 {@code fpBits} 均用此 id）。
	 */
	public void flushto(FpTokenBlockOrchestrator parentItem, FpGroupHotNgramBitIndex bitinfo, int newGroupId)
			throws IOException {

		long ts_begin=CLMillisecondClock.CLOCK.now();
		int del_term_docid=distinctDocUnion.nextSetBit(0);

		int stat_del_hotterm_cnt=0;
		long stat_hot_doc_cnt=0;
		long stat_common_doc_cnt=0;
		
		final byte[] reuse_bytes = new byte[1024];
		BytesRef reuse_term=new BytesRef(reuse_bytes);
		final int group_id = newGroupId;
		for(Entry<FpTermKey, FPDocList> e:hotTermToDocs.entrySet())
		{
			FpTermKey key=e.getKey();
			FPDocList val=e.getValue();
			int scanlevel=FpTokenTermLayout.readHotTermScanLevel(key.bytesRef());
			boolean isDelTerm=FpTokenTermLayout.readIsDelTerm(key.bytesRef())||val.docsize()<=0;
			int index=FpTokenTermLayout.readTermIndex(key.bytesRef());
			if(isDelTerm&&val.docsize()<=0) {//仅仅占位用
				val.addDoc(del_term_docid);
				stat_del_hotterm_cnt++;
			}
			
			stat_hot_doc_cnt+=val.docsize();
			
			BytesRef noheader_term=FpTokenTermLayout.removeHeaderBytes(key.bytesRef());
			FpTokenTermLayout.make_fp_term(reuse_term, (short)0, group_id, (byte)parentItem.targetLevel, FpTokenTermLayout.TERM_MARK_HOT, index, isDelTerm,(byte)scanlevel, noheader_term);
			BlockTermState stat = parentItem.termsWriter.writefp(parentItem.blockTreeWriter.state,parentItem.pool,parentItem.debugList,reuse_term, val, parentItem.norms);

		}
		
		
		for(Entry<FpTermKey, FPDocList> e:commonTermToDocs.entrySet())
		{
			FpTermKey key=e.getKey();
			FPDocList val=e.getValue();
			boolean isDelTerm=val.docsize()<=0;
			if(isDelTerm)
			{
				continue;
			}
			int index=FpTokenTermLayout.readTermIndex(key.bytesRef());

			stat_common_doc_cnt+=val.docsize();
			BytesRef noheader_term=FpTokenTermLayout.removeHeaderBytes(key.bytesRef());

			FpTokenTermLayout.make_fp_term(reuse_term, (short)0, group_id, (byte)parentItem.targetLevel, FpTokenTermLayout.TERM_MARK_COMMON, index, false,(byte)0, noheader_term);
			BlockTermState stat = parentItem.termsWriter.writefp(parentItem.blockTreeWriter.state,parentItem.pool,parentItem.debugList,reuse_term, val, parentItem.norms);

		}
		
		parentItem.stat.doclist_hot+=stat_hot_doc_cnt;
		parentItem.stat.doclist_common+=stat_common_doc_cnt;
		FpBlockInfo blkinfo=bitinfo.flushto(parentItem.blockTreeWriter.bitOut);
		parentItem.fpblock_list.put(group_id, blkinfo);
	
		long ts_end=CLMillisecondClock.CLOCK.now();

		LOG.info("original_flush diff:"+(ts_end-ts_begin)+"ms,doclist:["+stat_hot_doc_cnt+"~"+stat_common_doc_cnt+"],del_hotterm_cnt:"+stat_del_hotterm_cnt+",distinctDocUnion:"+distinctDocUnion.cardinality()+",hotTermToDocs:"+hotTermToDocs.size()+",commonTermToDocs:"+commonTermToDocs.size());

	
		this.resetAfterFlush();
	}
	private final AtomicReference<PostingsEnum> docsEnum_reuse = new AtomicReference<PostingsEnum>(null);

	public void ingestTermPostings(BytesRef term_withheader, TermsEnum termsEnum, int maxDocExclusive) throws IOException {
		BytesRef term_noheader=FpTokenTermLayout.clearAndCopyGroupBytes(term_withheader);
		final PostingsEnum pe = termsEnum.postings(docsEnum_reuse.get(), PostingsEnum.NONE);
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
		docsEnum_reuse.set(pe);

	}

	/** 将本组 doc/term 累加到 target（降级时并入可合并组）。 */
	public void mergeIntoRebuild(FpGroupDataRebuild target) throws IOException {
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
