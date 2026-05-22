package cn.lxdb.plugins.muqingyu.fptoken.dataset.block;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicReference;

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
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpHotTermHierarchyConstants;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpStatNgram;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpTermKey;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpTokenTermLayout;

/**
 * 单组内：组级 doc 并集用 {@link SparseFixedBitSet}；每个 term 对应 {@link FPDocList}，
 * 每个 term 的 {@link FPDocList} 默认用严格递增的 {@code int[]} 存 doc；若某次添加非递增或达到上限则升为
 * {@link SparseFixedBitSet}（见 {@link FPDocList#addDoc(int)}）。
 * 词项按 {@link FpTokenTermLayout#isHotTerm(BytesRef)} 分入热词 / 普通词两个 {@link TreeMap}；键为 {@link FpTermKey}（缓存 hash）。
 */
public final class FpGroupDataRebuild {
    public static final Logger LOG = LxdbLogerEncrypt.getLogger("mqy.fptoken");

	public final int maxDoc;
	/** 组内去重 doc 并集（闭块判定） */
	public final SparseFixedBitSet distinctDocUnion;
	/** 热词表，键为 {@link FpTermKey}（独立字节拷贝 + 预计算 hash） */
	public final TreeMap<FpTermKey, FPDocList> hotTermToDocs = new TreeMap<>();
	/**
	 * 以该热词为锚点前缀，向下最多合并几层子档（从 0 起）。
	 * 0=仅自身（如仅 ab）；1=含 +1 字节档（ab+ab*）；2=含 +2 字节档（ab+ab*+ab**）。见 {@link FpGroupHotNgramRebuild#resolveMaxDownLevelsFromAnchor}。
	 */
	public final TreeMap<FpTermKey, Integer> hotTermToLevel = new TreeMap<>();
	public final TreeMap<FpTermKey, Integer> hotTermToOrder = new TreeMap<>();

	/** 普通词表 */
	public final TreeMap<FpTermKey, FPDocList> commonTermToDocs = new TreeMap<>();
	public final TreeMap<FpTermKey, Integer> commonTermToOrder = new TreeMap<>();
	

	public FpGroupDataRebuild(int maxDoc) {
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

	/** 供 {@link FpGroupHotNgramBitIndex} 使用：热词编号表。 */
	public TreeMap<FpTermKey, Integer> hotTermOrderInternal() {
		return hotTermToOrder;
	}

	public TreeMap<FpTermKey, Integer> hotTermToLevelInternal() {
		return hotTermToLevel;
	}
	
	public TreeMap<FpTermKey, Integer> commonTermOrderInternal() {
		return commonTermToOrder;
	}

	/** 清空 {@link #hotTermToOrder}，再按 {@link #hotTermToDocs} 的 {@link TreeMap} 序从 1 起连续编号。 */
	public void rebuildHotTermOrderFromHotDocs() {
		hotTermToOrder.clear();
		int n = 1;
		for (FpTermKey k : hotTermToDocs.keySet()) {
			hotTermToOrder.put(k, Integer.valueOf(n++));
		}
	}
	
	/** 清空 {@link #hotTermToOrder}，再按 {@link #hotTermToDocs} 的 {@link TreeMap} 序从 1 起连续编号。 */
	public void rebuildCommonTermToOrderFromHotDocs() {
		commonTermToOrder.clear();
		int n = 1;
		for (FpTermKey k : commonTermToDocs.keySet()) {
			commonTermToOrder.put(k, Integer.valueOf(n++));
		}
	}

	public int hotTermOrderSize() {
		return hotTermToOrder.size();
	}

	public int commonTermOrderSize() {
		return commonTermToOrder.size();
	}

	public int maxDocInternal() {
		return maxDoc;
	}



	public void flushto(FpTokenBlockOrchestrator parentItem, boolean is_nochange_hot) throws IOException {
		long ts_begin=CLMillisecondClock.CLOCK.now();


		FpStatNgram ngramstat=FpGroupHotNgramRebuild.execute(this, parentItem,
				FpHotTermHierarchyConstants.HOT_TIER_TERM_COUNT_THRESHOLD);
		long ts_ngram=CLMillisecondClock.CLOCK.now();

		FpGroupHotNgramBitIndex bitinfo=FpGroupHotNgramBitIndex.execute(this);
		long ts_bitset=CLMillisecondClock.CLOCK.now();

		int del_term_docid=distinctDocUnion.nextSetBit(0);
		int stat_del_hotterm_cnt=0;
		long stat_hot_doc_cnt=0;
		long stat_common_doc_cnt=0;

		final byte[] reuse_bytes = new byte[1024];
		BytesRef reuse_term=new BytesRef(reuse_bytes);
		int group_id=parentItem.groupIndex.incrementAndGet();
		for(Entry<FpTermKey, FPDocList> e:hotTermToDocs.entrySet())
		{
			FpTermKey key=e.getKey();
			int level=hotTermToLevel.get(key);
			FPDocList val=e.getValue();
			Integer index=hotTermToOrder.get(key);
			boolean isDelTerm=val.docsize()<=0;
			if(isDelTerm) {//仅仅占位用
				val.addDoc(del_term_docid);
				stat_del_hotterm_cnt++;

			}
			
			stat_hot_doc_cnt+=val.docsize();

			
			FpTokenTermLayout.make_fp_term(reuse_term, (short)0, group_id, (byte)parentItem.targetLevel, FpTokenTermLayout.TERM_MARK_HOT, index, isDelTerm,(byte)level, key.bytesRef());
			BlockTermState stat = parentItem.termsWriter.writefp(parentItem.blockTreeWriter.state,parentItem.pool,parentItem.debugList,reuse_term, val, parentItem.norms);

		}
		
		
		for(Entry<FpTermKey, FPDocList> e:commonTermToDocs.entrySet())
		{
			FpTermKey key=e.getKey();
			FPDocList val=e.getValue();
			Integer index=commonTermToOrder.get(key);
			boolean isDelTerm=val.docsize()<=0;
			if(isDelTerm)
			{
				continue;
			}

			stat_common_doc_cnt+=val.docsize();

			FpTokenTermLayout.make_fp_term(reuse_term, (short)0, group_id, (byte)parentItem.targetLevel, FpTokenTermLayout.TERM_MARK_COMMON, index, false,(byte)0, key.bytesRef());
			BlockTermState stat = parentItem.termsWriter.writefp(parentItem.blockTreeWriter.state,parentItem.pool,parentItem.debugList,reuse_term, val, parentItem.norms);

		}
		

		FpBlockInfo blkinfo=bitinfo.flushto(parentItem.blockTreeWriter.bitOut);
		parentItem.fpblock_list.put(group_id, blkinfo);
	
		parentItem.stat.doclist_hot+=stat_hot_doc_cnt;
		parentItem.stat.doclist_common+=stat_common_doc_cnt;

	
		long ts_end=CLMillisecondClock.CLOCK.now();

		LOG.info("rebuild_flush diff:["+(ts_end-ts_begin)+"~ngram@"+(ts_ngram-ts_begin)+"~bitset@"+(ts_bitset-ts_ngram)+"]ms,doclist:["+stat_hot_doc_cnt+"~"+stat_common_doc_cnt+"],del_hotterm_cnt:"+stat_del_hotterm_cnt+",distinctDocUnion:"+distinctDocUnion.cardinality()+",hotTermToDocs:"+hotTermToDocs.size()+",commonTermToDocs:"+commonTermToDocs.size()+",ngramstat:"+ngramstat);

	
		this.resetAfterFlush();
	}

	private final AtomicReference<PostingsEnum> docsEnum_reuse = new AtomicReference<PostingsEnum>(null);

	public void ingestTermPostings(BytesRef term_withheader, TermsEnum termsEnum, int maxDocExclusive) throws IOException {
		if(FpTokenTermLayout.isHotTerm(term_withheader))
		{
			return ;
		}
		
		BytesRef term_noheader=FpTokenTermLayout.removeHeaderBytes(term_withheader);
		final PostingsEnum pe = termsEnum.postings(docsEnum_reuse.get(), PostingsEnum.NONE);
		final TreeMap<FpTermKey, FPDocList> bucket = commonTermToDocs;
		final FpTermKey probe = FpTermKey.viewOf(term_noheader);
		FPDocList acc = bucket.get(probe);
		if (acc == null) {
			acc = new FPDocList(maxDocExclusive);
			bucket.put(FpTermKey.copyOf(term_noheader), acc);
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





	public void resetAfterFlush() {
		hotTermToDocs.clear();
		hotTermToLevel.clear();
		hotTermToOrder.clear();
		commonTermToDocs.clear();
		commonTermToOrder.clear();
		distinctDocUnion.clear(0, maxDoc);
	}
}
