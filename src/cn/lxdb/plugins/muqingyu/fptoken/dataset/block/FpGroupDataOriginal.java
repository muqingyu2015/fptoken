package cn.lxdb.plugins.muqingyu.fptoken.dataset.block;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.SparseFixedBitSet;
import org.apache.lucene.util.offheap.OffheapPoolName;
import org.slf4j.Logger;

import cn.lucene.lxdb.params.LxdbLogerEncrypt;
import cn.lucene.proguard.keep.lxdb.common.CLMillisecondClock;
import cn.lxdb.plugins.muqingyu.fptoken.api.FpTokenBlockOrchestrator;
import cn.lxdb.plugins.muqingyu.fptoken.config.FpTokenBlockLevelPolicy;
import cn.lxdb.plugins.muqingyu.fptoken.config.Lucene80FPSearchConfig;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FPDocList;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpBlockInfo;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpLog;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpTermKey;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpTokenTermLayout;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.Utils;

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


	
	private static Object[] PRAL_BIG = FpPralLockPools.makePralLock(12);
	private static Object[] PRAL_MID = FpPralLockPools.makePralLock(16);
	private static AtomicLong PRAL_BIG_INDEX=new AtomicLong(0);
	private static AtomicLong PRAL_MID_INDEX=new AtomicLong(0);
	
	private static AtomicLong PRAL_BIG_CNT=new AtomicLong(0);
	private static AtomicLong PRAL_MID_CNT=new AtomicLong(0);
	private static AtomicLong PRAL_COMMON_CNT=new AtomicLong(0);

	public void flushto(FpTokenBlockOrchestrator parentItem, FpGroupHotNgramBitIndex bitinfo, int newGroupId,byte[] groupkey,String debug_msg)
			throws IOException {

		long ts=CLMillisecondClock.CLOCK.now();

		if(commonTermToDocs.size()>=FpTokenBlockLevelPolicy.BLOCK_LEVEL_TOP_CNT)
		{
			synchronized (PRAL_BIG[(int) (PRAL_BIG_INDEX.incrementAndGet()%PRAL_BIG.length)]) {
				try {
					PRAL_BIG_CNT.incrementAndGet();
					____flushto(ts,parentItem, bitinfo,newGroupId,groupkey, debug_msg);
				}finally {
					PRAL_BIG_CNT.decrementAndGet();

				}

			}
			
			return ;
		}
		
		if(commonTermToDocs.size()>=FpTokenBlockLevelPolicy.BLOCK_LEVEL_HIGH_CNT)
		{
			synchronized (PRAL_MID[(int) (PRAL_MID_INDEX.incrementAndGet()%PRAL_MID.length)]) {

				try {
					PRAL_MID_CNT.incrementAndGet();
					____flushto(ts,parentItem, bitinfo,newGroupId,groupkey, debug_msg);
				}finally {
					PRAL_MID_CNT.decrementAndGet();

				}

			

			}
			
			return ;
		}
		


		try {
			PRAL_COMMON_CNT.incrementAndGet();
			____flushto(ts,parentItem, bitinfo,newGroupId,groupkey, debug_msg);
		}finally {
			PRAL_COMMON_CNT.decrementAndGet();

		}

	
	
	}
	/**
	 * 透传写出：位图由调用方按<strong>合并前逻辑组</strong>读出；{@code newGroupId} 为本段新分配的落盘组号
	 * （倒排头、{@link cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpBlockInfo}、查询 {@code fpBits} 均用此 id）。
	 */
	public void ____flushto(long ts,FpTokenBlockOrchestrator parentItem, FpGroupHotNgramBitIndex bitinfo, int newGroupId,byte[] groupkey,String debug_msg)
			throws IOException {
		final BytesRef columnName = FpTokenTermLayout.readColumnName(new BytesRef(groupkey));
		int columnLevel=parentItem.getColumnLevel(columnName);

		long ts_begin=CLMillisecondClock.CLOCK.now();
		int del_term_docid=distinctDocUnion.nextSetBit(0);

		int stat_del_hotterm_cnt=0;
		long stat_hot_doc_cnt=0;
		long stat_common_doc_cnt=0;
		
		byte[] reuse_bytes = null;
		BytesRef reuse_term=null;
		final int group_id = newGroupId;
		for(Entry<FpTermKey, FPDocList> e:hotTermToDocs.entrySet())
		{
			FpTermKey key=e.getKey();
			FPDocList val=e.getValue();
			
			
			int downTierBudget = FpTokenTermLayout.readHotDownTierBudget(key.bytesRef());
			boolean isDelTerm=FpTokenTermLayout.readIsDelTerm(key.bytesRef())||val.docsize()<=0;
			int index=FpTokenTermLayout.readTermIndex(key.bytesRef());
			if(isDelTerm&&val.docsize()<=0) {//仅仅占位用
				val.addDoc(del_term_docid);
				stat_del_hotterm_cnt++;
			}
			
			stat_hot_doc_cnt+=val.docsize();
			
			BytesRef noheader_term=FpTokenTermLayout.removeColumnAndHeaderBytes(key.bytesRef());
			
			if(reuse_bytes==null)
			{
				reuse_bytes=new byte[1024+FpTokenTermLayout.headerOffset(key.bytesRef())];
				reuse_term=new BytesRef(reuse_bytes);
			}
			FpTokenTermLayout.make_fp_term(reuse_term, columnName, (short)0, group_id, (byte)columnLevel, FpTokenTermLayout.TERM_MARK_HOT, index, isDelTerm, (byte) downTierBudget, noheader_term);
			parentItem.writefpChecked(reuse_term, val, debug_msg + " original:hot");

			
			if(Lucene80FPSearchConfig.PRINT_DEBUG)
			{
				if(noheader_term.length<=0)
				{
					final StringBuilder sb = FpLog.kv();
					FpLog.append(sb, "event", "hotTermEmptyPayload");
					FpLog.append(sb, "phase", debug_msg);
					FpLog.append(sb, "termIndex", index);
					FpLog.append(sb, "termBytes", reuse_term.length);
					FpLog.append(sb, "term", Utils.BytesReftoString(reuse_term));
					FpLog.debugLine(LOG, FpLog.TAG_ORIGINAL, sb);
					continue;
				}
			
				short read_index_id=FpTokenTermLayout.read_index_id(reuse_term);
				int group_id_reuse=FpTokenTermLayout.read_group_id(reuse_term);
				int level=FpTokenTermLayout.readLevel(reuse_term);
				boolean ishot=FpTokenTermLayout.isHotTerm(reuse_term);
				boolean isdel=FpTokenTermLayout.readIsDelTerm(reuse_term);
				int hot_down_tier=FpTokenTermLayout.readHotDownTierBudget(reuse_term);
				BytesRef ref=FpTokenTermLayout.removeColumnAndHeaderBytes(reuse_term);
				
				
				final StringBuilder sb = FpLog.kv();
				FpLog.append(sb, "event", "hotTerm");
				FpLog.append(sb, "phase", debug_msg);
				FpLog.append(sb, "termIndex", index);
				FpLog.append(sb, "indexId", read_index_id);
				FpLog.append(sb, "groupId", group_id_reuse);
				FpLog.append(sb, "level", "L" + level);
				FpLog.append(sb, "isHot", ishot);
				FpLog.append(sb, "isDel", isdel);
				FpLog.append(sb, "hotDownTier", hot_down_tier);
				FpLog.append(sb, "docFreq", val.docsize());
				FpLog.append(sb, "payload", Utils.BytesReftoString(ref));
				FpLog.debugLine(LOG, FpLog.TAG_ORIGINAL, sb);
			
			}
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
			BytesRef noheader_term=FpTokenTermLayout.removeColumnAndHeaderBytes(key.bytesRef());
			if(reuse_bytes==null)
			{
				reuse_bytes=new byte[1024+FpTokenTermLayout.headerOffset(key.bytesRef())];
				reuse_term=new BytesRef(reuse_bytes);
			}
			FpTokenTermLayout.make_fp_term(reuse_term, columnName, (short)0, group_id, (byte)columnLevel, FpTokenTermLayout.TERM_MARK_COMMON, index, false,(byte)0, noheader_term);
			parentItem.writefpChecked(reuse_term, val, debug_msg + " original:common");

			if(Lucene80FPSearchConfig.PRINT_DEBUG)
			{
				if(noheader_term.length<=0)
				{
					final StringBuilder sb = FpLog.kv();
					FpLog.append(sb, "event", "commonTermEmptyPayload");
					FpLog.append(sb, "phase", debug_msg);
					FpLog.append(sb, "termIndex", index);
					FpLog.append(sb, "termBytes", reuse_term.length);
					FpLog.append(sb, "term", Utils.BytesReftoString(reuse_term));
					FpLog.debugLine(LOG, FpLog.TAG_ORIGINAL, sb);
					continue;
				}
			
				short read_index_id=FpTokenTermLayout.read_index_id(reuse_term);
				int group_id_reuse=FpTokenTermLayout.read_group_id(reuse_term);
				int level=FpTokenTermLayout.readLevel(reuse_term);
				boolean ishot=FpTokenTermLayout.isHotTerm(reuse_term);
				boolean isdel=FpTokenTermLayout.readIsDelTerm(reuse_term);
				int hot_down_tier=FpTokenTermLayout.readHotDownTierBudget(reuse_term);
				BytesRef ref=FpTokenTermLayout.removeColumnAndHeaderBytes(reuse_term);
				
				
				final StringBuilder sb = FpLog.kv();
				FpLog.append(sb, "event", "commonTerm");
				FpLog.append(sb, "phase", debug_msg);
				FpLog.append(sb, "termIndex", index);
				FpLog.append(sb, "indexId", read_index_id);
				FpLog.append(sb, "groupId", group_id_reuse);
				FpLog.append(sb, "level", "L" + level);
				FpLog.append(sb, "isHot", ishot);
				FpLog.append(sb, "isDel", isdel);
				FpLog.append(sb, "hotDownTier", hot_down_tier);
				FpLog.append(sb, "docFreq", val.docsize());
				FpLog.append(sb, "payload", Utils.BytesReftoString(ref));
				FpLog.debugLine(LOG, FpLog.TAG_ORIGINAL, sb);
			
			}
			
		}
		
		parentItem.stat.doclist_hot+=stat_hot_doc_cnt;
		parentItem.stat.doclist_common+=stat_common_doc_cnt;
		parentItem.stageBitIndex(group_id, bitinfo, "orginal", columnName, del_term_docid);
	
		long ts_end=CLMillisecondClock.CLOCK.now();

		final StringBuilder sbFlush = FpLog.kv();
		FpLog.append(sbFlush, "event", "flush");
		FpLog.append(sbFlush, "phase", debug_msg);
		FpLog.append(sbFlush, "groupId", group_id);
		FpLog.append(sbFlush, "msLock", ts_begin-ts );

		FpLog.append(sbFlush, "ms", ts_end - ts_begin);
		FpLog.append(sbFlush, "pral", PRAL_BIG_CNT.get()+":"+PRAL_MID_CNT.get()+":"+PRAL_COMMON_CNT.get() );

		FpLog.append(sbFlush, "targetLevel", "L" + columnLevel);
		FpLog.append(sbFlush, "doclistHot", stat_hot_doc_cnt);
		FpLog.append(sbFlush, "doclistCommon", stat_common_doc_cnt);
		FpLog.append(sbFlush, "delHotTerms", stat_del_hotterm_cnt);
		FpLog.append(sbFlush, "distinctDocs", distinctDocUnion.cardinality());
		FpLog.append(sbFlush, "hotTerms", hotTermToDocs.size());
		FpLog.append(sbFlush, "commonTerms", commonTermToDocs.size());
		FpLog.append(sbFlush, "bitindex", "staged");
		FpLog.infoLine(LOG, FpLog.TAG_ORIGINAL, sbFlush);

	
		this.resetAfterFlush();
	}
	private final AtomicReference<PostingsEnum> docsEnum_reuse = new AtomicReference<PostingsEnum>(null);

	public void ingestTermPostings(BytesRef term_withheader, TermsEnum termsEnum, int maxDocExclusive) throws IOException {
		BytesRef term_hasheader=FpTokenTermLayout.clearAndCopyGroupBytes(term_withheader);
		final PostingsEnum pe = termsEnum.postings(docsEnum_reuse.get(), PostingsEnum.NONE);
		final TreeMap<FpTermKey, FPDocList> bucket = FpTokenTermLayout.isHotTerm(term_withheader) ? hotTermToDocs : commonTermToDocs;
		final FpTermKey probe = FpTermKey.viewOf(term_hasheader);
		FPDocList acc = bucket.get(probe);
		if (acc == null) {//没意义 大部分都是新的
			acc = new FPDocList(maxDocExclusive);
			bucket.put(probe, acc);
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

//	/** 将本组 doc/term 累加到 target（降级时并入可合并组）。 */
//	public void mergeIntoRebuild(FpGroupDataRebuild target) throws IOException {
//		for (int d = distinctDocUnion.nextSetBit(0); d >= 0 && d < maxDoc; d = FPDocList.nextSetBitInSparse(distinctDocUnion,
//				maxDoc, d)) {
//			target.distinctDocUnion.set(d);
//		}
//		mergeTermBucketInto(hotTermToDocs, target.hotTermToDocs, target);
//		mergeTermBucketInto(commonTermToDocs, target.commonTermToDocs, target);
//		this.resetAfterFlush();
//	}

//	private static void mergeTermBucketInto(TreeMap<FpTermKey, FPDocList> from, TreeMap<FpTermKey, FPDocList> into,
//			FpGroupDataRebuild target) throws IOException {
//		for (Map.Entry<FpTermKey, FPDocList> e : from.entrySet()) {
//			FPDocList tgt = into.get(e.getKey());
//			if (tgt == null) {
//				tgt = new FPDocList(target.maxDoc);
//				BytesRef key=FpTokenTermLayout.removeColumnAndHeaderBytes(e.getKey().bytesRef());
//				into.put(FpTermKey.viewOf(key), tgt);
//			}
//			tgt.addAllDocsFrom(e.getValue());
//		}
//	}

	public void resetAfterFlush() {
		hotTermToDocs.clear();
		commonTermToDocs.clear();
		distinctDocUnion.clear(0, maxDoc);
	}
}
