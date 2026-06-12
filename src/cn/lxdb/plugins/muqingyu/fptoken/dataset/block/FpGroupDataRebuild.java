package cn.lxdb.plugins.muqingyu.fptoken.dataset.block;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
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
import cn.lxdb.plugins.muqingyu.fptoken.config.FpTokenBlockLevelPolicy;
import cn.lxdb.plugins.muqingyu.fptoken.config.Lucene80FPSearchConfig;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.block.FpGroupHotNgramRebuild.CommonTermSortEntry;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FPDocList;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpDocListEach;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpLog;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpStatNgram;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpTermKey;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpTokenTermLayout;
import cn.lxdb.plugins.muqingyu.fptoken.pool.FpHashMapPoolHub;
import cn.lxdb.plugins.muqingyu.fptoken.pool.FpHashMapPoolIds;

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
	/** 热词表，键序见 {@link FpTermKey#ORDER_BY_LENGTH_THEN_BYTES}。 */
//	public final HashMap<FpTermKey, FPDocList> hotTermToDocs1 = new HashMap<>();
	/**
	 * 热词锚点「向下扩展档」预算（≥1，含锚点档），由 {@link FpGroupHotNgramRebuild} 写入词项头 offset 12。
	 * 与 {@link FpTokenTermLayout#maxHotPayloadLen(int, int)}、检索热词长度截断一致。
	 */
	public  final HashMap<FpTermKey, FPDocList> hotTermToDocs1 = FpHashMapPoolHub.borrow(  FpHashMapPoolIds.hotTermToDocs,  FpTermKey.class,   FPDocList.class,  FpTokenBlockLevelPolicy.HASH_MAP_DEFAULT_SIZE);

	
	public  final HashMap<FpTermKey, Integer> hotTermDownTierBudget1 = FpHashMapPoolHub.borrow(  FpHashMapPoolIds.hotTermDownTierBudget,  FpTermKey.class,   Integer.class,  FpTokenBlockLevelPolicy.HASH_MAP_DEFAULT_SIZE);

//	public final HashMap<FpTermKey, Integer> hotTermToOrder1 = FpHashMapPoolHub.borrow(6, FpTermKey.class, Integer.class,
//			FpTokenBlockLevelPolicy.HASH_MAP_DEFAULT_SIZE);

	/** 普通词表 */
	public  final HashMap<FpTermKey, FPDocList> commonTermToDocs1 = FpHashMapPoolHub.borrow(  FpHashMapPoolIds.commonTermToDocs,  FpTermKey.class,   FPDocList.class,  FpTokenBlockLevelPolicy.HASH_MAP_DEFAULT_SIZE);


	private ArrayList<CommonTermSortEntry> commonTermFlushOrder1=null;

	public FpGroupDataRebuild(int maxDoc) {
		this.maxDoc = maxDoc;
		this.distinctDocUnion = new SparseFixedBitSet(OffheapPoolName.fptoken, maxDoc);
	}

	public int termCount() {
		return commonTermToDocs1.size();
	}

	/** 供 {@link FpGroupHotNgramRebuild} 使用。 */
	public HashMap<FpTermKey, FPDocList> hotTermMapInternal() {
		return hotTermToDocs1;
	}

	public HashMap<FpTermKey, FPDocList> commonTermMapInternal() {
		return commonTermToDocs1;
	}



	public HashMap<FpTermKey, Integer> hotTermDownTierBudgetInternal() {
		return hotTermDownTierBudget1;
	}
	


	public void setCommonTermFlushOrder(ArrayList<CommonTermSortEntry> order) {
		this.commonTermFlushOrder1 = order;
	}





	public int maxDocInternal() {
		return maxDoc;
	}

	private static Object[] PRAL_BIG=makePralLock(24);
	private static Object[] PRAL_MID=makePralLock(32);

	private static Object[] makePralLock(int cnt)
	{
		Object[] objs=new Object[cnt];
		for(int i=0;i<cnt;i++)
		{
			objs[i]=new Object();
		}
		return objs;
	}
	private static AtomicLong PRAL_BIG_INDEX=new AtomicLong(0);
	private static AtomicLong PRAL_MID_INDEX=new AtomicLong(0);
	
	private static AtomicLong PRAL_BIG_CNT=new AtomicLong(0);
	private static AtomicLong PRAL_MID_CNT=new AtomicLong(0);
	private static AtomicLong PRAL_COMMON_CNT=new AtomicLong(0);

	public void flushto(FpTokenBlockOrchestrator parentItem, byte[] groupkey,String debug_msg) throws IOException {
		long ts=CLMillisecondClock.CLOCK.now();

		if(commonTermToDocs1.size()>=FpTokenBlockLevelPolicy.BLOCK_LEVEL_TOP_CNT)
		{
			synchronized (PRAL_BIG[(int) (PRAL_BIG_INDEX.incrementAndGet()%PRAL_BIG.length)]) {
				try {
					PRAL_BIG_CNT.incrementAndGet();
					____flushto(ts,parentItem, groupkey, debug_msg);
				}finally {
					PRAL_BIG_CNT.decrementAndGet();

				}

			}
			
			return ;
		}
		
		if(commonTermToDocs1.size()>=FpTokenBlockLevelPolicy.BLOCK_LEVEL_HIGH_CNT)
		{
			synchronized (PRAL_MID[(int) (PRAL_MID_INDEX.incrementAndGet()%PRAL_MID.length)]) {

				try {
					PRAL_MID_CNT.incrementAndGet();
					____flushto(ts,parentItem, groupkey, debug_msg);
				}finally {
					PRAL_MID_CNT.decrementAndGet();

				}

			

			}
			
			return ;
		}
		


		try {
			PRAL_COMMON_CNT.incrementAndGet();
			____flushto(ts,parentItem, groupkey, debug_msg);
		}finally {
			PRAL_COMMON_CNT.decrementAndGet();

		}

	

	
		
	}

	public void ____flushto(long ts,FpTokenBlockOrchestrator parentItem, byte[] groupkey,String debug_msg) throws IOException {
		long ts_begin=CLMillisecondClock.CLOCK.now();

		final BytesRef columnName = FpTokenTermLayout.readColumnName(new BytesRef(groupkey));
		
		if(commonTermToDocs1.size()<=FpTokenBlockLevelPolicy.NO_INDEX_THRESHOLD)
		{
			//如果这个field的term数量很少，则采用暴力遍历，用于解决稀疏列

			int stat_del_hotterm_cnt=0;
			long stat_common_doc_cnt=0;

			final byte[] reuse_bytes = new byte[1024];
			BytesRef reuse_term=new BytesRef(reuse_bytes);
			int group_id=parentItem.groupIndex.incrementAndGet();
			
			int order = 1;

			for (CommonTermSortEntry entry : this.commonTermFlushOrder1)
			{
				FpTermKey key=entry.key;
			    final Integer index=Integer.valueOf(order++);

				FPDocList val=entry.sourceDocList;
				boolean isDelTerm=val.docsize()<=0;
				if(isDelTerm)
				{
					continue;
				}

				stat_common_doc_cnt+=val.docsize();

				final BytesRef columnPayload = key.bytesRef();

				FpTokenTermLayout.make_fp_term(reuse_term, columnName, (short)0, group_id, (byte)FpTokenBlockLevelPolicy.BLOCK_LEVEL_NOGROUP, FpTokenTermLayout.TERM_MARK_COMMON, index, false,(byte)0, columnPayload);
				BlockTermState stat=parentItem.writefpChecked(reuse_term, val, debug_msg + " rebuild:commonskip");
				if(Lucene80FPSearchConfig.PRINT_DEBUG)
				{
					final StringBuilder sb = FpLog.kv();
					FpLog.append(sb, "event", "commonSkipTerm");
					FpLog.append(sb, "phase", debug_msg);
					FpLog.append(sb, "termIndex", index);
					FpLog.append(sb, "docFreq", val.docsize());
					FpLog.append(sb, "postings", stat.docFreq);
					FpLog.append(sb, "term", FpTokenTermLayout.toReadableString(reuse_term));
					FpLog.debugLine(LOG, FpLog.TAG_REBUILD, sb);
				}
			}
			

		
			parentItem.stat.doclist_common+=stat_common_doc_cnt;

		
			long ts_end=CLMillisecondClock.CLOCK.now();

			final StringBuilder sbZero = FpLog.kv();
			FpLog.append(sbZero, "event", "flushZero");
			FpLog.append(sbZero, "phase", debug_msg);
			FpLog.append(sbZero, "ms", ts_end - ts_begin);
			FpLog.append(sbZero, "groupId", group_id);
			FpLog.append(sbZero, "doclistCommon", stat_common_doc_cnt);
			FpLog.append(sbZero, "delHotTerms", stat_del_hotterm_cnt);
			FpLog.append(sbZero, "distinctDocs", distinctDocUnion.cardinality());
			FpLog.append(sbZero, "commonTerms", commonTermToDocs1.size());
			FpLog.infoLineSampled(LOG, FpLog.TAG_REBUILD, sbZero, ts_end - ts_begin);

		
		}else {
			
			int columnLevel=FpTokenBlockLevelPolicy.resolveTargetBlockLevel(this.commonTermToDocs1.size(), distinctDocUnion.cardinality());

			FpStatNgram ngramstat=FpGroupHotNgramRebuild.execute(columnLevel,this, parentItem);
			long ts_ngram=CLMillisecondClock.CLOCK.now();

			final ArrayList<java.util.Map.Entry<FpTermKey, FPDocList>> hot_ordered = new ArrayList<>(hotTermToDocs1.entrySet());
			hot_ordered.sort(java.util.Map.Entry.comparingByKey(FpTermKey.ORDER_BY_LENGTH_THEN_BYTES));
			

			FpGroupHotNgramBitIndex bitinfo=FpGroupHotNgramBitIndex.execute1(columnLevel,this,this.hotTermToDocs1,hot_ordered,this.commonTermFlushOrder1);
			long ts_bitset=CLMillisecondClock.CLOCK.now();

			int del_term_docid=distinctDocUnion.nextSetBit(0);
			int stat_del_hotterm_cnt=0;
			long stat_hot_doc_cnt=0;
			long stat_common_doc_cnt=0;

			final byte[] reuse_bytes = new byte[1024];
			BytesRef reuse_term=new BytesRef(reuse_bytes);
			int group_id=parentItem.groupIndex.incrementAndGet();
			
			int order = 1;
			for (final java.util.Map.Entry<FpTermKey, FPDocList> e : hot_ordered) {
				  final FpTermKey key = e.getKey();
				    final FPDocList val = e.getValue();
				    final Integer index=Integer.valueOf(order++);
				
				final Integer budgetObj = hotTermDownTierBudget1.get(key);
				final int downTierBudget = budgetObj != null ? budgetObj.intValue() : 0;
				final boolean isDelTerm = val.docsize() <= 0;
				if (isDelTerm) { // 仅仅占位用
					val.addDoc(del_term_docid);
					stat_del_hotterm_cnt++;
				}

				stat_hot_doc_cnt += val.docsize();

				final BytesRef columnPayload = key.bytesRef();

				FpTokenTermLayout.make_fp_term(reuse_term, columnName, (short) 0, group_id, (byte) columnLevel,
						FpTokenTermLayout.TERM_MARK_HOT, index, isDelTerm, (byte) downTierBudget, columnPayload);
				parentItem.writefpChecked(reuse_term, val, debug_msg + " rebuild:hot");
				
				
				if(Lucene80FPSearchConfig.PRINT_DEBUG)
				{
					final StringBuilder sb = FpLog.kv();
					FpLog.append(sb, "event", "hotTerm");
					FpLog.append(sb, "phase", debug_msg);
					FpLog.append(sb, "termIndex", index);
					FpLog.append(sb, "docFreq", val.docsize());
					FpLog.append(sb, "downTier", downTierBudget);
					FpLog.append(sb, "isDel", isDelTerm);
					FpLog.append(sb, "term", FpTokenTermLayout.toReadableString(reuse_term));
					FpLog.debugLine(LOG, FpLog.TAG_REBUILD, sb);
				}
			}
			
			order = 1;
			for (CommonTermSortEntry entry : this.commonTermFlushOrder1)
			{
				FpTermKey key=entry.key;
			    final Integer index=Integer.valueOf(order++);

				FPDocList val=entry.sourceDocList;
				boolean isDelTerm=val.docsize()<=0;
				if(isDelTerm)
				{
					continue;
				}

				stat_common_doc_cnt+=val.docsize();

				final BytesRef columnPayload = key.bytesRef();

				FpTokenTermLayout.make_fp_term(reuse_term, columnName, (short)0, group_id, (byte)columnLevel, FpTokenTermLayout.TERM_MARK_COMMON, index, false,(byte)0, columnPayload);
				parentItem.writefpChecked(reuse_term, val, debug_msg + " rebuild:common");
				if(Lucene80FPSearchConfig.PRINT_DEBUG)
				{
					final StringBuilder sb = FpLog.kv();
					FpLog.append(sb, "event", "commonTerm");
					FpLog.append(sb, "phase", debug_msg);
					FpLog.append(sb, "termIndex", index);
					FpLog.append(sb, "docFreq", val.docsize());
					FpLog.append(sb, "targetLevel", "L" + columnLevel);
					FpLog.append(sb, "term", FpTokenTermLayout.toReadableString(reuse_term));
					FpLog.debugLine(LOG, FpLog.TAG_REBUILD, sb);
				}
			}
			

			parentItem.stageBitIndex(group_id, bitinfo, "rebuild", columnName, del_term_docid);
		
			parentItem.stat.doclist_hot+=stat_hot_doc_cnt;
			parentItem.stat.doclist_common+=stat_common_doc_cnt;

		
			long ts_end=CLMillisecondClock.CLOCK.now();

			final StringBuilder sbFlush = FpLog.kv();
			FpLog.append(sbFlush, "event", "flush");
			FpLog.append(sbFlush, "phase", debug_msg);
			FpLog.append(sbFlush, "groupId", group_id);
			FpLog.append(sbFlush, "msLock", ts_begin-ts );
			FpLog.append(sbFlush, "ms", ts_end - ts_begin);
			FpLog.append(sbFlush, "pral", PRAL_BIG_CNT.get()+":"+PRAL_MID_CNT.get()+":"+PRAL_COMMON_CNT.get() );

			FpLog.append(sbFlush, "msNgram", ts_ngram - ts_begin);
			FpLog.append(sbFlush, "msBitset", ts_bitset - ts_ngram);
			FpLog.append(sbFlush, "targetLevel", "L" + columnLevel);
			FpLog.append(sbFlush, "doclistHot", stat_hot_doc_cnt);
			FpLog.append(sbFlush, "doclistCommon", stat_common_doc_cnt);
			FpLog.append(sbFlush, "delHotTerms", stat_del_hotterm_cnt);
			FpLog.append(sbFlush, "distinctDocs", distinctDocUnion.cardinality());
			FpLog.append(sbFlush, "hotTerms", hotTermToDocs1.size());
			FpLog.append(sbFlush, "commonTerms", commonTermToDocs1.size());
			FpLog.append(sbFlush, "ngramStat", ngramstat);
			FpLog.append(sbFlush, "bitindex", "staged");
			FpLog.infoLineSampled(LOG, FpLog.TAG_REBUILD, sbFlush, ts_end - ts_begin);

		}
		

	
		this.resetAfterFlush();
	}

	private final AtomicReference<PostingsEnum> docsEnum_reuse = new AtomicReference<PostingsEnum>(null);

	public void ingestTermPostings(BytesRef term_withheader, TermsEnum termsEnum, int maxDocExclusive) throws IOException {
		if(FpTokenTermLayout.isHotTerm(term_withheader))
		{
			return ;
		}
		
		BytesRef term_noheader=FpTokenTermLayout.removeColumnAndHeaderBytes(term_withheader);
		final PostingsEnum pe = termsEnum.postings(docsEnum_reuse.get(), PostingsEnum.NONE);
		final HashMap<FpTermKey, FPDocList> bucket = commonTermToDocs1;
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
	
	
	public void ingestTermPostings(FpTermKey term_withheader, FPDocList termsEnum, int maxDocExclusive) throws IOException {
		if(FpTokenTermLayout.isHotTerm(term_withheader.bytesRef()))
		{
			return ;
		}
		
		BytesRef term_noheader=FpTokenTermLayout.removeColumnAndHeaderBytes(term_withheader.bytesRef());
		final HashMap<FpTermKey, FPDocList> bucket = commonTermToDocs1;
		final FpTermKey probe = FpTermKey.viewOf(term_noheader);
		 FPDocList acc = bucket.get(probe);
		if (acc == null) {
			acc = new FPDocList(maxDocExclusive);
			bucket.put(probe, acc);
		}
		final FPDocList acc_final=acc;
		termsEnum.foreach(new FpDocListEach() {
			
			@Override
			public void each_doc(int doc) throws IOException {

				if (doc >= 0 && doc < maxDocExclusive) {
					distinctDocUnion.set(doc);
					acc_final.addDoc(doc);
				}
			
				
			}
		});
	
		
		
	}





	public void resetAfterFlush() {
		distinctDocUnion.clear(0, maxDoc);
		FpHashMapPoolHub.release(FpHashMapPoolIds.anchorTierIndexByHotTerm, hotTermDownTierBudget1);
		FpHashMapPoolHub.release(FpHashMapPoolIds.hotTermToDocs, hotTermToDocs1);
		FpHashMapPoolHub.release(FpHashMapPoolIds.commonTermToDocs, commonTermToDocs1);

		
		
	}
}
