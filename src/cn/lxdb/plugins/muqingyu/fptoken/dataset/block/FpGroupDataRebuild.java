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
import cn.lxdb.plugins.muqingyu.fptoken.config.FpTokenBlockLevelPolicy;
import cn.lxdb.plugins.muqingyu.fptoken.config.Lucene80FPSearchConfig;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FPDocList;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpBlockInfo;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpLog;
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
	/** 热词表，键序见 {@link FpTermKey#ORDER_BY_LENGTH_THEN_BYTES}。 */
	public final TreeMap<FpTermKey, FPDocList> hotTermToDocs = new TreeMap<>(FpTermKey.ORDER_BY_LENGTH_THEN_BYTES);
	/**
	 * 热词锚点「向下扩展档」预算（≥1，含锚点档），由 {@link FpGroupHotNgramRebuild} 写入词项头 offset 12。
	 * 与 {@link FpTokenTermLayout#maxHotPayloadLen(int, int)}、检索热词长度截断一致。
	 */
	public final TreeMap<FpTermKey, Integer> hotTermDownTierBudget = new TreeMap<>(FpTermKey.ORDER_BY_LENGTH_THEN_BYTES);
	public final TreeMap<FpTermKey, Integer> hotTermToOrder = new TreeMap<>(FpTermKey.ORDER_BY_LENGTH_THEN_BYTES);

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

	public TreeMap<FpTermKey, Integer> hotTermDownTierBudgetInternal() {
		return hotTermDownTierBudget;
	}
	
	public TreeMap<FpTermKey, Integer> commonTermOrderInternal() {
		return commonTermToOrder;
	}

	/** 清空 {@link #hotTermToOrder}，再按 {@link #hotTermToDocs} 键序（{@link FpTermKey#ORDER_BY_LENGTH_THEN_BYTES}）从 1 起连续编号。 */
	public void rebuildHotTermOrderFromHotDocs() {
		hotTermToOrder.clear();
		int order = 1;
		for (FpTermKey key : hotTermToDocs.keySet()) {
			hotTermToOrder.put(key, order++);
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



	public void flushto(FpTokenBlockOrchestrator parentItem, byte[] groupkey,String debug_msg) throws IOException {
		long ts_begin=CLMillisecondClock.CLOCK.now();

		final BytesRef columnName = FpTokenTermLayout.readColumnName(new BytesRef(groupkey));
		
		if(commonTermToDocs.size()<=FpTokenBlockLevelPolicy.NO_INDEX_THRESHOLD)
		{
			//如果这个field的term数量很少，则采用暴力遍历，用于解决稀疏列
			this.rebuildCommonTermToOrderFromHotDocs();

			int stat_del_hotterm_cnt=0;
			long stat_common_doc_cnt=0;

			final byte[] reuse_bytes = new byte[1024];
			BytesRef reuse_term=new BytesRef(reuse_bytes);
			int group_id=parentItem.groupIndex.incrementAndGet();
			
			
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
			FpLog.append(sbZero, "commonTerms", commonTermToDocs.size());
			FpLog.infoLine(LOG, FpLog.TAG_REBUILD, sbZero);

		
		}else {
			FpStatNgram ngramstat=FpGroupHotNgramRebuild.execute(this, parentItem);
			long ts_ngram=CLMillisecondClock.CLOCK.now();

			
			this.rebuildHotTermOrderFromHotDocs();
			this.rebuildCommonTermToOrderFromHotDocs();
			int columnLevel=FpTokenBlockLevelPolicy.resolveTargetBlockLevel(this.commonTermToDocs.size(), distinctDocUnion.cardinality());

			FpGroupHotNgramBitIndex bitinfo=FpGroupHotNgramBitIndex.execute(columnLevel,this);
			long ts_bitset=CLMillisecondClock.CLOCK.now();

			int del_term_docid=distinctDocUnion.nextSetBit(0);
			int stat_del_hotterm_cnt=0;
			long stat_hot_doc_cnt=0;
			long stat_common_doc_cnt=0;

			final byte[] reuse_bytes = new byte[1024];
			BytesRef reuse_term=new BytesRef(reuse_bytes);
			int group_id=parentItem.groupIndex.incrementAndGet();
			for (Entry<FpTermKey, FPDocList> e : hotTermToDocs.entrySet()) {
				final FpTermKey key = e.getKey();
				final Integer budgetObj = hotTermDownTierBudget.get(key);
				final int downTierBudget = budgetObj != null ? budgetObj.intValue() : 0;
				final FPDocList val = e.getValue();
				final Integer index = hotTermToOrder.get(key);
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
			

			FpBlockInfo blkinfo=bitinfo.flushto(parentItem.blockTreeWriter.bitOut,"rebuild",columnName,del_term_docid);
			parentItem.fpblock_list.put(group_id, blkinfo);
		
			parentItem.stat.doclist_hot+=stat_hot_doc_cnt;
			parentItem.stat.doclist_common+=stat_common_doc_cnt;

		
			long ts_end=CLMillisecondClock.CLOCK.now();

			final StringBuilder sbFlush = FpLog.kv();
			FpLog.append(sbFlush, "event", "flush");
			FpLog.append(sbFlush, "phase", debug_msg);
			FpLog.append(sbFlush, "groupId", group_id);
			FpLog.append(sbFlush, "msTotal", ts_end - ts_begin);
			FpLog.append(sbFlush, "msNgram", ts_ngram - ts_begin);
			FpLog.append(sbFlush, "msBitset", ts_bitset - ts_ngram);
			FpLog.append(sbFlush, "targetLevel", "L" + columnLevel);
			FpLog.append(sbFlush, "doclistHot", stat_hot_doc_cnt);
			FpLog.append(sbFlush, "doclistCommon", stat_common_doc_cnt);
			FpLog.append(sbFlush, "delHotTerms", stat_del_hotterm_cnt);
			FpLog.append(sbFlush, "distinctDocs", distinctDocUnion.cardinality());
			FpLog.append(sbFlush, "hotTerms", hotTermToDocs.size());
			FpLog.append(sbFlush, "commonTerms", commonTermToDocs.size());
			FpLog.append(sbFlush, "ngramStat", ngramstat);
			FpLog.append(sbFlush, "block", blkinfo);
			FpLog.infoLine(LOG, FpLog.TAG_REBUILD, sbFlush);

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
		hotTermDownTierBudget.clear();
		hotTermToOrder.clear();
		commonTermToDocs.clear();
		commonTermToOrder.clear();
		distinctDocUnion.clear(0, maxDoc);
	}
}
