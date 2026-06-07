package cn.lxdb.plugins.muqingyu.fptoken.api;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.lucene.codecs.BlockTermState;
import org.apache.lucene.codecs.NormsProducer;
import org.apache.lucene.codecs.blocktree.BlockTreeTermsWriter;
import org.apache.lucene.codecs.blocktree.BlockTreeTermsWriter$TermsWriter;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.slf4j.Logger;

import com.luxindb.lxdb.pool.objectpool.ObjectPoolMulti;

import cn.lucene.lxdb.params.LxdbLogerEncrypt;
import cn.lxdb.plugins.muqingyu.fptoken.config.FpTokenBlockLevelPolicy;
import cn.lxdb.plugins.muqingyu.fptoken.config.Lucene80FPSearchConfig;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FPDocList;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.block.FpGroupDataRebuild;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.block.FpGroupHotNgramBitIndex;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpBlockInfo;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpCommonAccumDiag;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpGroupKVOriginal;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpGroupKVRebuild;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpLog;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpStat;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpTokenTermLayout;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.Utils;

/**
 * FP token 段内写倒排时的块编排。
 * <p>
 * 同一组号（词项前 6 字节）下多 term 共用<strong>唯一</strong>的 {@link FpGroupDataRebuild}（内存中最多缓冲一个可合并组）；
 * 组号变化时上一组<strong>立即</strong>{@link #flushOneGroup} 落盘；同组内若统计已达闭块条件也会在
 * {@link #acceptTerm} 内立刻写出，不依赖 {@link #finish()}。
 * 词项级别 &ge; 目标级别时先入 {@link #high_group_data}，按该组内 {@link FpGroupDataRebuild#distinctDocUnion} 与
 * {@link FpGroupDataRebuild#termCount()} 用 {@link FpTokenBlockLevelPolicy#shouldCompleteBlock(int, int, int)}（以组内最高
 * 级别字节为参数）判定：达标则逐 term 原样 {@code write}；否则降级并入普通 {@link #common_group_data} 对应组。
 */
public final class FpTokenBlockOrchestrator {
    public static final Logger LOG = LxdbLogerEncrypt.getLogger("mqy.fptoken");

	public final BlockTreeTermsWriter blockTreeWriter;
	public final Terms terms;
	public final int maxDoc;
//	public final int targetLevel;
	public final ObjectPoolMulti pool;
	public final BlockTreeTermsWriter$TermsWriter termsWriter;
	public final AtomicLong[] debugList;
	public final NormsProducer norms;
	public final FpStat stat=new FpStat();

	/** 当前可合并链路上「上一组」的 6 字节组号（与 {@link #common_group_data} 对应）；null 表示尚未处理过可合并词项。 */
	private FpGroupKVRebuild group_common=null;

	/** 高级别候选组（整组读入后再判定写或降级）。 */
	private FpGroupKVOriginal group_original=null;
	
	public AtomicInteger groupIndex=new AtomicInteger(0);
	
	public TreeMap<BytesRef,Integer> field_targetlevel=new TreeMap<BytesRef, Integer>();

	
	public final TreeMap<Integer, FpBlockInfo> fpblock_list;

	/** 本字段已写出 term 的上一条（用于乱序检测）。 */
	private BytesRef lastWrittenTerm;

	/** {@link #writefpChecked} 检测到的非严格升序次数。 */
	public long termWriteOrderViolationCount;

	public FpTokenBlockOrchestrator(TreeMap<Integer, FpBlockInfo> fpblock_list,BlockTreeTermsWriter blockTreeWriter, Terms terms, int maxDoc, String fieldName,
			ObjectPoolMulti pool, BlockTreeTermsWriter$TermsWriter termsWriter, AtomicLong[] debugList, NormsProducer norms) throws IOException {
		this.fpblock_list=fpblock_list;
		this.blockTreeWriter = blockTreeWriter;
		this.terms = terms;
		this.maxDoc = Math.max(1, maxDoc);

		
		TreeMap<BytesRef, Integer[]> field_status=new TreeMap<BytesRef, Integer[]>();
		ArrayList<TreeMap<Integer, FpBlockInfo>> listall=terms.getFpblock_listall();
		int cnt1=0;
		int cnt2=0;
		if(listall!=null)
		{
			for(TreeMap<Integer, FpBlockInfo> listsub:listall)
			{
				cnt1++;
				for(Entry<Integer, FpBlockInfo> e:listsub.entrySet())
				{
					cnt2++;
					FpBlockInfo info=e.getValue();
					
					Integer[] fieldInfo=field_status.get(info.fieldInfo);
					if(fieldInfo==null)
					{
						fieldInfo=new Integer[] {0,0};
						field_status.put(info.fieldInfo, fieldInfo);
						
					}
					
					fieldInfo[0]+=info.commonCount;
					fieldInfo[1]+=info.docCount;

					
				}
			}
		}
		
		for(Entry<BytesRef, Integer[]> e:field_status.entrySet())
		{
			Integer[] info=e.getValue();
			int level=FpTokenBlockLevelPolicy.resolveTargetBlockLevel(info[0], info[1]);
			field_targetlevel.put(e.getKey(),level );
			final StringBuilder sb = FpLog.kv();
			FpLog.append(sb, "event", "columnLevelGuess");
			FpLog.append(sb, "column", Utils.BytesReftoString(e.getKey()));
			FpLog.append(sb, "distinctDocs", info[0]);
			FpLog.append(sb, "distinctTerms", info[1]);
			FpLog.append(sb, "segmentsSeen", cnt1);
			FpLog.append(sb, "blocksResolved", cnt2);
			FpLog.append(sb, "targetLevel", "L" + level);
			FpLog.infoLine(LOG, FpLog.TAG_WRITE, sb);
		}

		
		
		
		this.pool = pool;
		this.termsWriter = termsWriter;
		this.debugList = debugList;
		this.norms = norms;
	}



	/**
	 * 在 {@code writefp} 前用 {@link BytesRef#compareTo} 检查是否仍严格升序；乱序只打日志不中断写入。
	 *
	 * @param context 调用来源，如 {@code flushHighGroup_finish}、{@code rebuild:hot}
	 */
	public BlockTermState writefpChecked(BytesRef term, FPDocList postings, String context) throws IOException {
		if (Lucene80FPSearchConfig.CHECK_TERM_WRITE_ORDER) {
			if( lastWrittenTerm != null)
			{
				final int cmp = term.compareTo(lastWrittenTerm);
				if (cmp <= 0) {
					termWriteOrderViolationCount++;
					final StringBuilder sb = FpLog.kv();
					FpLog.append(sb, "event", "termOrderViolation");
					FpLog.append(sb, "seq", termWriteOrderViolationCount);
					FpLog.append(sb, "context", context);
					FpLog.append(sb, "cmp", cmp);
					FpLog.append(sb, "prev", FpTokenTermLayout.toReadableString(lastWrittenTerm));
					FpLog.append(sb, "curr", FpTokenTermLayout.toReadableString(term));
					LOG.error(FpLog.line(FpLog.TAG_WRITE, sb), new IOException());
				}
			}
			lastWrittenTerm = BytesRef.deepCopyOf(term);

			
		}
		return termsWriter.writefp(blockTreeWriter.state, pool, debugList, term, postings, norms);
	}

	public long try_flush_common_cnt=0;

	private final FpCommonAccumDiag commonAccum = new FpCommonAccumDiag();

	public void acceptTerm(BytesRef term, TermsEnum termsEnum) throws IOException {
		boolean high_change_field=group_original != null && !FpTokenTermLayout.column_equals(term, group_original.key);
		boolean common_change_field=group_common!=null&&!FpTokenTermLayout.column_equals(term, group_common.key);
		if (high_change_field||common_change_field) {
			flushHighGroup("changefield");
			flushCommonGroup("changefield");
		}
		
		
		//换组了要刷新high
		if (group_original != null && !FpTokenTermLayout.column_index_group_equals(term, group_original.key)) {
			flushHighGroup("changegroup");
		}
		//换组了要根据情况看是否刷新common
		if (group_common!=null&&!FpTokenTermLayout.column_index_group_equals(term, group_common.key)) {//如果换组了
			tryFlushCommonIfComplete();
		}

		final int termLevel = FpTokenTermLayout.readLevel(term);
		final BytesRef  columName = FpTokenTermLayout.readColumnName(term);
		Integer targetLevel= this.getColumnLevel(columName);//这里如果影响性能，就考虑优化
	
		
		if (termLevel >= targetLevel) {
			//如果这个块本身就已经达到了targetLevel,在检查,如果没有因文档删除导致的降级,直接原封写入节省CPU,否则参与合并
			
			if (group_original == null) {
				group_original = new FpGroupKVOriginal(FpTokenTermLayout.column_index_group_copy(term),maxDoc);
			}
			group_original.val.ingestTermPostings(term, termsEnum, maxDoc);
		
		}else {
			if (group_common == null) {
				group_common = new FpGroupKVRebuild(FpTokenTermLayout.column_index_group_copy(term),maxDoc);
				commonAccum.reset();
			}

			final int sourceGroupId = FpTokenTermLayout.read_group_id(term);
			group_common.val.ingestTermPostings(term, termsEnum, maxDoc);
			commonAccum.recordIngest(sourceGroupId);
			maybeLogCommonAccum("ingest", null, sourceGroupId);
			tryFlushCommonIfCompletePeriodic();
		}


		
	}

	/**
	 * 段合并时 term 常带 {@code group_id!=0}，旧逻辑只在换组 / finish 才闭块，会攒到 8 万+ common。
	 * 每 {@link #try_flush_common_cnt} 次 ingest 后检查；达标用列级阈值，并强制 TOP 上限 {@link FpTokenBlockLevelPolicy#OVER_WRITE_TOP_CNT}。
	 */
	private void tryFlushCommonIfCompletePeriodic() throws IOException {
		try_flush_common_cnt++;
		if (try_flush_common_cnt <= 1024) {
			return;
		}
		try_flush_common_cnt = 0;
		if (group_common == null) {
			return;
		}
	
		tryFlushTopCapIfOver();
		maybeLogCommonAccum("periodic", null, -1);
	}

	/** 列级闭块未触发时，仍按 L4 上限强制 flush，防止 common 超过 ~39321。 */
	private boolean tryFlushTopCapIfOver() throws IOException {
		if (group_common == null) {
			return false;
		}
		
		FpGroupDataRebuild state=group_common.val;
		final int distinctDocs = state.distinctDocUnion.cardinality();
		final int distinctTerms = state.termCount();
		
	
		if (FpTokenBlockLevelPolicy.shouldCompleteBlock(FpTokenBlockLevelPolicy.getOverRate(FpTokenBlockLevelPolicy.BLOCK_LEVEL_TOP),FpTokenBlockLevelPolicy.BLOCK_LEVEL_TOP, distinctDocs, distinctTerms)) {
			flushCommonGroup("top_over");
			return true;
		}
		
		return false;
	}

	private void maybeLogCommonAccum(String trigger, String flushPhase, int lastSourceGroupId) {
		logCommonAccum(trigger, flushPhase, lastSourceGroupId, false);
	}

	private void logCommonAccum(String trigger, String flushPhase, int lastSourceGroupId, boolean force) {
		if (!Lucene80FPSearchConfig.LOG_COMMON_ACCUM_WARN || group_common == null) {
			return;
		}
		final int commonTerms = group_common.val.termCount();
		final int threshold = Lucene80FPSearchConfig.COMMON_ACCUM_WARN_THRESHOLD;
		final int step = Lucene80FPSearchConfig.COMMON_ACCUM_WARN_STEP;
		if (!force && !commonAccum.shouldLogNow(commonTerms, threshold, step)) {
			return;
		}
		if (!force) {
			commonAccum.markLogged(commonTerms);
		}

		final StringBuilder sb = FpLog.kv();
		FpLog.append(sb, "event", "commonAccumWarn");
		FpLog.append(sb, "trigger", trigger);
		if (flushPhase != null) {
			FpLog.append(sb, "flushPhase", flushPhase);
		}
		FpLog.append(sb, "commonTerms", commonTerms);
		FpLog.append(sb, "distinctDocs", group_common.val.distinctDocUnion.cardinality());
		FpLog.append(sb, "groupKey", Utils.BytesReftoString(new BytesRef(group_common.key)));
		if (lastSourceGroupId >= 0) {
			FpLog.append(sb, "lastSourceGroupId", lastSourceGroupId);
			FpLog.append(sb, "lastSourceGroupZero", lastSourceGroupId == 0);
		}
		commonAccum.appendFields(sb);
		LOG.warn(FpLog.line(FpLog.TAG_WRITE, sb));
	}

	private boolean tryFlushCommonIfComplete() throws IOException {
		if (group_common == null) {
			return false;
		}
		FpGroupDataRebuild state=group_common.val;
		final int distinctDocs = state.distinctDocUnion.cardinality();
		final int distinctTerms = state.termCount();
		
		
		final BytesRef  columName = FpTokenTermLayout.readColumnName(new BytesRef(group_common.key));
		Integer targetLevel= this.field_targetlevel.get(columName);//这里如果影响性能，就考虑优化
		String debug_msg="";
		if(targetLevel==null)
		{
			debug_msg="nocolum";
			targetLevel=FpTokenBlockLevelPolicy.BLOCK_LEVEL_LOW;
		}
		if (FpTokenBlockLevelPolicy.shouldCompleteBlock(FpTokenBlockLevelPolicy.getOverRate(targetLevel),targetLevel, distinctDocs, distinctTerms)) {
			flushCommonGroup(debug_msg);
			return true;
		}
		return false;
	}

	public void finish() throws IOException {
		flushHighGroup("finish");
		flushCommonGroup("finish");
		if (termWriteOrderViolationCount > 0) {
			final StringBuilder sb = FpLog.kv();
			FpLog.append(sb, "event", "finishWarn");
			FpLog.append(sb, "termOrderViolations", termWriteOrderViolationCount);
			LOG.warn(FpLog.line(FpLog.TAG_WRITE, sb));
		}
	}

	public int getColumnLevel(final BytesRef  columName)
	{
		Integer targetLevel= this.field_targetlevel.get(columName);//这里如果影响性能，就考虑优化
		if(targetLevel==null)
		{
			targetLevel=FpTokenBlockLevelPolicy.BLOCK_LEVEL_LOW;
		}
		return targetLevel;
	}

	/**
	 * 结束当前高级别候选组：体量达到该组级别要求则原样写出；否则并入同组号的可合并 {@link FpGroupDataRebuild}。
	 */
	private void flushHighGroup(String debugmsg) throws IOException {
		if (group_original == null) {
			group_original = null;
			return;
		}
		this.stat.flush_high_cnt++;

		final int distinctDocs = group_original.val.distinctDocUnion.cardinality();
		final int distinctTerms = group_original.val.termCount();
		
		final BytesRef  columName = FpTokenTermLayout.readColumnName(new BytesRef(group_original.key));
		Integer targetLevel= this.getColumnLevel(columName);//这里如果影响性能，就考虑优化
		
		final boolean meets = FpTokenBlockLevelPolicy.shouldCompleteBlock(1, targetLevel, distinctDocs,distinctTerms);

		boolean needCommonMerger=true;
		if (meets) {//没变化,每降级直接写入,无需重新计算
			final short index_id = FpTokenTermLayout.read_index_id1(group_original.key);
			// 合并前逻辑组：从当前索引读已有位图（与透传 posting 同源）
			final int logical_group = FpTokenTermLayout.readGroupId(group_original.key);
			final FpGroupHotNgramBitIndex bits = this.terms.fpBits(index_id, logical_group, null, null);
			if(bits!=null)
			{
				// 本段新组号：写出倒排头 + fpblock_list + 本次 bit 区（与查询一致）
				final int new_group_id = groupIndex.incrementAndGet();
				group_original.val.flushto(this, bits, new_group_id,group_original.key,"flushHighGroup_"+debugmsg);

				this.stat.flush_high_cnt_original++;
				needCommonMerger=false;
			}else {
				final StringBuilder sb = FpLog.kv();
				FpLog.append(sb, "event", "missingBitIndex");
				FpLog.append(sb, "indexId", index_id);
				FpLog.append(sb, "logicalGroup", logical_group);
				FpLog.append(sb, "action", "downgradeToRebuild");
				LOG.error(FpLog.line(FpLog.TAG_WRITE, sb));
			}
			
		} 
		if(needCommonMerger){
			if (group_common == null) {
				group_common = new FpGroupKVRebuild(FpTokenTermLayout.column_index_group_copy(new BytesRef(group_original.key)),maxDoc);
				commonAccum.reset();
			}
			final int mergeSourceGroupId = FpTokenTermLayout.read_group_id(new BytesRef(group_original.key));
			final int mergeHotTerms = group_original.val.hotTermMapInternal().size();
			final int mergeCommonTerms = group_original.val.termCount();
			commonAccum.recordMergeBatch(mergeSourceGroupId, mergeHotTerms, mergeCommonTerms);
			group_original.val.mergeIntoRebuild(group_common.val);

			this.stat.flush_high_cnt_rebuild++;

			maybeLogCommonAccum("highMerge", null, mergeSourceGroupId);
			tryFlushCommonIfComplete();
		}
		group_original = null;

	}



	private void flushCommonGroup(String debugmsg) throws IOException {
		
		try_flush_common_cnt=0;
		if (group_common == null ) {
			group_common = null;
			return;
		}

		if (group_common.val.termCount() >= Lucene80FPSearchConfig.COMMON_ACCUM_WARN_THRESHOLD) {
			logCommonAccum("flush", debugmsg, -1, true);
		}

		group_common.val.flushto(this,group_common.key,"flushCommonGroup_"+debugmsg);

		this.stat.flush_common_cnt++;
		group_common = null;
		commonAccum.reset();
	}

}
