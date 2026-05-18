package cn.lxdb.plugins.muqingyu.fptoken.api;

import java.io.IOException;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

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
import cn.lxdb.plugins.muqingyu.fptoken.dataset.block.FpGroupDataRebuild;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.block.FpGroupHotNgramBitIndex;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpBlockInfo;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpGroupKVOriginal;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpGroupKVRebuild;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpTokenTermLayout;

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
	public final int targetLevel;
	public final ObjectPoolMulti pool;
	public final BlockTreeTermsWriter$TermsWriter termsWriter;
	public final AtomicLong[] debugList;
	public final NormsProducer norms;

	/** 当前可合并链路上「上一组」的 6 字节组号（与 {@link #common_group_data} 对应）；null 表示尚未处理过可合并词项。 */
	private FpGroupKVRebuild group_common=null;

	/** 高级别候选组（整组读入后再判定写或降级）。 */
	private FpGroupKVOriginal group_original=null;
	
	public AtomicInteger groupIndex=new AtomicInteger(0);
	
	public final TreeMap<Integer, FpBlockInfo> fpblock_list;
	public FpTokenBlockOrchestrator(TreeMap<Integer, FpBlockInfo> fpblock_list,BlockTreeTermsWriter blockTreeWriter, Terms terms, int maxDoc, String fieldName,
			ObjectPoolMulti pool, BlockTreeTermsWriter$TermsWriter termsWriter, AtomicLong[] debugList, NormsProducer norms) throws IOException {
		this.fpblock_list=fpblock_list;
		this.blockTreeWriter = blockTreeWriter;
		this.terms = terms;
		this.maxDoc = Math.max(1, maxDoc);
		long termGuess;
		try {
			termGuess = terms.guess_size();
		} catch (Throwable e) {
			termGuess = 0;
		}
		this.targetLevel = FpTokenBlockLevelPolicy.resolveTargetBlockLevel(maxDoc, termGuess);
		this.pool = pool;
		this.termsWriter = termsWriter;
		this.debugList = debugList;
		this.norms = norms;
	}



	public void acceptTerm(BytesRef term, TermsEnum termsEnum) throws IOException {
	
		
		//换组了要刷新high
		if (group_original != null && !FpTokenTermLayout.indexAndGroupEquals(term, group_original.key)) {
			flushHighGroup();
		}
		//换组了要根据情况看是否刷新common
		if (group_common!=null&&!FpTokenTermLayout.indexAndGroupEquals(term, group_common.key)) {//如果换组了
			tryFlushCommonIfComplete();
		}

		final int termLevel = FpTokenTermLayout.readLevel(term);
		if (termLevel >= targetLevel) {
			//如果这个块本身就已经达到了targetLevel,在检查,如果没有因文档删除导致的降级,直接原封写入节省CPU,否则参与合并
			
			if (group_original == null) {
				group_original = new FpGroupKVOriginal(maxDoc);
				FpTokenTermLayout.copyIndexAndGroup(term, group_original.key);
			}
			group_original.val.ingestTermPostings(term, termsEnum, maxDoc);
		
		}else {
			if (group_common == null) {
				group_common = new FpGroupKVRebuild(maxDoc);
				FpTokenTermLayout.copyIndexAndGroup(term, group_common.key);
			}
			
			group_common.val.ingestTermPostings(term, termsEnum, maxDoc);
		}


		
	}
	
	private boolean tryFlushCommonIfComplete() throws IOException {
		FpGroupDataRebuild state=group_common.val;
		final int distinctDocs = state.distinctDocUnion.cardinality();
		final int distinctTerms = state.termCount();
		if (FpTokenBlockLevelPolicy.shouldCompleteBlock(3,targetLevel, distinctDocs, distinctTerms)) {
			flushCommonGroup();
			return true;
		}
		return false;
	}

	public void finish() throws IOException {
		flushHighGroup();
		flushCommonGroup();
	}


	/**
	 * 结束当前高级别候选组：体量达到该组级别要求则原样写出；否则并入同组号的可合并 {@link FpGroupDataRebuild}。
	 */
	private void flushHighGroup() throws IOException {
		if (group_original == null) {
			group_original = null;
			return;
		}
		final int distinctDocs = group_original.val.distinctDocUnion.cardinality();
		final int distinctTerms = group_original.val.termCount();
		final boolean meets = FpTokenBlockLevelPolicy.shouldCompleteBlock(1, this.targetLevel, distinctDocs,distinctTerms);

		if (meets) {//没变化,每降级直接写入,无需重新计算
			
			int index_id=FpTokenTermLayout.read_index_id(group_original.key);
			int group_id=FpTokenTermLayout.read_group_id(group_original.key);

			FpGroupHotNgramBitIndex bits=this.terms.fpBits((short) index_id, group_id, null, null);
			group_original.val.flushto(this,bits);
			
			LOG.info("flushHighGroup:flushto");
		} else {
			if (group_common == null) {
				group_common = new FpGroupKVRebuild(maxDoc);
				FpTokenTermLayout.copyIndexAndGroup(new BytesRef(group_original.key), group_common.key);
			}
			group_original.val.mergeInto(group_common.val);
			LOG.info("flushHighGroup:mergeInto");

			tryFlushCommonIfComplete();
		}
		group_original = null;

	}



	private void flushCommonGroup() throws IOException {
		if (group_common == null ) {
			group_common = null;
			return;
		}
		
		group_common.val.flushto(this,false);
		LOG.info("flushCommonGroup:flushto");

		group_common = null;
	}

}
