package cn.lxdb.plugins.muqingyu.fptoken.api;

import java.io.IOException;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.lucene.codecs.NormsProducer;
import org.apache.lucene.codecs.blocktree.BlockTreeTermsWriter;
import org.apache.lucene.codecs.blocktree.BlockTreeTermsWriter$TermsWriter;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.slf4j.Logger;

import com.luxindb.lxdb.pool.objectpool.ObjectPoolMulti;

import cn.lucene.lxdb.params.LxdbLogerEncrypt;
import cn.lucene.proguard.keep.lxdb.common.CLMillisecondClock;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpBlockInfo;

/**
 * BlockTree 写词项入口的 FP 扩展：在写某个字段的 {@link Terms} 时，使用 {@link FpTokenBlockOrchestrator}
 * 按组号与级别做合并/透传编排，再调用底层 {@link TermsWriter#write}。
 */
public class FpTermsWriter {
    public static final Logger LOG = LxdbLogerEncrypt.getLogger("mqy.fptoken");

	/** 被委托的 Lucene BlockTree 写实例（持有 codec 状态等）。 */
	public BlockTreeTermsWriter termWriter;
	public IndexOutput bitOut;
	public TreeMap<Integer, FpBlockInfo> fpblock_list=new TreeMap<Integer, FpBlockInfo>();

	/**
	 * @param w 底层 {@link BlockTreeTermsWriter}
	 */
	public FpTermsWriter(BlockTreeTermsWriter w,IndexOutput bitOut) {
		this.termWriter = w;
		this.bitOut=bitOut;
	}

	/**
	 * 写完整字段词表，不接收块完成回调。
	 *
	 * @see #writeTerms(int, ObjectPoolMulti, Terms, TermsWriter, FieldInfos, String, AtomicLong[], NormsProducer, FpTokenBlockFinishListener)
	 */
	public void writeTerms(int maxDoc, ObjectPoolMulti pool, Terms terms, org.apache.lucene.codecs.blocktree.BlockTreeTermsWriter$TermsWriter termsWriter,
			FieldInfos fieldInfos, String field, AtomicLong[] debug_list, NormsProducer norms) throws IOException {
		long ts_begin=CLMillisecondClock.CLOCK.now();


		final TermsEnum termsEnum = terms.iterator_fp();
		final FpTokenBlockOrchestrator orchestrator = new FpTokenBlockOrchestrator(fpblock_list,termWriter, terms, maxDoc, field, pool,
				termsWriter, debug_list, norms);

		long term_cnt=0;
		while (true) {
			BytesRef term = termsEnum.next();
			if (term == null) {
				break;
			}
			term_cnt++;
			orchestrator.acceptTerm(term, termsEnum);
		}
		orchestrator.finish();
		

		long ts_end=CLMillisecondClock.CLOCK.now();

		LOG.info("writeTerms diff:"+(ts_end-ts_begin)+"ms,term_cnt:"+term_cnt+",maxDoc:"+maxDoc+","+orchestrator.stat);

	
	}

	


	public void close() throws IOException {
		// 包装类自身无资源；底层 writer 由外层管理
	}
}
