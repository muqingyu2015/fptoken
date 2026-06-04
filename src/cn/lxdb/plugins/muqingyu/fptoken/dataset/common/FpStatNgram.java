package cn.lxdb.plugins.muqingyu.fptoken.dataset.common;

import java.util.Arrays;
import java.util.TreeMap;

import cn.lxdb.plugins.muqingyu.fptoken.config.Lucene80FPSearchConfig;

public class FpStatNgram {

	@Override
	public String toString() {
		return "[term_cnt=" + term_cnt + ", token_cnt=" + token_cnt + ", distinct_ngram=" + distinct_ngram
				+ ", hot_pending=" + hot_pending + ", hot_final=" + hot_final + ", budget_entries=" + budget_entries
				+ ", freqThreshold_keep=" + freqThreshold_keep + ", freqThreshold_skip=" + freqThreshold_skip
				+ ", hot_doc_cnt_keep=" + hot_doc_cnt_keep + ", hot_doc_cnt_skip=" + hot_doc_cnt_skip
				+ ", hot_doclist_sparse=" + hot_doclist_sparse + "/" + hot_pending
				+ ", ngram_level_ok=" + ngram_level_ok + ", ngram_level_skip=" + ngram_level_skip + ", phases_ms="
				+ ms_count + "+" + ms_build + "+" + ms_budget + "+" + ms_merge + ", term_level_cnt=" + getLevelString()
				+ "]";
	}
	public long term_cnt=0;
	public long token_cnt=0;
	/** {@code countNgramOccurrencesInCommon} 后 {@code ngramOccurrenceCount.size()} */
	public int distinct_ngram=0;
	/** {@code buildHotTerms} 后待 merge 热词数 */
	public int hot_pending=0;
	/** {@code putAll} 后进 {@code hotTermToDocs} 的热词数 */
	public int hot_final=0;
	/** {@code hotTermDownTierBudget} 条数 */
	public int budget_entries=0;
	public long ms_count=0;
	public long ms_build=0;
	public long ms_budget=0;
	public long ms_merge=0;

	public long freqThreshold_keep=0;
	public long freqThreshold_skip=0;
	
	public long hot_doc_cnt_keep=0;
	public long hot_doc_cnt_skip=0;
	/** merge 结束后以 {@link org.apache.lucene.util.SparseFixedBitSet} 存放 doc 的热词条数 */
	public int hot_doclist_sparse=0;

	public long ngram_level_ok=0;
	public long ngram_level_skip=0;
	public long[][] term_level_cnt=mkLevel();
	public String getLevelString()
	{
		StringBuffer buff=new StringBuffer();
		for(int i=0;i<term_level_cnt.length;i++)
		{
			for(int j=0;j<term_level_cnt[i].length;j++)
			{
				if(term_level_cnt[i][j]>0)
				{
					buff.append("["+i+":"+j+"="+term_level_cnt[i][j]+"]");
				}
			}
		}
		
		return buff.toString();
	}
	
	public static long[][] mkLevel()
	{
		long[][] term_level_cnt=new long[Lucene80FPSearchConfig.NGRAM_MAX*2][Lucene80FPSearchConfig.NGRAM_MAX*2];
		for(int i=0;i<term_level_cnt.length;i++)
		{
			Arrays.fill(term_level_cnt[i], 0);
		}
		
		return term_level_cnt;
		
	}

}
