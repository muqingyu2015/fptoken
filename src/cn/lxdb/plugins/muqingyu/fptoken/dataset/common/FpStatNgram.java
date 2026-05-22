package cn.lxdb.plugins.muqingyu.fptoken.dataset.common;

import java.util.Arrays;
import java.util.TreeMap;

import cn.lxdb.plugins.muqingyu.fptoken.config.Lucene80FPSearchConfig;

public class FpStatNgram {

	@Override
	public String toString() {
		return "[term_cnt=" + term_cnt + ", token_cnt=" + token_cnt + ", freqThreshold_keep="
				+ freqThreshold_keep + ", freqThreshold_skip=" + freqThreshold_skip + ", hot_doc_cnt_keep="
				+ hot_doc_cnt_keep + ", hot_doc_cnt_skip=" + hot_doc_cnt_skip + ", ngram_level_ok=" + ngram_level_ok
				+ ", ngram_level_skip=" + ngram_level_skip + ", term_level_cnt=" + getLevelString()
				+ "]";
	}
	public long term_cnt=0;
	public long token_cnt=0;
	public long freqThreshold_keep=0;
	public long freqThreshold_skip=0;
	
	public long hot_doc_cnt_keep=0;
	public long hot_doc_cnt_skip=0;

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
