package cn.lxdb.plugins.muqingyu.fptoken.dataset.common;

public class FpStatNgram {
	@Override
	public String toString() {
		return "[term_cnt=" + term_cnt + ", token_cnt=" + token_cnt + ", freqThreshold_keep="
				+ freqThreshold_keep + ", freqThreshold_skip=" + freqThreshold_skip + ", hot_doc_cnt_keep="
				+ hot_doc_cnt_keep + ", hot_doc_cnt_skip=" + hot_doc_cnt_skip + "]";
	}
	public long term_cnt=0;
	public long token_cnt=0;
	public long freqThreshold_keep=0;
	public long freqThreshold_skip=0;
	
	public long hot_doc_cnt_keep=0;
	public long hot_doc_cnt_skip=0;

	public long ngram_level_ok=0;
	public long ngram_level_skip=0;


}
