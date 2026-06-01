package cn.lxdb.plugins.muqingyu.fptoken.dataset.common;

public class FpStat {
	@Override
	public String toString() {
		return "[flush_common_cnt=" + flush_common_cnt + ", flush_high_cnt="
				+ flush_high_cnt + ", flush_high_cnt_original=" + flush_high_cnt_original + ", flush_high_cnt_rebuild="
				+ flush_high_cnt_rebuild + ", doclist_hot=" + doclist_hot + ", doclist_common=" + doclist_common + "]";
	}
	public int flush_common_cnt=0;
	public int flush_high_cnt=0;
	public int flush_high_cnt_original=0;
	public int flush_high_cnt_rebuild=0;
	public long doclist_hot=0;
	public long doclist_common=0;


}
