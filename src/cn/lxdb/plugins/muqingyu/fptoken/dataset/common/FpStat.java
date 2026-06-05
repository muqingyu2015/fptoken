package cn.lxdb.plugins.muqingyu.fptoken.dataset.common;

public class FpStat {
	public int flush_common_cnt=0;
	public int flush_high_cnt=0;
	public int flush_high_cnt_original=0;
	public int flush_high_cnt_rebuild=0;
	public long doclist_hot=0;
	public long doclist_common=0;

	@Override
	public String toString() {
		return "commonFlush=" + flush_common_cnt
				+ " highFlush=" + flush_high_cnt
				+ " (original=" + flush_high_cnt_original + " rebuild=" + flush_high_cnt_rebuild + ")"
				+ " doclistHot=" + doclist_hot
				+ " doclistCommon=" + doclist_common;
	}

}
