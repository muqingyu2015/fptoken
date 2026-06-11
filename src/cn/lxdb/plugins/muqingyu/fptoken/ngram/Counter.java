package cn.lxdb.plugins.muqingyu.fptoken.ngram;

import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpTermKey;

public class Counter {
	public int cnt;
	public FpTermKey key;
	public Counter(int cnt,FpTermKey key) {
		this.cnt = cnt;
		this.key=key;
	}
}
