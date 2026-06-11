package cn.lxdb.plugins.muqingyu.fptoken.dataset.common;

import cn.lxdb.plugins.muqingyu.fptoken.dataset.block.FpGroupDataOriginal;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.block.FpGroupDataRebuild;

public class FpGroupKVOriginal {
	public byte[] key;
	public final FpGroupDataOriginal val;
	public FpGroupKVOriginal(byte[] key,int maxDoc)
	{
		this.key=key;
		this.val=new FpGroupDataOriginal(maxDoc);
	}
}
