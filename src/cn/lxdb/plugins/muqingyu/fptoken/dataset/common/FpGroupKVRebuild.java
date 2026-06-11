package cn.lxdb.plugins.muqingyu.fptoken.dataset.common;

import cn.lxdb.plugins.muqingyu.fptoken.dataset.block.FpGroupDataRebuild;

public class FpGroupKVRebuild {
	public byte[] key;
	public final FpGroupDataRebuild val;
	public FpGroupKVRebuild(byte[] key,int maxDoc)
	{
		this.key=key;
		this.val=new FpGroupDataRebuild(maxDoc);
	}
}
