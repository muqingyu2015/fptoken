package cn.lxdb.plugins.muqingyu.fptoken.dataset.common;

import cn.lxdb.plugins.muqingyu.fptoken.dataset.block.FpGroupDataRebuild;

public class FpGroupKVRebuild {
	public byte[] key=	new byte[FpTokenTermLayout.INDEX_AND_GROUP_BYTES];
	public final FpGroupDataRebuild val;
	public FpGroupKVRebuild(int maxDoc)
	{
		this.val=new FpGroupDataRebuild(maxDoc);
	}
}
