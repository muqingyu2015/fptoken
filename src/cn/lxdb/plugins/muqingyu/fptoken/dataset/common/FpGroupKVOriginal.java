package cn.lxdb.plugins.muqingyu.fptoken.dataset.common;

import cn.lxdb.plugins.muqingyu.fptoken.dataset.block.FpGroupDataOriginal;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.block.FpGroupDataRebuild;

public class FpGroupKVOriginal {
	public byte[] key=	new byte[FpTokenTermLayout.INDEX_AND_GROUP_BYTES];
	public final FpGroupDataOriginal val;
	public FpGroupKVOriginal(int maxDoc)
	{
		this.val=new FpGroupDataOriginal(maxDoc);
	}
}
