package cn.lxdb.plugins.muqingyu.fptoken.dataset.common;

import java.util.Arrays;

import cn.lxdb.plugins.muqingyu.fptoken.config.FpTokenBlockLevelPolicy;

public class FpSearchStat {
	
	public long doccount=0;
	
	@Override
	public String toString() {
		return "FpSearchStat [doccount=" + doccount + ", commonhit=" + commonhit + ", hothit=" + hothit + ", blkCount="
				+ Arrays.toString(blkCount) + ", bitHitHot=" + Arrays.toString(bitHitHot) + ", blkHitHot="
				+ Arrays.toString(blkHitHot) + ", bitHitCommon=" + Arrays.toString(bitHitCommon) + ", blkHitCommon="
				+ Arrays.toString(blkHitCommon) + ", termHitHot=" + Arrays.toString(termHitHot) + ", termHitCommon="
				+ Arrays.toString(termHitCommon) + ", termMissHot1=" + Arrays.toString(termMissHot1)
				+ ", termMissCommon1=" + Arrays.toString(termMissCommon1) + ", termMissHot2="
				+ Arrays.toString(termMissHot2) + ", termMissCommon2=" + Arrays.toString(termMissCommon2)
				+ ", termMissHot3=" + Arrays.toString(termMissHot3) + ", termMissCommon3="
				+ Arrays.toString(termMissCommon3) + ", termMiss0=" + termMiss0 + ", termHit0=" + termHit0 + "]";
	}
	public long commonhit=0;
	public long hothit=0;

	public long[] blkCount=new long[FpTokenBlockLevelPolicy.BLOCK_LEVEL_HIGH+1];
	
	public long[] bitHitHot=new long[FpTokenBlockLevelPolicy.BLOCK_LEVEL_HIGH+1];
	public long[] blkHitHot=new long[FpTokenBlockLevelPolicy.BLOCK_LEVEL_HIGH+1];
	
	public long[] bitHitCommon=new long[FpTokenBlockLevelPolicy.BLOCK_LEVEL_HIGH+1];
	public long[] blkHitCommon=new long[FpTokenBlockLevelPolicy.BLOCK_LEVEL_HIGH+1];
	
	public long[] termHitHot=new long[FpTokenBlockLevelPolicy.BLOCK_LEVEL_HIGH+1];
	public long[] termHitCommon=new long[FpTokenBlockLevelPolicy.BLOCK_LEVEL_HIGH+1];

	public long[] termMissHot1=new long[FpTokenBlockLevelPolicy.BLOCK_LEVEL_HIGH+1];
	public long[] termMissCommon1=new long[FpTokenBlockLevelPolicy.BLOCK_LEVEL_HIGH+1];
	
	public long[] termMissHot2=new long[FpTokenBlockLevelPolicy.BLOCK_LEVEL_HIGH+1];
	public long[] termMissCommon2=new long[FpTokenBlockLevelPolicy.BLOCK_LEVEL_HIGH+1];
	
	public long[] termMissHot3=new long[FpTokenBlockLevelPolicy.BLOCK_LEVEL_HIGH+1];
	public long[] termMissCommon3=new long[FpTokenBlockLevelPolicy.BLOCK_LEVEL_HIGH+1];
	
	public long termMiss0=0;
	public long termHit0=0;


}
