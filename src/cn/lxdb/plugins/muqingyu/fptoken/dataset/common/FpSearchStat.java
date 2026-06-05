package cn.lxdb.plugins.muqingyu.fptoken.dataset.common;

import cn.lxdb.plugins.muqingyu.fptoken.config.FpTokenBlockLevelPolicy;

public class FpSearchStat {
	
	public long doccount=0;
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

	@Override
	public String toString() {
		final StringBuilder sb = new StringBuilder(256);
		sb.append("segmentDocs=").append(doccount);
		sb.append(" hotHitDocs=").append(hothit);
		sb.append(" commonHitDocs=").append(commonhit);
		appendTierFunnel(sb, "hot", blkCount, bitHitHot, blkHitHot, termHitHot, termMissHot1, termMissHot2,
				termMissHot3);
		appendTierFunnel(sb, "common", blkCount, bitHitCommon, blkHitCommon, termHitCommon, termMissCommon1,
				termMissCommon2, termMissCommon3);
		if (termHit0 > 0 || termMiss0 > 0) {
			sb.append(" sparse{termHit=").append(termHit0).append(" termMiss=").append(termMiss0).append('}');
		}
		return sb.toString();
	}

	private static void appendTierFunnel(StringBuilder sb, String tier, long[] blkCount, long[] bitHit,
			long[] blkHit, long[] termHit, long[] seekMiss, long[] headerMiss, long[] payloadMiss) {
		for (int lvl = 0; lvl <= FpTokenBlockLevelPolicy.BLOCK_LEVEL_HIGH; lvl++) {
			if (bitHit[lvl] == 0 && termHit[lvl] == 0 && payloadMiss[lvl] == 0 && blkCount[lvl] == 0) {
				continue;
			}
			sb.append(' ').append(tier).append(".L").append(lvl).append('{');
			sb.append("blocks=").append(blkCount[lvl]);
			sb.append(" bitCand=").append(bitHit[lvl]);
			sb.append(" groups=").append(blkHit[lvl]);
			sb.append(" termHit=").append(termHit[lvl]);
			sb.append(" seekMiss=").append(seekMiss[lvl]);
			sb.append(" headerMiss=").append(headerMiss[lvl]);
			sb.append(" payloadMiss=").append(payloadMiss[lvl]);
			sb.append('}');
		}
	}

}
