package cn.lxdb.plugins.muqingyu.fptoken.tests.unit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import cn.lxdb.plugins.muqingyu.fptoken.config.FpTokenBlockLevelPolicy;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpSearchStat;

class FpSearchStatTest {

	@Test
	void toString_includesTierFunnel() {
		FpSearchStat s = new FpSearchStat();
		s.doccount = 100;
		s.hothit = 10;
		s.commonhit = 20;
		s.blkCount[1] = 3;
		s.bitHitHot[1] = 2;
		s.termHitHot[1] = 1;
		String t = s.toString();
		assertTrue(t.contains("segmentDocs=100"));
		assertTrue(t.contains("hotHitDocs=10"));
		assertTrue(t.contains("commonHitDocs=20"));
		assertTrue(t.contains("hot.L1{"));
	}

	@Test
	void arraysSizedForTopLevel() {
		FpSearchStat s = new FpSearchStat();
		assertEquals(FpTokenBlockLevelPolicy.BLOCK_LEVEL_TOP + 1, s.blkCount.length);
	}

	@Test
	void toString_includesCommonTierAndSparse() {
		FpSearchStat s = new FpSearchStat();
		s.blkCount[2] = 1;
		s.bitHitCommon[2] = 2;
		s.termHitCommon[2] = 3;
		s.termHit0 = 4;
		s.termMiss0 = 5;
		String t = s.toString();
		assertTrue(t.contains("common.L2{"));
		assertTrue(t.contains("sparse{termHit=4 termMiss=5}"));
	}
}
