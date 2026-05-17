package cn.lxdb.plugins.muqingyu.fptoken.tests.unit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import cn.lxdb.plugins.muqingyu.fptoken.config.FpTokenBlockLevelPolicy;

class FpTokenBlockLevelPolicyTest {

	@Test
	void resolveTargetBlockLevel_largeSegment() {
		assertEquals(FpTokenBlockLevelPolicy.BLOCK_LEVEL_HIGH,
				FpTokenBlockLevelPolicy.resolveTargetBlockLevel(200_000, 10));
		assertEquals(FpTokenBlockLevelPolicy.BLOCK_LEVEL_LOW,
				FpTokenBlockLevelPolicy.resolveTargetBlockLevel(500, 500));
	}

	@Test
	void shouldCompleteBlock_eitherDimensionSuffices() {
		assertTrue(FpTokenBlockLevelPolicy.shouldCompleteBlock(1.0, 1, 1000, 0));
		assertTrue(FpTokenBlockLevelPolicy.shouldCompleteBlock(1.0, 1, 0, 1000));
		assertFalse(FpTokenBlockLevelPolicy.shouldCompleteBlock(1.0, 1, 999, 999));
		assertTrue(FpTokenBlockLevelPolicy.shouldCompleteBlock(3.0, 1, 3000, 0));
	}
}
