package cn.lxdb.plugins.muqingyu.fptoken.tests.unit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import cn.lxdb.plugins.muqingyu.fptoken.config.FpTokenBlockLevelPolicy;

class FpTokenBlockLevelPolicyTest {

	@Test
	void resolveTargetBlockLevel_byTermSize() {
		assertEquals(FpTokenBlockLevelPolicy.BLOCK_LEVEL_TOP,
				FpTokenBlockLevelPolicy.resolveTargetBlockLevel(20_000, 10));
		assertEquals(FpTokenBlockLevelPolicy.BLOCK_LEVEL_HIGH,
				FpTokenBlockLevelPolicy.resolveTargetBlockLevel(9_000, 10));
		assertEquals(FpTokenBlockLevelPolicy.BLOCK_LEVEL_MID,
				FpTokenBlockLevelPolicy.resolveTargetBlockLevel(5_000, 10));
		assertEquals(FpTokenBlockLevelPolicy.BLOCK_LEVEL_LOW,
				FpTokenBlockLevelPolicy.resolveTargetBlockLevel(500, 500));
	}

	@Test
	void get_hot_layer_threshold_byLevel() {
		assertEquals(48, FpTokenBlockLevelPolicy.get_hot_layer_threshold(FpTokenBlockLevelPolicy.BLOCK_LEVEL_TOP));
		assertEquals(32, FpTokenBlockLevelPolicy.get_hot_layer_threshold(FpTokenBlockLevelPolicy.BLOCK_LEVEL_HIGH));
		assertEquals(24, FpTokenBlockLevelPolicy.get_hot_layer_threshold(FpTokenBlockLevelPolicy.BLOCK_LEVEL_MID));
		assertEquals(16, FpTokenBlockLevelPolicy.get_hot_layer_threshold(FpTokenBlockLevelPolicy.BLOCK_LEVEL_LOW));
	}

	@Test
	void get_common_to_hot_threshold_byLevel() {
		assertEquals(32, FpTokenBlockLevelPolicy.get_common_to_hot_threshold(FpTokenBlockLevelPolicy.BLOCK_LEVEL_TOP));
		assertEquals(16, FpTokenBlockLevelPolicy.get_common_to_hot_threshold(FpTokenBlockLevelPolicy.BLOCK_LEVEL_HIGH));
		assertEquals(8, FpTokenBlockLevelPolicy.get_common_to_hot_threshold(FpTokenBlockLevelPolicy.BLOCK_LEVEL_MID));
		assertEquals(4, FpTokenBlockLevelPolicy.get_common_to_hot_threshold(FpTokenBlockLevelPolicy.BLOCK_LEVEL_LOW));
	}

	@Test
	void getOverRate_increasesForLowerLevels() {
		assertEquals(1.1, FpTokenBlockLevelPolicy.getOverRate(FpTokenBlockLevelPolicy.BLOCK_LEVEL_TOP), 1e-9);
		assertEquals(1.4, FpTokenBlockLevelPolicy.getOverRate(FpTokenBlockLevelPolicy.BLOCK_LEVEL_LOW), 1e-9);
	}

	@Test
	void shouldCompleteBlock_eitherDimensionSuffices() {
		// level 1 (LOW): docTh=512*5=2560, termTh=512
		assertFalse(FpTokenBlockLevelPolicy.shouldCompleteBlock(1.0, FpTokenBlockLevelPolicy.BLOCK_LEVEL_LOW, 100, 100));
		assertTrue(FpTokenBlockLevelPolicy.shouldCompleteBlock(1.0, FpTokenBlockLevelPolicy.BLOCK_LEVEL_LOW, 3000, 0));
		assertTrue(FpTokenBlockLevelPolicy.shouldCompleteBlock(1.0, FpTokenBlockLevelPolicy.BLOCK_LEVEL_LOW, 0, 600));
		assertTrue(FpTokenBlockLevelPolicy.shouldCompleteBlock(3.0, FpTokenBlockLevelPolicy.BLOCK_LEVEL_LOW, 8000, 0));
	}

	@Test
	void resolveTargetBlockLevel_usesMaxDocWhenLarger() {
		// maxDoc/5 dominates term_size
		assertEquals(FpTokenBlockLevelPolicy.BLOCK_LEVEL_TOP,
				FpTokenBlockLevelPolicy.resolveTargetBlockLevel(100, 100_000));
		assertEquals(FpTokenBlockLevelPolicy.BLOCK_LEVEL_HIGH,
				FpTokenBlockLevelPolicy.resolveTargetBlockLevel(100, 50_000));
	}

	@Test
	void overWriteTopCnt_derivedFromTopLevel() {
		assertEquals((int) (FpTokenBlockLevelPolicy.BLOCK_LEVEL_TOP_CNT
				* FpTokenBlockLevelPolicy.getOverRate(FpTokenBlockLevelPolicy.BLOCK_LEVEL_TOP)),
				FpTokenBlockLevelPolicy.OVER_WRITE_TOP_CNT);
	}

	@Test
	void getOverRate_unknownLevel_defaultsToLowestRate() {
		assertEquals(1.4, FpTokenBlockLevelPolicy.getOverRate(99), 1e-9);
	}
}
