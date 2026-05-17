package cn.lxdb.plugins.muqingyu.fptoken.tests.unit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/**
 * 与 {@link cn.lxdb.plugins.muqingyu.fptoken.config.FpTokenBlockLevelPolicy} 行为对齐的镜像断言。
 * 该类因依赖未随 {@code lib/lxdb_common-*.jar} 一并发布的 Lucene 补丁类型，当前未编入主代码编译单元。
 */
class FpTokenBlockLevelPolicyTest {

	private static int resolveTargetBlockLevel(int maxDoc, long termSize) {
		long check = Math.max(maxDoc, termSize);
		return check >= 100_000 ? 3 : 1;
	}

	private static boolean shouldCompleteBlock(double rate, int targetLevel, int distinctDocs, int distinctTerms) {
		int docTh = targetLevel == 3 ? 100_000 : 1_000;
		int termTh = docTh;
		return distinctDocs >= (docTh * rate) || distinctTerms >= (termTh * rate);
	}

	@Test
	void resolveTargetBlockLevel_largeSegment() {
		assertEquals(3, resolveTargetBlockLevel(200_000, 10));
		assertEquals(1, resolveTargetBlockLevel(500, 500));
	}

	@Test
	void shouldCompleteBlock_eitherDimensionSuffices() {
		assertTrue(shouldCompleteBlock(1.0, 1, 1000, 0));
		assertTrue(shouldCompleteBlock(1.0, 1, 0, 1000));
		assertFalse(shouldCompleteBlock(1.0, 1, 999, 999));
		assertTrue(shouldCompleteBlock(3.0, 1, 3000, 0));
	}
}
