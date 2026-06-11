package cn.lxdb.plugins.muqingyu.fptoken.tests.unit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import cn.lxdb.plugins.muqingyu.fptoken.config.FpTokenBlockLevelPolicy;
import cn.lxdb.plugins.muqingyu.fptoken.config.Lucene80FPSearchConfig;

class Lucene80FPSearchConfigTest {

	@Test
	void isFpField_recognizesSuffixes() {
		assertTrue(Lucene80FPSearchConfig.isFpField("content_bfp"));
		assertTrue(Lucene80FPSearchConfig.isFpField("title_sfp"));
		assertFalse(Lucene80FPSearchConfig.isFpField("content_bfp_extra"));
		assertFalse(Lucene80FPSearchConfig.isFpField("bfp"));
		assertFalse(Lucene80FPSearchConfig.isFpField(null));
	}

	@Test
	void ngramBounds() {
		assertEquals(1, Lucene80FPSearchConfig.NGRAM_MIN);
		assertEquals(6, Lucene80FPSearchConfig.NGRAM_MAX);
	}

	@Test
	void bitsetWindowDerivedFromNgramMax() {
		assertEquals(Lucene80FPSearchConfig.BITSET_WINDOW_SIZE - Lucene80FPSearchConfig.NGRAM_MAX,
				Lucene80FPSearchConfig.BITSET_STEP_SIZE);
	}

	@Test
	void commonAccumWarnThreshold_usesOverWriteTopCnt() {
		assertEquals(FpTokenBlockLevelPolicy.OVER_WRITE_TOP_CNT + 2048,
				Lucene80FPSearchConfig.COMMON_ACCUM_WARN_THRESHOLD);
	}

	@Test
	void defaultIndexId_isZero() {
		assertEquals(0, Lucene80FPSearchConfig.DEFAULT_INDEX_ID);
	}
}
