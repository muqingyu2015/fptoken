package cn.lxdb.plugins.muqingyu.fptoken.tests.unit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import cn.lxdb.plugins.muqingyu.fptoken.config.Lucene80FPSearchConfig;

class Lucene80FPSearchConfigTest {

	@Test
	void isFpField_recognizesSuffixes() {
		assertTrue(Lucene80FPSearchConfig.isFpField("payload_bfp"));
		assertTrue(Lucene80FPSearchConfig.isFpField("text_sfp"));
		assertFalse(Lucene80FPSearchConfig.isFpField("payload"));
		assertFalse(Lucene80FPSearchConfig.isFpField(null));
	}

	@Test
	void ngramAndBucketConstants_matchSearchWritePaths() {
		assertEquals(1, Lucene80FPSearchConfig.NGRAM_MIN);
		assertEquals(6, Lucene80FPSearchConfig.NGRAM_MAX);
		assertEquals(256, Lucene80FPSearchConfig.BUCKETS);
		assertEquals(32, Lucene80FPSearchConfig.HOT_TIER_TERM_COUNT_THRESHOLD);
		assertEquals((short) 0, Lucene80FPSearchConfig.DEFAULT_INDEX_ID);
	}
}
