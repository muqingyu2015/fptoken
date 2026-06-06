package cn.lxdb.plugins.muqingyu.fptoken.tests.unit;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

import cn.lxdb.plugins.muqingyu.fptoken.config.Lucene80FPSearchConfig;

class Lucene80FPSearchConfigBucketsTest {

	@Test
	void allLengthRowsUseUnifiedBucketCount() {
		for (int lenIdx = 0; lenIdx < Lucene80FPSearchConfig.NGRAM_MAX; lenIdx++) {
			assertEquals(Lucene80FPSearchConfig.BUCKETS, Lucene80FPSearchConfig.bucketsForLengthIndex(lenIdx));
		}
	}

	@Test
	void totalBankPairs_isUniformGrid() {
		assertEquals(Lucene80FPSearchConfig.NGRAM_MAX * Lucene80FPSearchConfig.BUCKETS,
				Lucene80FPSearchConfig.totalBankPairs());
	}
}
