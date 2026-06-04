package cn.lxdb.plugins.muqingyu.fptoken.tests.unit;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

import cn.lxdb.plugins.muqingyu.fptoken.config.Lucene80FPSearchConfig;

class Lucene80FPSearchConfigBucketsTest {

	@Test
	void len1RowUses256Buckets_len2PlusUses512() {
		assertEquals(256, Lucene80FPSearchConfig.bucketsForLengthIndex(0));
		assertEquals(512, Lucene80FPSearchConfig.bucketsForLengthIndex(1));
		assertEquals(512, Lucene80FPSearchConfig.bucketsForLengthIndex(5));
	}

	@Test
	void totalBankPairs_savesOneRowOfWaste() {
		final int uniform512 = Lucene80FPSearchConfig.NGRAM_MAX * Lucene80FPSearchConfig.BUCKETS;
		final int variable = Lucene80FPSearchConfig.totalBankPairs();
		assertEquals(uniform512 - 256, variable);
	}
}
