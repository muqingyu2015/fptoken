package cn.lxdb.plugins.muqingyu.fptoken.tests.unit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import cn.lxdb.plugins.muqingyu.fptoken.config.Lucene80FPSearchConfig;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpStatNgram;

class FpStatNgramTest {

	@Test
	void mkLevel_dimensions() {
		long[][] m = FpStatNgram.mkLevel();
		assertEquals(Lucene80FPSearchConfig.NGRAM_MAX * 2, m.length);
		assertEquals(Lucene80FPSearchConfig.NGRAM_MAX * 2, m[0].length);
	}

	@Test
	void getLevelString_formatsNonZeroCells() {
		FpStatNgram s = new FpStatNgram();
		s.term_level_cnt[2][3] = 5;
		assertEquals("len2@L3=5", s.getLevelString());
	}

	@Test
	void commonHotHitMax_isNgramMaxSquaredTimesBitsetWindow() {
		assertEquals(Lucene80FPSearchConfig.NGRAM_MAX * Lucene80FPSearchConfig.NGRAM_MAX
				* Lucene80FPSearchConfig.BITSET_WINDOW_SIZE, Lucene80FPSearchConfig.commonHotHitMaxWindows());
		assertEquals(1152, Lucene80FPSearchConfig.commonHotHitMaxWindows());
	}

	@Test
	void hotHitCountToTier_groupsByFifty() {
		assertEquals(FpStatNgram.HOT_HIT_TIER_ZERO, FpStatNgram.hotHitCountToTier(0));
		assertEquals(1, FpStatNgram.hotHitCountToTier(1));
		assertEquals(1, FpStatNgram.hotHitCountToTier(50));
		assertEquals(2, FpStatNgram.hotHitCountToTier(51));
		assertEquals(3, FpStatNgram.hotHitCountToTier(101));
		assertEquals(24, FpStatNgram.hotHitCountToTier(1152));
		assertEquals(24, FpStatNgram.hotHitCountToTier(2000));
	}

	@Test
	void hotHitTierLabel_reflectsFiftyWideBands() {
		assertEquals("hit0", FpStatNgram.hotHitTierLabel(0));
		assertEquals("hit1_50", FpStatNgram.hotHitTierLabel(1));
		assertEquals("hit51_100", FpStatNgram.hotHitTierLabel(2));
		assertEquals("hit101_150", FpStatNgram.hotHitTierLabel(3));
		assertEquals("hit1151_1152", FpStatNgram.hotHitTierLabel(24));
	}

	@Test
	void getCommonHotHitTierString_formatsBuckets() {
		FpStatNgram s = new FpStatNgram();
		s.commonHotHitTierCnt[2] = 10;
		s.commonHotHitTierCnt[0] = 3;
		assertEquals("hit0=3,hit51_100=10", s.getCommonHotHitTierString());
	}

	@Test
	void toString_includesCommonHotHitTierAndMax() {
		FpStatNgram s = new FpStatNgram();
		s.commonHotHitTierCnt[3] = 1;
		String t = s.toString();
		assertTrue(t.contains("commonHotHitMax=1152"));
		assertTrue(t.contains("commonHotHitTier=hit101_150=1"));
	}

	@Test
	void toString_includesPhaseTimings() {
		FpStatNgram s = new FpStatNgram();
		s.ms_count = 1;
		s.ms_build = 2;
		s.ms_budget = 3;
		s.ms_merge = 4;
		String t = s.toString();
		assertTrue(t.contains("phasesMs=1+2+3+4"));
	}

	@Test
	void getLevelString_emptyWhenAllZero() {
		assertEquals("", new FpStatNgram().getLevelString());
	}

	@Test
	void getLevelString_multipleCellsCommaSeparated() {
		FpStatNgram s = new FpStatNgram();
		s.term_level_cnt[1][1] = 2;
		s.term_level_cnt[3][2] = 4;
		assertEquals("len1@L1=2,len3@L2=4", s.getLevelString());
	}
}
