package cn.lxdb.plugins.muqingyu.fptoken.tests.unit;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpStat;

class FpStatTest {

	@Test
	void toString_includesCounters() {
		FpStat s = new FpStat();
		s.flush_common_cnt = 1;
		s.flush_high_cnt = 2;
		s.flush_high_cnt_original = 3;
		s.flush_high_cnt_rebuild = 4;
		s.doclist_hot = 5;
		s.doclist_common = 6;
		String t = s.toString();
		assertTrue(t.contains("commonFlush=1"));
		assertTrue(t.contains("highFlush=2"));
		assertTrue(t.contains("original=3"));
		assertTrue(t.contains("rebuild=4"));
		assertTrue(t.contains("doclistHot=5"));
		assertTrue(t.contains("doclistCommon=6"));
		assertFalse(t.contains("null"));
	}
}
