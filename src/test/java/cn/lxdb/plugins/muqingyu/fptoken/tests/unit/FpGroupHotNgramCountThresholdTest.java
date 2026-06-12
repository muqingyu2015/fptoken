package cn.lxdb.plugins.muqingyu.fptoken.tests.unit;

import static cn.lxdb.plugins.muqingyu.fptoken.tests.support.FpBitIndexTestSupport.addCommonTermsSharingNgram;
import static cn.lxdb.plugins.muqingyu.fptoken.tests.support.FpBitIndexTestSupport.defaultLowThreshold;
import static cn.lxdb.plugins.muqingyu.fptoken.tests.support.FpBitIndexTestSupport.hotPayloads;
import static cn.lxdb.plugins.muqingyu.fptoken.tests.support.FpBitIndexTestSupport.invokeBuildHotTerms;
import static cn.lxdb.plugins.muqingyu.fptoken.tests.support.FpBitIndexTestSupport.invokeCountNgram;
import static cn.lxdb.plugins.muqingyu.fptoken.tests.support.FpBitIndexTestSupport.resetPools;
import static cn.lxdb.plugins.muqingyu.fptoken.tests.support.FpBitIndexTestSupport.termKey;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FPDocList;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpStatNgram;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpTermKey;
import cn.lxdb.plugins.muqingyu.fptoken.ngram.Counter;

/** countNgramOccurrencesInCommon 阈值与热词挖掘回归。 */
@Tag("lxdb-runtime")
class FpGroupHotNgramCountThresholdTest {

	private static final int MAX_DOC = 64;

	@BeforeEach
	void setUp() {
		resetPools();
	}

	@AfterEach
	void tearDown() {
		resetPools();
	}

	@Test
	void countNgram_stopsIncrementingOnceThresholdReached() throws Exception {
		final int threshold = defaultLowThreshold();
		final HashMap<FpTermKey, FPDocList> common = new HashMap<>();
		addCommonTermsSharingNgram(common, MAX_DOC, threshold + 6, "x", "y");

		final FpStatNgram stat = new FpStatNgram();
		final HashMap<FpTermKey, Counter> counts = new HashMap<>();
		invokeCountNgram(threshold, stat, common, counts);

		final Counter ab = counts.get(termKey("ab"));
		assertTrue(ab != null, "应统计到 ngram ab");
		assertTrue(ab.cnt >= threshold,
				"cnt 应至少达到热阈值");
		assertTrue(ab.cnt <= threshold + 1,
				"达阈值后最多多计 1 次，不应继续累加到全部 document（threshold+6）");
	}

	@Test
	void buildHot_promotesSharedNgramAtThreshold() throws Exception {
		final int threshold = defaultLowThreshold();
		final HashMap<FpTermKey, FPDocList> common = new HashMap<>();
		addCommonTermsSharingNgram(common, MAX_DOC, threshold, "p", "q");

		final HashMap<FpTermKey, Counter> counts = new HashMap<>();
		invokeCountNgram(threshold, stat(), common, counts);
		final HashMap<FpTermKey, FPDocList> hot = invokeBuildHotTerms(threshold, MAX_DOC, counts);

		assertTrue(hotPayloads(hot).contains("ab"), "出现次数达阈值的 ngram 应进入热词表");
	}

	@Test
	void buildHot_doesNotPromoteBelowThreshold() throws Exception {
		final int threshold = defaultLowThreshold();
		final HashMap<FpTermKey, FPDocList> common = new HashMap<>();
		addCommonTermsSharingNgram(common, MAX_DOC, threshold - 1, "m", "n");

		final HashMap<FpTermKey, Counter> counts = new HashMap<>();
		invokeCountNgram(threshold, stat(), common, counts);
		final HashMap<FpTermKey, FPDocList> hot = invokeBuildHotTerms(threshold, MAX_DOC, counts);

		assertFalse(hotPayloads(hot).contains("ab"), "未达阈值的 ngram 不应进入热词表");
	}

	@Test
	void expandHotPrefix_addsShorterHotPrefixes() throws Exception {
		final int threshold = defaultLowThreshold();
		final HashMap<FpTermKey, FPDocList> common = new HashMap<>();
		addCommonTermsSharingNgram(common, MAX_DOC, threshold, "", "cdef");

		final HashMap<FpTermKey, Counter> counts = new HashMap<>();
		invokeCountNgram(threshold, stat(), common, counts);

		assertTrue(counts.containsKey(termKey("a")), "热长串应补全前缀 a");
		assertTrue(counts.containsKey(termKey("ab")), "热长串应补全前缀 ab");
	}

	private static FpStatNgram stat() {
		return new FpStatNgram();
	}
}
