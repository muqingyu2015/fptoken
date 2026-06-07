package cn.lxdb.plugins.muqingyu.fptoken.tests.functional;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import cn.lxdb.plugins.muqingyu.fptoken.api.FpSearch;
import cn.lxdb.plugins.muqingyu.fptoken.config.Lucene80FPSearchConfig;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.block.FpGroupDataRebuild;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpSearchStat;
import cn.lxdb.plugins.muqingyu.fptoken.tests.support.FpTestDocSets;
import cn.lxdb.plugins.muqingyu.fptoken.tests.support.FpTestFixtures;
import cn.lxdb.plugins.muqingyu.fptoken.token.BinarySlidingWindowApi;
import cn.lxdb.plugins.muqingyu.fptoken.token.FpToken;
import cn.lxdb.plugins.muqingyu.fptoken.token.FpTokenBytesMode;
import cn.lxdb.plugins.muqingyu.fptoken.token.WindowTerm;

/** 用户反馈：as/left 能搜到，cas/lef 搜不到。 */
class FpQueryLengthPatternTest {

	private static final BytesRef COLUMN_D = new BytesRef("d");

	


	private static BytesRef[] querySlicesFor(String queryText) {
		final byte[] sourceBytes = FpToken.textToSourceBytes(queryText, FpTokenBytesMode.UTF8);
		final List<WindowTerm> windows = BinarySlidingWindowApi.slidingWindows(sourceBytes, 0, sourceBytes.length,
				Lucene80FPSearchConfig.NGRAM_MAX, Lucene80FPSearchConfig.NGRAM_MAX - 1);
		final Map<FpToken.DedupKey, FpToken.PendingTerm> dedup = new LinkedHashMap<>();
		for (WindowTerm window : windows) {
			final byte[] padded = window.getWindowBytes();
			final FpToken.DedupKey probe = new FpToken.DedupKey(padded, padded.length);
			if (!dedup.containsKey(probe)) {
				dedup.put(probe, new FpToken.PendingTerm(padded, padded.length));
			}
		}
		final ArrayList<BytesRef> list = new ArrayList<>();
		for (FpToken.PendingTerm pt : dedup.values()) {
			list.add(new BytesRef(pt.buffer, 0, pt.length));
		}
		return list.toArray(new BytesRef[0]);
	}
}
