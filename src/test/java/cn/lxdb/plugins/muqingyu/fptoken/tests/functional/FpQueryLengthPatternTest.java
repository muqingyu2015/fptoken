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
import cn.lxdb.plugins.muqingyu.fptoken.tests.support.FpTestIndexBuilder;
import cn.lxdb.plugins.muqingyu.fptoken.token.BinarySlidingWindowApi;
import cn.lxdb.plugins.muqingyu.fptoken.token.FpToken;
import cn.lxdb.plugins.muqingyu.fptoken.token.FpTokenBytesMode;
import cn.lxdb.plugins.muqingyu.fptoken.token.WindowTerm;

/** 用户反馈：as/left 能搜到，cas/lef 搜不到。 */
class FpQueryLengthPatternTest {

	private static final BytesRef COLUMN_D = new BytesRef("d");

	@Test
	void query_lef_whenOnlyHotAnchorIsLeft_shouldStillHit() throws Exception {
		final FpGroupDataRebuild group = new FpGroupDataRebuild(256);
		for (int i = 0; i < 20; i++) {
			final byte[] window = ("data left padding variant " + i + " !!").getBytes(StandardCharsets.UTF_8);
			FpTestFixtures.putCommonTerm(group, window, i);
		}
		final FpTestIndexBuilder.BuiltIndex idx = FpTestIndexBuilder.buildFromRebuildGroup(group, COLUMN_D, 1);
		final BytesRef[] leftSlice = querySlicesFor("left");
		final BytesRef[] lefSlice = querySlicesFor("lef");

		final FpSearchStat statLeft = new FpSearchStat();
		final FpSearchStat statLef = new FpSearchStat();
		final FixedBitSet leftHits = new FpSearch(statLeft).search(idx.fpblockList, idx.terms, idx.maxDoc, COLUMN_D,
				leftSlice);
		final FixedBitSet lefHits = new FpSearch(statLef).search(idx.fpblockList, idx.terms, idx.maxDoc, COLUMN_D,
				lefSlice);

		System.out.println("left hotOrd=" + idx.memoryBitIndex.lookupHotOrders(leftSlice[0]).length + " hit="
				+ leftHits.cardinality() + " stat=" + statLeft);
		System.out.println("lef  hotOrd=" + idx.memoryBitIndex.lookupHotOrders(lefSlice[0]).length + " hit="
				+ lefHits.cardinality() + " stat=" + statLef);

		assertTrue(leftHits.cardinality() > 0, "left should hit");
		assertTrue(lefHits.cardinality() > 0, "lef should hit when left is indexed; stat=" + statLef);
	}

	@Test
	void query_cas_whenOnlyHotAnchorIsCase_shouldStillHit() throws Exception {
		final FpGroupDataRebuild group = new FpGroupDataRebuild(256);
		for (int i = 0; i < 20; i++) {
			final byte[] window = ("data case padding variant " + i + " !!").getBytes(StandardCharsets.UTF_8);
			FpTestFixtures.putCommonTerm(group, window, i);
		}
		final FpTestIndexBuilder.BuiltIndex idx = FpTestIndexBuilder.buildFromRebuildGroup(group, COLUMN_D, 1);
		final BytesRef[] caseSlice = querySlicesFor("case");
		final BytesRef[] casSlice = querySlicesFor("cas");

		final FpSearchStat statCase = new FpSearchStat();
		final FpSearchStat statCas = new FpSearchStat();
		final FixedBitSet caseHits = new FpSearch(statCase).search(idx.fpblockList, idx.terms, idx.maxDoc, COLUMN_D,
				caseSlice);
		final FixedBitSet casHits = new FpSearch(statCas).search(idx.fpblockList, idx.terms, idx.maxDoc, COLUMN_D,
				casSlice);

		System.out.println("case hotOrd=" + idx.memoryBitIndex.lookupHotOrders(caseSlice[0]).length + " hit="
				+ caseHits.cardinality() + " stat=" + statCase);
		System.out.println("cas  hotOrd=" + idx.memoryBitIndex.lookupHotOrders(casSlice[0]).length + " hit="
				+ casHits.cardinality() + " stat=" + statCas);

		assertTrue(caseHits.cardinality() > 0, "case should hit");
		assertTrue(casHits.cardinality() > 0, "cas should hit when case is indexed; stat=" + statCas);
	}

	@ParameterizedTest
	@CsvSource({ "as", "cas", "left", "lef" })
	void query_substrings_inSingleDoc(String query) throws Exception {
		final FpTestIndexBuilder.BuiltIndex idx = buildIndex("prefix case left lefter suffix padding here!!");
		final BytesRef[] slices = querySlicesFor(query);
		final FpSearchStat stat = new FpSearchStat();
		final FixedBitSet hits = new FpSearch(stat).search(idx.fpblockList, idx.terms, idx.maxDoc, COLUMN_D, slices);
		assertTrue(FpTestDocSets.equals(hits, idx.maxDoc, 42), "query " + query + " stat=" + stat);
	}

	private static FpTestIndexBuilder.BuiltIndex buildIndex(String docLine) throws Exception {
		final byte[] docBytes = docLine.getBytes(StandardCharsets.UTF_8);
		final List<WindowTerm> windows = BinarySlidingWindowApi.bitsetWindowsforToken(docBytes, 0, docBytes.length);
		final FpGroupDataRebuild group = new FpGroupDataRebuild(128);
		for (WindowTerm w : windows) {
			FpTestFixtures.putCommonTerm(group, w.getWindowBytes(), 42);
		}
		return FpTestIndexBuilder.buildFromRebuildGroup(group, COLUMN_D, 1);
	}

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
