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

/**
 * 复现用户场景：正文含 {@code t_acctbal decimal(15,2) not null,}，查询 {@code cctbal}。
 */
class FpAcctbalQueryDiagnosticTest {

	private static final BytesRef COLUMN_D = new BytesRef("d");

	@Test
	void query_cctbal_findsDocWithAcctbalWindow() throws Exception {
		final String docLine = "  t_acctbal decimal(15,2) not null, other cols ";
		final byte[] docBytes = docLine.getBytes(StandardCharsets.UTF_8);
		final List<WindowTerm> windows = BinarySlidingWindowApi.bitsetWindowsforToken(docBytes, 0, docBytes.length);

		final FpGroupDataRebuild group = new FpGroupDataRebuild(64);
		int docId = 0;
		for (WindowTerm w : windows) {
			FpTestFixtures.putCommonTerm(group, w.getWindowBytes(), docId);
		}

		final FpTestIndexBuilder.BuiltIndex idx = FpTestIndexBuilder.buildFromRebuildGroup(group, COLUMN_D, 1);

		final BytesRef[] querySlices = querySlicesFor("cctbal");
		final FpSearchStat stat = new FpSearchStat();
		final FpSearch search = new FpSearch(stat);
		final FixedBitSet hits = search.search(idx.fpblockList, idx.terms, idx.maxDoc, COLUMN_D, querySlices);

		System.out.println("windows=" + windows.size() + " stat=" + stat);
		for (int i = 0; i < windows.size(); i++) {
			System.out.println("  win" + i + "=" + new String(windows.get(i).getWindowBytes(), StandardCharsets.UTF_8));
		}
		System.out.println("querySlices=" + querySlices.length);
		for (BytesRef s : querySlices) {
			System.out.println("  slice=" + new String(s.bytes, s.offset, s.length, StandardCharsets.UTF_8));
		}
		System.out.println("hotOrders=" + idx.memoryBitIndex.lookupHotOrders(querySlices[0]).length
				+ " commonOrders=" + idx.memoryBitIndex.lookupCommonOrders(querySlices[0]).length);

		assertTrue(FpTestDocSets.equals(hits, idx.maxDoc, docId),
				"cctbal should hit doc containing t_acctbal; stat=" + stat);
	}

	@Test
	void query_cctbal_columnMismatch_txt4_vs_d_returnsEmpty() throws Exception {
		final byte[] payload = "xxx t_acctbal decimal(15,2) not xxx".getBytes(StandardCharsets.UTF_8);
		final FpGroupDataRebuild group = new FpGroupDataRebuild(64);
		FpTestFixtures.putCommonTerm(group, payload, 7);

		final FpTestIndexBuilder.BuiltIndex idx = FpTestIndexBuilder.buildFromRebuildGroup(group, COLUMN_D, 1);
		final BytesRef[] querySlices = querySlicesFor("cctbal");

		final FpSearch search = new FpSearch(new FpSearchStat());
		final FixedBitSet wrongColumn = search.search(idx.fpblockList, idx.terms, idx.maxDoc, new BytesRef("txt4"),
				querySlices);
		assertTrue(FpTestDocSets.equals(wrongColumn, idx.maxDoc, new int[0]),
				"indexed column=d but query column=txt4 should miss");
	}

	@Test
	void query_cctbal_whenHotTerm_stillFindsDocs() throws Exception {
		final FpGroupDataRebuild group = new FpGroupDataRebuild(256);
		for (int i = 0; i < 20; i++) {
			final byte[] window = ("prefix cctbal suffix padding " + i + " !!").getBytes(StandardCharsets.UTF_8);
			FpTestFixtures.putCommonTerm(group, window, i);
		}

		final FpTestIndexBuilder.BuiltIndex idx = FpTestIndexBuilder.buildFromRebuildGroup(group, COLUMN_D, 1);
		final BytesRef[] querySlices = querySlicesFor("cctbal");

		final FpSearchStat stat = new FpSearchStat();
		final FpSearch search = new FpSearch(stat);
		final FixedBitSet hits = search.search(idx.fpblockList, idx.terms, idx.maxDoc, COLUMN_D, querySlices);

		System.out.println("hot path stat=" + stat);
		System.out.println("hotOrders=" + idx.memoryBitIndex.lookupHotOrders(querySlices[0]).length
				+ " commonOrders=" + idx.memoryBitIndex.lookupCommonOrders(querySlices[0]).length);

		assertTrue(hits.cardinality() > 0, "hot cctbal should still return docs; stat=" + stat);
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
