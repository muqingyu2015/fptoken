package cn.lxdb.plugins.muqingyu.fptoken.tests.performance;

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import cn.lxdb.plugins.muqingyu.fptoken.api.FpSearch;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.block.FpGroupDataRebuild;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpSearchStat;
import cn.lxdb.plugins.muqingyu.fptoken.tests.support.FpTestColumnNames;
import cn.lxdb.plugins.muqingyu.fptoken.tests.support.FpTestDocSets;
import cn.lxdb.plugins.muqingyu.fptoken.tests.support.FpTestFixtures;
import cn.lxdb.plugins.muqingyu.fptoken.tests.support.FpTestIndexBuilder;

@Tag("performance")
@EnabledIfSystemProperty(named = "fptoken.runPerfTests", matches = "true")
class FpSearchPerfTest {

	@Test
	void search_manySlices_completesWithinBudget() throws Exception {
		final FpGroupDataRebuild group = new FpGroupDataRebuild(2048);
		FpTestFixtures.putCommonTermsSharingNgram(group, "ab", 100);
		for (int i = 0; i < 50; i++) {
			FpTestFixtures.putCommonTerm(group, "term" + i, i + 200);
		}

		final FpTestIndexBuilder.BuiltIndex idx = FpTestIndexBuilder.buildFromRebuildGroup(group,
				FpTestColumnNames.DEFAULT, 9);
		final FpSearch search = new FpSearch(new FpSearchStat());

		final BytesRef[] slices = new BytesRef[4];
		slices[0] = new BytesRef(new byte[] { 'a', 'b' });
		slices[1] = new BytesRef(new byte[] { 't' });
		slices[2] = new BytesRef(new byte[] { 'e' });
		slices[3] = new BytesRef(new byte[] { 'm' });

		final int rounds = 100;
		final long t0 = System.nanoTime();
		FixedBitSet last = null;
		for (int r = 0; r < rounds; r++) {
			last = search.search(idx.fpblockList, idx.terms, idx.maxDoc, idx.columnName, slices);
		}
		final double msPerSearch = (System.nanoTime() - t0) / rounds / 1_000_000.0;
		System.out.println("[perf] fpSearch 4-slice ms=" + msPerSearch);

		assertTrue(last != null);
		assertTrue(msPerSearch < 500.0, "4-slice search should stay under 500ms per call in perf smoke");
		assertTrue(FpTestDocSets.toSortedArray(last, idx.maxDoc).length >= 0);
	}
}
