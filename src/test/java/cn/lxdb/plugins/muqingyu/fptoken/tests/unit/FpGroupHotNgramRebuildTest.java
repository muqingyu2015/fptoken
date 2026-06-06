package cn.lxdb.plugins.muqingyu.fptoken.tests.unit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.lucene.util.BytesRef;
import org.junit.jupiter.api.Test;

import cn.lxdb.plugins.muqingyu.fptoken.config.Lucene80FPSearchConfig;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.block.FpGroupDataRebuild;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.block.FpGroupHotNgramRebuild;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FPDocList;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpStatNgram;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpTermKey;
import cn.lxdb.plugins.muqingyu.fptoken.tests.support.FpTestColumnNames;
import cn.lxdb.plugins.muqingyu.fptoken.tests.support.FpTestFixtures;
import cn.lxdb.plugins.muqingyu.fptoken.tests.support.FpTestIndexBuilder;

/** {@link FpGroupHotNgramRebuild#execute} 热词挖掘、downTier 预算与 doc merge。 */
class FpGroupHotNgramRebuildTest {

	@Test
	void execute_promotesRepeatedNgramToHot() throws Exception {
		final FpGroupDataRebuild group = new FpGroupDataRebuild(256);
		final int threshold = Lucene80FPSearchConfig.HOT_TIER_TERM_COUNT_THRESHOLD;
		FpTestFixtures.putCommonTermsSharingNgram(group, "ab", threshold);

		final FpStatNgram stat = FpGroupHotNgramRebuild.execute(group, null);

		final FpTermKey hotAb = FpTestFixtures.termKey("ab");
		assertTrue(group.hotTermToDocs.containsKey(hotAb), "shared ngram ab should become hot");
		assertTrue(group.hotTermDownTierBudget.containsKey(hotAb));
		assertTrue(stat.hot_final > 0);
	}

	@Test
	void rebuildPipeline_hotTermCarriesMergedDocs() throws Exception {
		final FpGroupDataRebuild group = new FpGroupDataRebuild(256);
		FpTestFixtures.putCommonTermsSharingNgram(group, "ab", 16);

		final FpTestIndexBuilder.BuiltIndex idx = FpTestIndexBuilder.buildFromRebuildGroup(group,
				FpTestColumnNames.DEFAULT, 1);

		final BytesRef hotAb = new BytesRef(new byte[] { 'a', 'b' });
		assertTrue(idx.memoryBitIndex.lookupHotOrders(hotAb).length > 0);
		assertTrue(idx.memoryBitIndex.getHotCount() > 0);
	}

	@Test
	void execute_skipsRareMultiByteNgram() throws Exception {
		final FpGroupDataRebuild group = new FpGroupDataRebuild(64);
		FpTestFixtures.putCommonTerm(group, "unique1", 1);
		FpTestFixtures.putCommonTerm(group, "unique2", 2);

		FpGroupHotNgramRebuild.execute(group, null);

		assertFalse(group.hotTermToDocs.containsKey(FpTestFixtures.termKey("uni")),
				"3-byte ngram appearing once should not become hot");
	}

	@Test
	void execute_assignsDownTierBudgetAtLeastOne() throws Exception {
		final FpGroupDataRebuild group = new FpGroupDataRebuild(128);
		FpTestFixtures.putCommonTermsSharingNgram(group, "q", 20);

		FpGroupHotNgramRebuild.execute(group, null);

		final Integer budget = group.hotTermDownTierBudget.get(FpTestFixtures.termKey("q"));
		assertTrue(budget != null && budget.intValue() >= 1);
	}
}
