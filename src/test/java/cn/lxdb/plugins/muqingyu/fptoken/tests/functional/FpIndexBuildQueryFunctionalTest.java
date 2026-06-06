package cn.lxdb.plugins.muqingyu.fptoken.tests.functional;

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.junit.jupiter.api.Test;

import cn.lxdb.plugins.muqingyu.fptoken.api.FpSearch;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.block.FpGroupDataRebuild;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpSearchStat;
import cn.lxdb.plugins.muqingyu.fptoken.tests.support.FpTestColumnNames;
import cn.lxdb.plugins.muqingyu.fptoken.tests.support.FpTestDocSets;
import cn.lxdb.plugins.muqingyu.fptoken.tests.support.FpTestFixtures;
import cn.lxdb.plugins.muqingyu.fptoken.tests.support.FpTestIndexBuilder;

/**
 * 索引产生 → {@link FpSearch#search} 查询：hot/common 倒排 + 多 slice AND。
 */
class FpIndexBuildQueryFunctionalTest {

	@Test
	void search_hotSlice_findsMergedDocs() throws Exception {
		final FpTestIndexBuilder.BuiltIndex idx = buildStandardIndex();
		final FpSearch search = new FpSearch(new FpSearchStat());

		final BytesRef[] slices = { new BytesRef(new byte[] { 'a', 'b' }) };
		final FixedBitSet hits = search.search(idx.fpblockList, idx.terms, idx.maxDoc, idx.columnName, slices);

		assertTrue(FpTestDocSets.equals(hits, idx.maxDoc, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16),
				"hot ngram ab should OR docs from 16 common terms");
	}

	@Test
	void search_commonSlice_findsUniquePayloadDoc() throws Exception {
		final FpTestIndexBuilder.BuiltIndex idx = buildStandardIndex();
		final FpSearch search = new FpSearch(new FpSearchStat());

		final BytesRef[] slices = { new BytesRef("zzonly".getBytes()) };
		final FixedBitSet hits = search.search(idx.fpblockList, idx.terms, idx.maxDoc, idx.columnName, slices);

		assertTrue(FpTestDocSets.equals(hits, idx.maxDoc, 99));
	}

	@Test
	void search_twoSlices_and_intersection() throws Exception {
		final FpGroupDataRebuild group = new FpGroupDataRebuild(256);
		FpTestFixtures.putCommonTerm(group, "abXpayload", 10, 11);
		FpTestFixtures.putCommonTerm(group, "abYpayload", 11, 12);
		FpTestFixtures.putCommonTermsSharingNgram(group, "ab", 18);

		final FpTestIndexBuilder.BuiltIndex idx = FpTestIndexBuilder.buildFromRebuildGroup(group,
				FpTestColumnNames.DEFAULT, 5);
		final FpSearch search = new FpSearch(new FpSearchStat());

		final BytesRef[] slices = {
				new BytesRef(new byte[] { 'a', 'b' }),
				new BytesRef(new byte[] { 'X' })
		};
		final FixedBitSet hits = search.search(idx.fpblockList, idx.terms, idx.maxDoc, idx.columnName, slices);

		assertTrue(FpTestDocSets.equals(hits, idx.maxDoc, 10, 11), "doc must match both ab hot path and X subslice in payload");
	}

	@Test
	void search_missingSlice_returnsEmpty() throws Exception {
		final FpTestIndexBuilder.BuiltIndex idx = buildStandardIndex();
		final FpSearch search = new FpSearch(new FpSearchStat());

		final BytesRef[] slices = { new BytesRef("NOTFOUND".getBytes()) };
		final FixedBitSet hits = search.search(idx.fpblockList, idx.terms, idx.maxDoc, idx.columnName, slices);

		assertTrue(FpTestDocSets.equals(hits, idx.maxDoc, new int[0]));
	}

	@Test
	void search_selectiveFpBits_matchesFullIndex() throws Exception {
		final FpTestIndexBuilder.BuiltIndex idx = buildStandardIndex();
		final FpSearch search = new FpSearch(new FpSearchStat());
		final BytesRef[] slices = { new BytesRef(new byte[] { 'a', 'b' }) };
		final long[] keys = cn.lxdb.plugins.muqingyu.fptoken.dataset.block.FpGroupHotNgramBitIndex
				.selectiveKeysForSlices(slices);

		final FixedBitSet full = search.search(idx.fpblockList, idx.terms, idx.maxDoc, idx.columnName, slices);

		final cn.lxdb.plugins.muqingyu.fptoken.tests.support.InMemoryFpTerms selectiveTerms = idx.terms
				.withGroupBitIndex(idx.groupId, idx.memoryBitIndex.viewSelective(keys, keys));
		final FixedBitSet selective = search.search(idx.fpblockList, selectiveTerms, idx.maxDoc, idx.columnName,
				slices);

		assertTrue(java.util.Arrays.equals(FpTestDocSets.toSortedArray(full, idx.maxDoc),
				FpTestDocSets.toSortedArray(selective, idx.maxDoc)));
	}

	private static FpTestIndexBuilder.BuiltIndex buildStandardIndex() throws Exception {
		final FpGroupDataRebuild group = new FpGroupDataRebuild(256);
		FpTestFixtures.putCommonTermsSharingNgram(group, "ab", 16);
		FpTestFixtures.putCommonTerm(group, "zzonly", 99);
		return FpTestIndexBuilder.buildFromRebuildGroup(group, FpTestColumnNames.DEFAULT,
				FpTestIndexBuilder.DEFAULT_GROUP_ID);
	}
}
