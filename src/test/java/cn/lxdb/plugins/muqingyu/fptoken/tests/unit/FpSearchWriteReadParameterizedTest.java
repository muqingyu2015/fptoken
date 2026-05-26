package cn.lxdb.plugins.muqingyu.fptoken.tests.unit;

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import cn.lxdb.plugins.muqingyu.fptoken.api.FpSearch;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.block.FpGroupHotNgramBitIndex;
import cn.lxdb.plugins.muqingyu.fptoken.tests.support.FpParameterizedTestSources;
import cn.lxdb.plugins.muqingyu.fptoken.tests.support.FpRebuildSeedIndex;
import cn.lxdb.plugins.muqingyu.fptoken.tests.support.FpWriteReadTestIndex;

/**
 * rebuild 写路径 + {@link FpSearch#search} 读路径对齐（150 组）。
 * <p>
 * 需完整编译 {@code src/cn} 与 {@code lib/} 补丁 Lucene；仅 subset 编译时此类会失败。
 */
class FpSearchWriteReadParameterizedTest {

	@ParameterizedTest(name = "seed={0}")
	@MethodSource("cn.lxdb.plugins.muqingyu.fptoken.tests.support.FpParameterizedTestSources#rebuildSearchAlignmentSeeds")
	void search_hitsDocAfterRebuildWrite(int seed) throws Exception {
		final int maxDoc = 64;
		final FpRebuildSeedIndex index = FpParameterizedTestSources.buildIndexForSeed(seed);

		final BytesRef slice = FpParameterizedTestSources.primarySearchSlice(index.payload);
		final int li = slice.length - 1;
		final int bucket = FpGroupHotNgramBitIndex.bucketIndex(slice.bytes, slice.offset, slice.length);
		final boolean marked = index.built.bitIndex.banksHot[li][bucket].nextSetBit(0) >= 0
				|| index.built.bitIndex.banksCommon[li][bucket].nextSetBit(0) >= 0;
		assertTrue(marked, "seed=" + seed + " write path should mark slice in hot or common bank");

		final FixedBitSet hits = new FpSearch().search(
				FpWriteReadTestIndex.blockList(index.built.groupId, index.built.blockInfo), index.built.terms, maxDoc,
				new BytesRef[] { slice });

		boolean anyExpected = false;
		for (int doc : index.expectedDocs) {
			if (hits.get(doc)) {
				anyExpected = true;
				break;
			}
		}
		assertTrue(anyExpected, "seed=" + seed + " search should hit at least one doc from rebuild postings");
	}
}
