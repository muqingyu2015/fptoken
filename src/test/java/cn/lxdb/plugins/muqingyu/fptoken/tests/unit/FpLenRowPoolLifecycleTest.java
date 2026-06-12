package cn.lxdb.plugins.muqingyu.fptoken.tests.unit;

import static cn.lxdb.plugins.muqingyu.fptoken.tests.support.FpBitIndexTestSupport.buildBitIndexFromCommonPayloads;
import static cn.lxdb.plugins.muqingyu.fptoken.tests.support.FpBitIndexTestSupport.poolLeased;
import static cn.lxdb.plugins.muqingyu.fptoken.tests.support.FpBitIndexTestSupport.resetPools;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import cn.lxdb.plugins.muqingyu.fptoken.dataset.block.FpGroupHotNgramBitIndex;
import cn.lxdb.plugins.muqingyu.fptoken.pool.FpHashMapPoolHub;
import cn.lxdb.plugins.muqingyu.fptoken.pool.FpHashMapPoolIds;

/** LenRow buildMap / sparseOrders 对象池生命周期。 */
@Tag("lxdb-runtime")
class FpLenRowPoolLifecycleTest {

	@BeforeEach
	void setUp() {
		resetPools();
	}

	@AfterEach
	void tearDown() {
		resetPools();
	}

	@Test
	void execute1_finalizeRows_releasesLenRowBuildMapLeases() throws Exception {
		buildBitIndexFromCommonPayloads(Arrays.asList("abcdef", "ghijkl"));
		assertEquals(0, poolLeased(FpHashMapPoolIds.lenRowBuildMap),
				"buildMap 应在 finalizeRow 后全部归还");
	}

	@Test
	void releasePooledMaps_isIdempotentOnFullBuildIndex() throws Exception {
		final FpGroupHotNgramBitIndex bitIndex = buildBitIndexFromCommonPayloads(Arrays.asList("abc"));
		bitIndex.releasePooledMaps();
		bitIndex.releasePooledMaps();
		assertEquals(0, poolLeased(FpHashMapPoolIds.lenRowSparseOrders));
		assertEquals(0, poolLeased(FpHashMapPoolIds.lenRowBuildMap));
	}

	@Test
	void lenRowBuildMap_reusesMapsAcrossSequentialBuilds() throws Exception {
		for (int round = 0; round < 3; round++) {
			buildBitIndexFromCommonPayloads(Arrays.asList("term" + round));
			assertEquals(0, poolLeased(FpHashMapPoolIds.lenRowBuildMap), "round=" + round);
		}
		assertTrue(FpHashMapPoolHub.stats(FpHashMapPoolIds.lenRowBuildMap).createdCount() <= 12,
				"多轮构建应复用池内 map，不应每轮新建 12 张");
	}
}
