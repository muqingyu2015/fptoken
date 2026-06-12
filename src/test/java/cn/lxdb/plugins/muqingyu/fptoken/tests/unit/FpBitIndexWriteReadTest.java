package cn.lxdb.plugins.muqingyu.fptoken.tests.unit;

import static cn.lxdb.plugins.muqingyu.fptoken.tests.support.FpBitIndexTestSupport.buildBitIndexFromCommonPayloads;
import static cn.lxdb.plugins.muqingyu.fptoken.tests.support.FpBitIndexTestSupport.flushBitIndexToRam;
import static cn.lxdb.plugins.muqingyu.fptoken.tests.support.FpBitIndexTestSupport.openRamBits;
import static cn.lxdb.plugins.muqingyu.fptoken.tests.support.FpBitIndexTestSupport.poolLeased;
import static cn.lxdb.plugins.muqingyu.fptoken.tests.support.FpBitIndexTestSupport.resetPools;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BytesRef;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import cn.lxdb.plugins.muqingyu.fptoken.dataset.block.FpGroupHotNgramBitIndex;
import cn.lxdb.plugins.muqingyu.fptoken.pool.FpHashMapPoolIds;
import cn.lxdb.plugins.muqingyu.fptoken.tests.support.FpBitIndexTestSupport.RamFlushedBitIndex;

/** bitindex 写读、selective 与 releasePooledMaps 一致性。 */
@Tag("lxdb-runtime")
class FpBitIndexWriteReadTest {

	@BeforeEach
	void setUp() {
		resetPools();
	}

	@AfterEach
	void tearDown() {
		resetPools();
	}

	@Test
	void flush_fullRead_roundTrip_preservesLookup() throws Exception {
		final FpGroupHotNgramBitIndex built = buildBitIndexFromCommonPayloads(
				List.of("abcdef", "abzzzz", "zzabc"));
		final RamFlushedBitIndex ram = flushBitIndexToRam(built, 0);
		built.releasePooledMaps();

		try (IndexInput in = openRamBits(ram.dir)) {
			final FpGroupHotNgramBitIndex loaded = FpGroupHotNgramBitIndex.readfrom(in, ram.blockInfo);
			final BytesRef slice = new BytesRef("abc".getBytes(StandardCharsets.UTF_8));
			assertArrayEquals(built.lookupCommonOrders(slice), loaded.lookupCommonOrders(slice));
		}
	}

	@Test
	void selectiveRead_matchesFullRead_forQuerySlices() throws Exception {
		final FpGroupHotNgramBitIndex built = buildBitIndexFromCommonPayloads(
				List.of("abcdef", "abzzzz", "zzabc"));
		final RamFlushedBitIndex ram = flushBitIndexToRam(built, 0);

		try (IndexInput in = openRamBits(ram.dir)) {
			final FpGroupHotNgramBitIndex full = FpGroupHotNgramBitIndex.readfrom(in, ram.blockInfo);

			final BytesRef[] slices = {
					new BytesRef("ab".getBytes(StandardCharsets.UTF_8)),
					new BytesRef("abc".getBytes(StandardCharsets.UTF_8)),
					new BytesRef("zz".getBytes(StandardCharsets.UTF_8)) };
			final long[] keys = FpGroupHotNgramBitIndex.selectiveKeysForSlices(slices);

			in.seek(0);
			final FpGroupHotNgramBitIndex selective = FpGroupHotNgramBitIndex.readfromBanksSelective(in, ram.blockInfo,
					keys, keys);

			for (BytesRef slice : slices) {
				assertArrayEquals(full.lookupCommonOrders(slice), selective.lookupCommonOrders(slice),
						"common " + slice);
			}

			selective.releasePooledMaps();
			assertEquals(0, poolLeased(FpHashMapPoolIds.lenRowSparseOrders));
			assertEquals(0, selective.lookupCommonOrders(slices[1]).length,
					"释放后 selective 实例不应再命中（防误用）");
		}
	}

	@Test
	void selectiveRead_manyBuckets_exceedingSkipInterval_stillMatchesFull() throws Exception {
		final List<String> payloads = new ArrayList<>();
		for (int i = 0; i < 150; i++) {
			payloads.add("z" + (char) ('A' + (i % 26)) + (char) ('a' + (i % 26)) + i);
		}
		final FpGroupHotNgramBitIndex built = buildBitIndexFromCommonPayloads(payloads);
		final RamFlushedBitIndex ram = flushBitIndexToRam(built, 0);

		final BytesRef probe = new BytesRef("zAa0".getBytes(StandardCharsets.UTF_8));
		final long[] keys = FpGroupHotNgramBitIndex.selectiveKeysForSlices(new BytesRef[] { probe });

		try (IndexInput in = openRamBits(ram.dir)) {
			final FpGroupHotNgramBitIndex full = FpGroupHotNgramBitIndex.readfrom(in, ram.blockInfo);
			in.seek(0);
			final FpGroupHotNgramBitIndex selective = FpGroupHotNgramBitIndex.readfromBanksSelective(in, ram.blockInfo,
					keys, keys);
			assertArrayEquals(full.lookupCommonOrders(probe), selective.lookupCommonOrders(probe));
			selective.releasePooledMaps();
		}
	}
}
