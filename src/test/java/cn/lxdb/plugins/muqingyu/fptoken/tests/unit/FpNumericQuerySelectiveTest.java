package cn.lxdb.plugins.muqingyu.fptoken.tests.unit;

import static cn.lxdb.plugins.muqingyu.fptoken.tests.support.FpBitIndexTestSupport.buildBitIndexFromCommonPayloads;
import static cn.lxdb.plugins.muqingyu.fptoken.tests.support.FpBitIndexTestSupport.flushBitIndexToRam;
import static cn.lxdb.plugins.muqingyu.fptoken.tests.support.FpBitIndexTestSupport.openRamBits;
import static cn.lxdb.plugins.muqingyu.fptoken.tests.support.FpBitIndexTestSupport.resetPools;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BytesRef;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import cn.lxdb.plugins.muqingyu.fptoken.config.Lucene80FPSearchConfig;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.block.FpGroupHotNgramBitIndex;
import cn.lxdb.plugins.muqingyu.fptoken.token.BinarySlidingWindowApi;
import cn.lxdb.plugins.muqingyu.fptoken.token.FpToken;
import cn.lxdb.plugins.muqingyu.fptoken.token.WindowTerm;
import cn.lxdb.plugins.muqingyu.fptoken.tests.support.FpBitIndexTestSupport.RamFlushedBitIndex;

/** 复现长数字串（如 4009980000）多 slice selective 读盘。 */
@Tag("lxdb-runtime")
class FpNumericQuerySelectiveTest {

	@BeforeEach
	void setUp() {
		resetPools();
	}

	@AfterEach
	void tearDown() {
		resetPools();
	}

	private static BytesRef[] querySlices(String queryText) {
		final byte[] sourceBytes = queryText.getBytes(StandardCharsets.UTF_8);
		final List<WindowTerm> windows = BinarySlidingWindowApi.slidingWindows(sourceBytes, 0, sourceBytes.length,
				Lucene80FPSearchConfig.NGRAM_MAX, Lucene80FPSearchConfig.NGRAM_MAX - 2);
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

	@Test
	void hotOnlyLongPayload_middleSlice_inHotTierNotCommon() throws Exception {
		final java.util.HashMap<cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpTermKey, cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FPDocList> hotMap = new java.util.HashMap<>();
		final java.util.ArrayList<java.util.Map.Entry<cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpTermKey, cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FPDocList>> hotOrdered = new java.util.ArrayList<>();
		final cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpTermKey key = cn.lxdb.plugins.muqingyu.fptoken.tests.support.FpBitIndexTestSupport
				.termKey("4009980000");
		final cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FPDocList docs = new cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FPDocList(
				512);
		docs.addDoc(0);
		hotMap.put(key, docs);
		hotOrdered.add(java.util.Map.entry(key, docs));
		final FpGroupHotNgramBitIndex built = FpGroupHotNgramBitIndex.execute1(
				cn.lxdb.plugins.muqingyu.fptoken.config.FpTokenBlockLevelPolicy.BLOCK_LEVEL_LOW, null, hotMap,
				hotOrdered, new java.util.ArrayList<>());

		final BytesRef[] slices = querySlices("4009980000");
		for (BytesRef slice : slices) {
			final String label = new String(slice.bytes, slice.offset, slice.length, StandardCharsets.UTF_8);
			assertTrue(built.lookupHotOrders(slice).length > 0, "hot should index slice " + label);
			assertTrue(built.lookupCommonOrders(slice).length == 0,
					"common should skip ngrams covered by hot payload at " + label);
		}
	}

	@Test
	void selectiveSession_query400998_twoSlices_allResolve() throws Exception {
		final FpGroupHotNgramBitIndex built = buildBitIndexFromCommonPayloads(List.of("4009980000"));
		final RamFlushedBitIndex ram = flushBitIndexToRam(built, 0);

		final BytesRef[] slices = querySlices("400998");
		assertTrue(slices.length == 2, "6-byte query must produce 2 anchor slices, got " + slices.length);

		final long[] keys = FpGroupHotNgramBitIndex.selectiveKeysForSlices(slices);
		assertTrue(keys.length == 2, "two distinct bucket keys expected");

		try (IndexInput in = openRamBits(ram.dir)) {
			final FpGroupHotNgramBitIndex full = FpGroupHotNgramBitIndex.readfrom(in, ram.blockInfo);
			try (FpGroupHotNgramBitIndex.SelectiveSession session = FpGroupHotNgramBitIndex.SelectiveSession.open(in,
					ram.blockInfo, keys, keys)) {
				session.runSkip1Phase();
				session.runSkip2Phase();
				final FpGroupHotNgramBitIndex selective = session.finishKeysOrderPhase();
				for (BytesRef slice : slices) {
					final String label = new String(slice.bytes, slice.offset, slice.length, StandardCharsets.UTF_8);
					assertArrayEquals(full.lookupCommonOrders(slice), selective.lookupCommonOrders(slice),
							"slice " + label);
					assertTrue(selective.lookupCommonOrders(slice).length > 0, "slice " + label);
				}
				selective.releasePooledMaps();
			}
		}
	}

	@Test
	void selectiveSession_fullNumericQuery_matchesFullRead() throws Exception {
		final List<String> payloads = new ArrayList<>();
		payloads.add("4009980000");
		for (int i = 0; i < 320; i++) {
			payloads.add(String.format("noise%03d_%d", i, i * 17));
		}
		final FpGroupHotNgramBitIndex built = buildBitIndexFromCommonPayloads(payloads);
		final RamFlushedBitIndex ram = flushBitIndexToRam(built, 0);

		final BytesRef[] slices = querySlices("4009980000");
		assertTrue(slices.length >= 2, "long numeric query should produce multiple anchor slices");
		final long[] keys = FpGroupHotNgramBitIndex.selectiveKeysForSlices(slices);
		assertTrue(keys.length >= 2, "each slice should contribute a bucket key");

		try (IndexInput in = openRamBits(ram.dir)) {
			final FpGroupHotNgramBitIndex full = FpGroupHotNgramBitIndex.readfrom(in, ram.blockInfo);
			try (FpGroupHotNgramBitIndex.SelectiveSession session = FpGroupHotNgramBitIndex.SelectiveSession.open(in,
					ram.blockInfo, keys, keys)) {
				session.runSkip1Phase();
				session.runSkip2Phase();
				final FpGroupHotNgramBitIndex selective = session.finishKeysOrderPhase();
				for (BytesRef slice : slices) {
					assertArrayEquals(full.lookupCommonOrders(slice), selective.lookupCommonOrders(slice),
							"slice=" + new String(slice.bytes, slice.offset, slice.length, StandardCharsets.UTF_8));
				}
				selective.releasePooledMaps();
			}
		}
	}

	@Test
	void selectiveRead_prefix40099_alsoResolvesLongerQuerySlices() throws Exception {
		final FpGroupHotNgramBitIndex built = buildBitIndexFromCommonPayloads(List.of("4009980000"));
		final RamFlushedBitIndex ram = flushBitIndexToRam(built, 0);

		final BytesRef[] shortSlices = querySlices("40099");
		final BytesRef[] longSlices = querySlices("4009980000");

		try (IndexInput in = openRamBits(ram.dir)) {
			final FpGroupHotNgramBitIndex full = FpGroupHotNgramBitIndex.readfrom(in, ram.blockInfo);
			final long[] longKeys = FpGroupHotNgramBitIndex.selectiveKeysForSlices(longSlices);
			in.seek(0);
			final FpGroupHotNgramBitIndex selective = FpGroupHotNgramBitIndex.readfromBanksSelective(in, ram.blockInfo,
					longKeys, longKeys);
			for (BytesRef slice : shortSlices) {
				assertArrayEquals(full.lookupCommonOrders(slice), selective.lookupCommonOrders(slice));
			}
			for (BytesRef slice : longSlices) {
				assertArrayEquals(full.lookupCommonOrders(slice), selective.lookupCommonOrders(slice),
						"slice=" + new String(slice.bytes, slice.offset, slice.length, StandardCharsets.UTF_8));
			}
			selective.releasePooledMaps();
		}
	}
}
