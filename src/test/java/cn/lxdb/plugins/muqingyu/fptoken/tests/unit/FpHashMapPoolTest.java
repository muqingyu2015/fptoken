package cn.lxdb.plugins.muqingyu.fptoken.tests.unit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpTermKey;
import cn.lxdb.plugins.muqingyu.fptoken.ngram.Counter;
import cn.lxdb.plugins.muqingyu.fptoken.pool.FpHashMapPoolHub;
import cn.lxdb.plugins.muqingyu.fptoken.pool.FpHashMapPoolIds;
import cn.lxdb.plugins.muqingyu.fptoken.pool.FpHashMapPoolSettings;

@Tag("lxdb-runtime")
class FpHashMapPoolTest {

	private final MutableClock clock = new MutableClock();

	@AfterEach
	void tearDown() {
		FpHashMapPoolHub.resetForTests();
		// 恢复 FpHashMapPoolIds 静态定义
		FpHashMapPoolHub.definePool(FpHashMapPoolIds.NGRAM_OCCURRENCE, FpTermKey.class, Counter.class, 4096);
	}

	@Test
	void lazyRegisterOnFirstBorrowWithTypes() {
		FpHashMapPoolHub.setClock(clock);
		assertTrue(!FpHashMapPoolHub.isRegistered(50));

		final HashMap<String, Integer> map = FpHashMapPoolHub.borrow(50, String.class, Integer.class, 8);
		assertTrue(FpHashMapPoolHub.isRegistered(50));
		map.put("a", 1);
		FpHashMapPoolHub.release(50, map);

		final HashMap<String, Integer> again = FpHashMapPoolHub.borrow(50);
		assertTrue(again.isEmpty());
		assertSame(map, again);
	}

	@Test
	void predefinedPoolBorrowByIdOnly() {
		FpHashMapPoolHub.setClock(clock);
		final HashMap<FpTermKey, Counter> map = FpHashMapPoolHub.borrow(FpHashMapPoolIds.NGRAM_OCCURRENCE);
		FpHashMapPoolHub.release(FpHashMapPoolIds.NGRAM_OCCURRENCE, map);
	}

	@Test
	void isolatedPoolsReuseDistinctMaps() {
		FpHashMapPoolHub.setClock(clock);
		FpHashMapPoolHub.definePool(1, String.class, Integer.class, 8);
		FpHashMapPoolHub.definePool(2, FpTermKey.class, Counter.class, 16);

		final HashMap<String, Integer> a1 = FpHashMapPoolHub.borrow(1);
		a1.put("x", 1);
		FpHashMapPoolHub.release(1, a1);

		final HashMap<FpTermKey, Counter> b1 = FpHashMapPoolHub.borrow(2);
		FpHashMapPoolHub.release(2, b1);

		final HashMap<String, Integer> a2 = FpHashMapPoolHub.borrow(1);
		assertTrue(a2.isEmpty());
		assertSame(a1, a2);
		assertEquals(1, FpHashMapPoolHub.stats(1).createdCount());
		assertEquals(1, FpHashMapPoolHub.stats(2).createdCount());
	}

	@Test
	void releaseClearsContentForReuse() {
		FpHashMapPoolHub.setClock(clock);
		final HashMap<FpTermKey, Counter> first = FpHashMapPoolHub.borrow(FpHashMapPoolIds.NGRAM_OCCURRENCE);
		first.put(FpTermKey.copyOf(new org.apache.lucene.util.BytesRef("ab")), new Counter(1, null));
		FpHashMapPoolHub.release(FpHashMapPoolIds.NGRAM_OCCURRENCE, first);

		final HashMap<FpTermKey, Counter> second = FpHashMapPoolHub.borrow(FpHashMapPoolIds.NGRAM_OCCURRENCE);
		assertSame(first, second);
		assertTrue(second.isEmpty());
	}

	@Test
	void redefineWithDifferentTypesFails() {
		FpHashMapPoolHub.borrow(7, String.class, String.class, 4);
		assertThrows(IllegalStateException.class, () -> FpHashMapPoolHub.borrow(7, String.class, Integer.class, 4));
	}

	@Test
	void reuseLifetimeExpiresAfterMaxAge() {
		final FpHashMapPoolSettings settings = new FpHashMapPoolSettings(1_000L, 1_000L, 60_000L, 8);
		FpHashMapPoolHub.setClock(clock);
		FpHashMapPoolHub.definePool(9, String.class, String.class, 4, settings);

		final HashMap<String, String> first = FpHashMapPoolHub.borrow(9);
		FpHashMapPoolHub.release(9, first);

		clock.advance(1_001L);
		final HashMap<String, String> second = FpHashMapPoolHub.borrow(9);
		assertTrue(second != first);
		assertEquals(2, FpHashMapPoolHub.stats(9).createdCount());
		assertEquals(1, FpHashMapPoolHub.stats(9).expiredIdleEvictCount());
	}

	@Test
	void forgottenReleaseDoesNotGrowLeasedAfterMapDropped() {
		FpHashMapPoolHub.setClock(clock);
		FpHashMapPoolHub.definePool(88, String.class, Integer.class, 4);

		HashMap<String, Integer> map = FpHashMapPoolHub.borrow(88);
		assertEquals(1, FpHashMapPoolHub.stats(88).leasedSize());
		map = null;

		for (int i = 0; i < 5; i++) {
			System.gc();
		}
		FpHashMapPoolHub.cleanupNow();

		final HashMap<String, Integer> map2 = FpHashMapPoolHub.borrow(88);
		try {
			assertEquals(1, FpHashMapPoolHub.stats(88).leasedSize());
		} finally {
			FpHashMapPoolHub.release(88, map2);
		}
	}

	@Test
	void releaseUnknownMapFails() {
		FpHashMapPoolHub.setClock(clock);
		FpHashMapPoolHub.definePool(3, String.class, String.class, 0);
		final HashMap<String, String> foreign = new HashMap<>();
		assertThrows(IllegalStateException.class, () -> FpHashMapPoolHub.release(3, foreign));
	}

	private static final class MutableClock implements cn.lxdb.plugins.muqingyu.fptoken.pool.FpHashMapPoolClock {
		private long now;

		@Override
		public long nowMs() {
			return now;
		}

		void advance(long deltaMs) {
			now += deltaMs;
		}
	}
}
