package cn.lxdb.plugins.muqingyu.fptoken.tests.unit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpTermKey;
import cn.lxdb.plugins.muqingyu.fptoken.ngram.Counter;
import cn.lxdb.plugins.muqingyu.fptoken.pool.FpHashMapPoolHub;
import cn.lxdb.plugins.muqingyu.fptoken.pool.FpHashMapPoolIds;
import cn.lxdb.plugins.muqingyu.fptoken.pool.FpHashMapPoolLease;
import cn.lxdb.plugins.muqingyu.fptoken.pool.FpHashMapPoolSettings;

@Tag("lxdb-runtime")
class FpHashMapPoolTest {

	private final MutableClock clock = new MutableClock();

	@BeforeEach
	void setUp() {
		FpHashMapPoolHub.resetForTests();
		FpHashMapPoolHub.setClock(clock);
		FpHashMapPoolHub.definePool(FpHashMapPoolIds.NGRAM_OCCURRENCE, FpTermKey.class, Counter.class, 4096);
	}

	@AfterEach
	void tearDown() {
		FpHashMapPoolHub.resetForTests();
	}

	@Test
	void lazyRegisterOnFirstBorrowWithTypes() {
		assertTrue(!FpHashMapPoolHub.isRegistered(50));

		final FpHashMapPoolLease<String, Integer> lease = FpHashMapPoolHub.borrow(50, String.class, Integer.class, 8);
		assertTrue(FpHashMapPoolHub.isRegistered(50));
		lease.map().put("a", 1);
		FpHashMapPoolHub.release(lease);

		final FpHashMapPoolLease<String, Integer> again = FpHashMapPoolHub.borrow(50);
		assertTrue(again.map().isEmpty());
		assertSame(lease.map(), again.map());
		FpHashMapPoolHub.release(again);
	}

	@Test
	void predefinedPoolBorrowByIdOnly() {
		final FpHashMapPoolLease<FpTermKey, Counter> lease = FpHashMapPoolHub.borrow(FpHashMapPoolIds.NGRAM_OCCURRENCE);
		FpHashMapPoolHub.release(lease);
	}

	@Test
	void isolatedPoolsReuseDistinctMaps() {
		FpHashMapPoolHub.definePool(51, String.class, Integer.class, 8);
		FpHashMapPoolHub.definePool(52, FpTermKey.class, Counter.class, 16);

		final FpHashMapPoolLease<String, Integer> a1 = FpHashMapPoolHub.borrow(51);
		a1.map().put("x", 1);
		FpHashMapPoolHub.release(a1);

		final FpHashMapPoolLease<FpTermKey, Counter> b1 = FpHashMapPoolHub.borrow(52);
		FpHashMapPoolHub.release(b1);

		final FpHashMapPoolLease<String, Integer> a2 = FpHashMapPoolHub.borrow(51);
		assertTrue(a2.map().isEmpty());
		assertSame(a1.map(), a2.map());
		FpHashMapPoolHub.release(a2);
		assertEquals(1, FpHashMapPoolHub.stats(51).createdCount());
		assertEquals(1, FpHashMapPoolHub.stats(52).createdCount());
	}

	@Test
	void releaseClearsContentForReuse() {
		final FpHashMapPoolLease<FpTermKey, Counter> first = FpHashMapPoolHub.borrow(FpHashMapPoolIds.NGRAM_OCCURRENCE);
		first.map().put(FpTermKey.copyOf(new org.apache.lucene.util.BytesRef("ab")), new Counter(1, null));
		FpHashMapPoolHub.release(first);

		final FpHashMapPoolLease<FpTermKey, Counter> second = FpHashMapPoolHub.borrow(FpHashMapPoolIds.NGRAM_OCCURRENCE);
		assertSame(first.map(), second.map());
		assertTrue(second.map().isEmpty());
		FpHashMapPoolHub.release(second);
	}

	@Test
	void releaseAfterHeavyMutationStillFindsLease() {
		final FpHashMapPoolLease<FpTermKey, Counter> lease = FpHashMapPoolHub.borrow(FpHashMapPoolIds.NGRAM_OCCURRENCE);
		for (int i = 0; i < 1000; i++) {
			lease.map().put(FpTermKey.copyOf(new org.apache.lucene.util.BytesRef("k" + i)), new Counter(i, null));
		}
		FpHashMapPoolHub.release(lease);
		assertEquals(0, FpHashMapPoolHub.stats(FpHashMapPoolIds.NGRAM_OCCURRENCE).leasedSize());
	}

	@Test
	void redefineWithDifferentTypesFails() {
		FpHashMapPoolHub.borrow(7, String.class, String.class, 4);
		assertThrows(IllegalStateException.class, () -> FpHashMapPoolHub.borrow(7, String.class, Integer.class, 4));
	}

	@Test
	void reuseLifetimeExpiresAfterMaxAge() {
		final FpHashMapPoolSettings settings = new FpHashMapPoolSettings(1_000L, 1_000L, 60_000L, 8);
		FpHashMapPoolHub.definePool(9, String.class, String.class, 4, settings);

		final FpHashMapPoolLease<String, String> first = FpHashMapPoolHub.borrow(9);
		FpHashMapPoolHub.release(first);

		clock.advance(1_001L);
		final FpHashMapPoolLease<String, String> second = FpHashMapPoolHub.borrow(9);
		assertTrue(second.map() != first.map());
		FpHashMapPoolHub.release(second);
		assertEquals(2, FpHashMapPoolHub.stats(9).createdCount());
		assertEquals(1, FpHashMapPoolHub.stats(9).expiredIdleEvictCount());
	}

	@Test
	void doubleReleaseFails() {
		final FpHashMapPoolLease<String, Integer> lease = FpHashMapPoolHub.borrow(88, String.class, Integer.class, 4);
		FpHashMapPoolHub.release(lease);
		assertThrows(IllegalStateException.class, () -> FpHashMapPoolHub.release(lease));
	}

	@Test
	void releaseAfterPoolResetFails() {
		final FpHashMapPoolLease<String, String> lease = FpHashMapPoolHub.borrow(3, String.class, String.class, 0);
		FpHashMapPoolHub.resetForTests();
		assertThrows(IllegalStateException.class, () -> FpHashMapPoolHub.release(lease));
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
