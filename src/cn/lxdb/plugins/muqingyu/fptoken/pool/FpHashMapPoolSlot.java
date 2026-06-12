package cn.lxdb.plugins.muqingyu.fptoken.pool;

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * 单一 {@code (K,V)} 类型的 HashMap 对象池槽位；借出跟踪按 {@code leaseId} 强引用，直到 {@link #release}。
 * <p>
 * {@link HashMap#clear()} 在锁外执行，避免大 map 清空长时间阻塞其它 borrow/release。
 */
final class FpHashMapPoolSlot<K, V> {

	private static final class Holder<K, V> {
		final HashMap<K, V> map;
		final long bornAtMs;
		long lastIdleAtMs;

		Holder(HashMap<K, V> map, long bornAtMs) {
			this.map = map;
			this.bornAtMs = bornAtMs;
			this.lastIdleAtMs = bornAtMs;
		}
	}

	private static final class ActiveLease<K, V> {
		final HashMap<K, V> map;
		final long bornAtMs;

		ActiveLease(HashMap<K, V> map, long bornAtMs) {
			this.map = map;
			this.bornAtMs = bornAtMs;
		}
	}

	private static final class ReleaseDisposition {
		final HashMap<?, ?> map;
		final long bornAtMs;
		final boolean returnToIdle;

		ReleaseDisposition(HashMap<?, ?> map, long bornAtMs, boolean returnToIdle) {
			this.map = map;
			this.bornAtMs = bornAtMs;
			this.returnToIdle = returnToIdle;
		}
	}

	private final int poolId;
	private final Class<K> keyType;
	private final Class<V> valueType;
	private final int initialCapacity;
	private final FpHashMapPoolSettings settings;
	private final FpHashMapPoolClock clock;

	private final ArrayDeque<Holder<K, V>> idle = new ArrayDeque<>();
	private final Map<Long, ActiveLease<K, V>> leased = new HashMap<>();
	private long nextLeaseId = 1L;

	private long borrowCount;
	private long releaseCount;
	private long createdCount;
	private long expiredReuseCount;
	private long expiredIdleEvictCount;

	FpHashMapPoolSlot(int poolId, Class<K> keyType, Class<V> valueType, int initialCapacity,
			FpHashMapPoolSettings settings, FpHashMapPoolClock clock) {
		this.poolId = poolId;
		this.keyType = Objects.requireNonNull(keyType, "keyType");
		this.valueType = Objects.requireNonNull(valueType, "valueType");
		this.initialCapacity = Math.max(0, initialCapacity);
		this.settings = Objects.requireNonNull(settings, "settings");
		this.clock = Objects.requireNonNull(clock, "clock");
	}

	int poolId() {
		return poolId;
	}

	Class<K> keyType() {
		return keyType;
	}

	Class<V> valueType() {
		return valueType;
	}

	FpHashMapPoolSettings settings() {
		return settings;
	}

	long cleanupIntervalMs() {
		return settings.cleanupIntervalMs();
	}

	FpHashMapPoolLease<K, V> borrowLease() {
		final HashMap<K, V> map;
		final long bornAtMs;
		final long leaseId;
		final boolean clearBeforeUse;
		synchronized (this) {
			final long now = clock.nowMs();
			final Holder<K, V> reused = pollReusableHolderLocked(now);
			if (reused == null) {
				map = newHashMap();
				bornAtMs = now;
				createdCount++;
				clearBeforeUse = false;
			} else {
				map = reused.map;
				bornAtMs = reused.bornAtMs;
				clearBeforeUse = true;
			}
			leaseId = nextLeaseId++;
			leased.put(leaseId, new ActiveLease<>(map, bornAtMs));
			borrowCount++;
		}
		if (clearBeforeUse) {
			clearOutsideLock(map);
		}
		return new FpHashMapPoolLease<>(poolId, leaseId, map, bornAtMs);
	}

	void release(FpHashMapPoolLease<K, V> lease) {
		Objects.requireNonNull(lease, "lease");
		if (lease.poolId() != poolId) {
			throw new IllegalStateException(
					"lease poolId=" + lease.poolId() + " does not match slot poolId=" + poolId);
		}
		lease.ensureReleasable();
		final ReleaseDisposition disposition = removeLeaseLocked(lease);
		clearOutsideLock(disposition.map);
		if (disposition.returnToIdle) {
			offerIdleLocked(disposition.map, disposition.bornAtMs);
		}
	}

	synchronized int evictExpiredIdle() {
		final long now = clock.nowMs();
		int evicted = 0;
		final int size = idle.size();
		for (int i = 0; i < size; i++) {
			final Holder<K, V> holder = idle.pollFirst();
			if (holder == null) {
				break;
			}
			if (isReuseLifetimeExpired(holder.bornAtMs, now) || isIdleExpired(holder, now)) {
				evicted++;
				expiredIdleEvictCount++;
			} else {
				idle.addLast(holder);
			}
		}
		return evicted;
	}

	synchronized FpHashMapPoolStats snapshotStats() {
		return new FpHashMapPoolStats(poolId, idle.size(), leased.size(), borrowCount, releaseCount, createdCount,
				expiredReuseCount, expiredIdleEvictCount);
	}

	private synchronized ReleaseDisposition removeLeaseLocked(FpHashMapPoolLease<K, V> lease) {
		final ActiveLease<K, V> active = leased.remove(lease.leaseId());
		if (active == null) {
			throw new IllegalStateException("unknown or already released lease: poolId=" + poolId + " leaseId="
					+ lease.leaseId());
		}
		if (active.map != lease.map()) {
			throw new IllegalStateException("lease map mismatch: poolId=" + poolId + " leaseId=" + lease.leaseId());
		}
		lease.markReleased();
		releaseCount++;
		final long now = clock.nowMs();
		if (isReuseLifetimeExpired(active.bornAtMs, now)) {
			expiredReuseCount++;
			return new ReleaseDisposition(active.map, active.bornAtMs, false);
		}
		return new ReleaseDisposition(active.map, active.bornAtMs, idle.size() < settings.maxIdleSize());
	}

	private synchronized void offerIdleLocked(HashMap<?, ?> map, long bornAtMs) {
		if (idle.size() >= settings.maxIdleSize()) {
			return;
		}
		@SuppressWarnings("unchecked")
		final HashMap<K, V> typed = (HashMap<K, V>) map;
		final Holder<K, V> holder = new Holder<>(typed, bornAtMs);
		holder.lastIdleAtMs = clock.nowMs();
		idle.addLast(holder);
	}

	private Holder<K, V> pollReusableHolderLocked(long now) {
		while (true) {
			final Holder<K, V> holder = idle.pollFirst();
			if (holder == null) {
				return null;
			}
			if (isReuseLifetimeExpired(holder.bornAtMs, now) || isIdleExpired(holder, now)) {
				expiredIdleEvictCount++;
				continue;
			}
			return holder;
		}
	}

	@SuppressWarnings("unchecked")
	private static void clearOutsideLock(HashMap<?, ?> map) {
		((HashMap<Object, Object>) map).clear();
	}

	private HashMap<K, V> newHashMap() {
		if (initialCapacity <= 0) {
			return new HashMap<>();
		}
		return new HashMap<>(initialCapacity);
	}

	private boolean isReuseLifetimeExpired(long bornAtMs, long now) {
		return now - bornAtMs >= settings.maxReuseLifetimeMs();
	}

	private boolean isIdleExpired(Holder<K, V> holder, long now) {
		return now - holder.lastIdleAtMs >= settings.maxIdleInPoolMs();
	}
}
