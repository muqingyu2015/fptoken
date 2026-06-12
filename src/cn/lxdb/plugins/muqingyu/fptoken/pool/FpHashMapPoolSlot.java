package cn.lxdb.plugins.muqingyu.fptoken.pool;

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * 单一 {@code (K,V)} 类型的 HashMap 对象池槽位；借出跟踪按 {@code leaseId} 强引用，直到 {@link #release}。
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

	synchronized FpHashMapPoolLease<K, V> borrowLease() {
		final long now = clock.nowMs();
		final Holder<K, V> holder = pollReusableHolder(now);
		final HashMap<K, V> map;
		final long bornAtMs;
		if (holder == null) {
			map = newHashMap();
			bornAtMs = now;
			createdCount++;
		} else {
			map = holder.map;
			bornAtMs = holder.bornAtMs;
		}
		final long leaseId = nextLeaseId++;
		leased.put(leaseId, new ActiveLease<>(map, bornAtMs));
		borrowCount++;
		return new FpHashMapPoolLease<>(poolId, leaseId, map, bornAtMs);
	}

	synchronized void release(FpHashMapPoolLease<K, V> lease) {
		Objects.requireNonNull(lease, "lease");
		if (lease.poolId() != poolId) {
			throw new IllegalStateException(
					"lease poolId=" + lease.poolId() + " does not match slot poolId=" + poolId);
		}
		lease.ensureReleasable();
		final ActiveLease<K, V> active = leased.remove(lease.leaseId());
		if (active == null) {
			throw new IllegalStateException("unknown or already released lease: poolId=" + poolId + " leaseId="
					+ lease.leaseId());
		}
		if (active.map != lease.map()) {
			throw new IllegalStateException("lease map mismatch: poolId=" + poolId + " leaseId=" + lease.leaseId());
		}
		lease.markReleased();
		final HashMap<K, V> map = active.map;
		map.clear();
		releaseCount++;
		final long now = clock.nowMs();
		if (isReuseLifetimeExpired(active.bornAtMs, now)) {
			expiredReuseCount++;
			return;
		}
		if (idle.size() < settings.maxIdleSize()) {
			final Holder<K, V> holder = new Holder<>(map, active.bornAtMs);
			holder.lastIdleAtMs = now;
			idle.addLast(holder);
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

	private Holder<K, V> pollReusableHolder(long now) {
		while (true) {
			final Holder<K, V> holder = idle.pollFirst();
			if (holder == null) {
				return null;
			}
			if (isReuseLifetimeExpired(holder.bornAtMs, now) || isIdleExpired(holder, now)) {
				expiredIdleEvictCount++;
				continue;
			}
			holder.map.clear();
			return holder;
		}
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
