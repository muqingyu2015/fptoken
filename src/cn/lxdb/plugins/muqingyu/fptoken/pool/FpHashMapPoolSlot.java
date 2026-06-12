package cn.lxdb.plugins.muqingyu.fptoken.pool;

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.WeakHashMap;

/**
 * 单一 {@code (K,V)} 类型的 HashMap 对象池槽位。
 * <p>
 * 借出跟踪使用 {@link WeakHashMap}：若调用方忘记 {@link FpHashMapPoolHub#release} 且不再持有 map 引用，
 * map 可被 GC 回收，池不会长期强引用导致泄漏。未归还的 map 不会进入空闲队列。
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

	/** 借出元数据；不持有 map 引用，避免妨碍 GC。 */
	private static final class LeaseMeta {
		final long bornAtMs;

		LeaseMeta(long bornAtMs) {
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
	/** key 为弱引用：调用方丢弃 map 后条目可被 GC 清理，不会一直占着 leased 计数。 */
	private final Map<HashMap<K, V>, LeaseMeta> leased = new WeakHashMap<>();

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

	synchronized HashMap<K, V> borrowMap() {
		expungeStaleLeases();
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
		leased.put(map, new LeaseMeta(bornAtMs));
		borrowCount++;
		return map;
	}

	synchronized void release(HashMap<K, V> map) {
		Objects.requireNonNull(map, "map");
		expungeStaleLeases();
		final LeaseMeta meta = leased.remove(map);
		if (meta == null) {
			throw new IllegalStateException("HashMap was not borrowed from poolId=" + poolId
					+ " (already released, never borrowed, or lease was abandoned by GC)");
		}
		map.clear();
		releaseCount++;
		final long now = clock.nowMs();
		if (isReuseLifetimeExpired(meta.bornAtMs, now)) {
			expiredReuseCount++;
			return;
		}
		if (idle.size() < settings.maxIdleSize()) {
			final Holder<K, V> holder = new Holder<>(map, meta.bornAtMs);
			holder.lastIdleAtMs = now;
			idle.addLast(holder);
		}
	}

	synchronized int evictExpiredIdle() {
		expungeStaleLeases();
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
		expungeStaleLeases();
		return new FpHashMapPoolStats(poolId, idle.size(), leased.size(), borrowCount, releaseCount, createdCount,
				expiredReuseCount, expiredIdleEvictCount);
	}

	private void expungeStaleLeases() {
		// WeakHashMap 在访问时清理已被 GC 的弱键条目
		leased.size();
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
