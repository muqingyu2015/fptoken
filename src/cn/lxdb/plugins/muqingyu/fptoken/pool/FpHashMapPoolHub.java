package cn.lxdb.plugins.muqingyu.fptoken.pool;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 多槽位 HashMap 对象池：按 {@code poolId} 隔离，首次借出时自动注册。
 *
 * <p>推荐用法（借 / 还分开，不用 try）：
 * <pre>{@code
 * HashMap<FpTermKey, Counter> map = FpHashMapPoolHub.borrow(
 *     FpHashMapPoolIds.NGRAM_OCCURRENCE, FpTermKey.class, Counter.class, 4096);
 * try {
 *     // 使用 map
 * } finally {
 *     FpHashMapPoolHub.release(FpHashMapPoolIds.NGRAM_OCCURRENCE, map);
 * }
 * }</pre>
 *
 * <p>若已在 {@link FpHashMapPoolIds} 中 {@link #definePool(int, Class, Class, int)}，
 * 也可简写为 {@code borrow(poolId)} / {@code release(poolId, map)}。
 *
 * <p><b>忘记 release</b>：租约表对 map 使用弱引用；业务侧不再持有 map 时，GC 会回收 map，
 * 池内对应租约条目会被清理，<strong>不会</strong>在空闲池或 leased 里无限积压。未归还的 map 也不会被复用给其他人。
 */
public final class FpHashMapPoolHub {

	private static final ConcurrentHashMap<Integer, FpHashMapPoolDef> DEFS = new ConcurrentHashMap<>();
	private static final ConcurrentHashMap<Integer, FpHashMapPoolSlot<?, ?>> SLOTS = new ConcurrentHashMap<>();
	private static final AtomicBoolean CLEANER_STARTED = new AtomicBoolean(false);
	private static volatile FpHashMapPoolClock CLOCK = FpHashMapPoolClock.SYSTEM;
	private static volatile Thread cleanerThread;
	private static volatile long cleanerIntervalMs = FpHashMapPoolSettings.DEFAULT.cleanupIntervalMs();

	private FpHashMapPoolHub() {
	}

	public static void setClock(FpHashMapPoolClock clock) {
		CLOCK = Objects.requireNonNull(clock, "clock");
	}

	public static FpHashMapPoolClock clock() {
		return CLOCK;
	}

	/**
	 * 预定义池（可选）：类加载或启动时调用；与首次 {@link #borrow(int, Class, Class, int)} 等价。
	 * 可重复调用，类型一致时幂等。
	 */
	public static <K, V> void definePool(int poolId, Class<K> keyType, Class<V> valueType, int initialCapacity) {
		ensureRegistered(poolId, keyType, valueType, initialCapacity, FpHashMapPoolSettings.DEFAULT);
	}

	public static <K, V> void definePool(int poolId, Class<K> keyType, Class<V> valueType, int initialCapacity,
			FpHashMapPoolSettings settings) {
		ensureRegistered(poolId, keyType, valueType, initialCapacity, settings);
	}

	/** 同 {@link #definePool(int, Class, Class, int)}，保留显式注册命名。 */
	public static <K, V> void register(int poolId, Class<K> keyType, Class<V> valueType) {
		definePool(poolId, keyType, valueType, 0);
	}

	public static <K, V> void register(int poolId, Class<K> keyType, Class<V> valueType, int initialCapacity) {
		definePool(poolId, keyType, valueType, initialCapacity);
	}

	public static <K, V> void register(int poolId, Class<K> keyType, Class<V> valueType, int initialCapacity,
			FpHashMapPoolSettings settings) {
		definePool(poolId, keyType, valueType, initialCapacity, settings);
	}

	public static boolean isRegistered(int poolId) {
		return SLOTS.containsKey(poolId);
	}

	/** 池已定义或曾借出过时，仅传 poolId 即可。 */
	public static <K, V> HashMap<K, V> borrow(int poolId) {
		if (!SLOTS.containsKey(poolId)) {
			throw new IllegalStateException(
					"poolId not defined: " + poolId + "; use borrow(poolId, keyType, valueType, initialCapacity) first");
		}
		return borrowFromSlot(poolId);
	}

	/**
	 * 首次调用时按类型自动注册；之后同 poolId 类型必须一致。
	 */
	public static <K, V> HashMap<K, V> borrow(int poolId, Class<K> keyType, Class<V> valueType) {
		return borrow(poolId, keyType, valueType, 0);
	}

	public static <K, V> HashMap<K, V> borrow(int poolId, Class<K> keyType, Class<V> valueType, int initialCapacity) {
		ensureRegistered(poolId, keyType, valueType, initialCapacity, FpHashMapPoolSettings.DEFAULT);
		return borrowFromSlot(poolId);
	}

	public static <K, V> HashMap<K, V> borrow(int poolId, Class<K> keyType, Class<V> valueType, int initialCapacity,
			FpHashMapPoolSettings settings) {
		ensureRegistered(poolId, keyType, valueType, initialCapacity, settings);
		return borrowFromSlot(poolId);
	}

	public static void release(int poolId, HashMap<?, ?> map) {
		Objects.requireNonNull(map, "map");
		releaseUnchecked(poolId, map);
	}

	public static FpHashMapPoolStats stats(int poolId) {
		return slot(poolId).snapshotStats();
	}

	public static Map<Integer, FpHashMapPoolStats> statsAll() {
		final Map<Integer, FpHashMapPoolStats> out = new HashMap<>();
		for (Map.Entry<Integer, FpHashMapPoolSlot<?, ?>> e : SLOTS.entrySet()) {
			out.put(e.getKey(), e.getValue().snapshotStats());
		}
		return out;
	}

	public static int cleanupNow() {
		int evicted = 0;
		for (FpHashMapPoolSlot<?, ?> slot : SLOTS.values()) {
			evicted += slot.evictExpiredIdle();
		}
		return evicted;
	}

	public static synchronized void shutdownCleaner() {
		final Thread t = cleanerThread;
		if (t != null) {
			t.interrupt();
			cleanerThread = null;
		}
		CLEANER_STARTED.set(false);
	}

	public static synchronized void resetForTests() {
		shutdownCleaner();
		SLOTS.clear();
		DEFS.clear();
		cleanerIntervalMs = FpHashMapPoolSettings.DEFAULT.cleanupIntervalMs();
		CLOCK = FpHashMapPoolClock.SYSTEM;
	}

	private static <K, V> void ensureRegistered(int poolId, Class<K> keyType, Class<V> valueType, int initialCapacity,
			FpHashMapPoolSettings settings) {
		Objects.requireNonNull(keyType, "keyType");
		Objects.requireNonNull(valueType, "valueType");
		Objects.requireNonNull(settings, "settings");
		final FpHashMapPoolDef def = new FpHashMapPoolDef(keyType, valueType, initialCapacity, settings);
		final FpHashMapPoolDef prevDef = DEFS.putIfAbsent(poolId, def);
		if (prevDef != null && !prevDef.matches(keyType, valueType)) {
			throw new IllegalStateException("poolId " + poolId + " already defined as ("
					+ prevDef.keyType().getName() + "," + prevDef.valueType().getName() + "), cannot redefine as ("
					+ keyType.getName() + "," + valueType.getName() + ")");
		}
		if (SLOTS.containsKey(poolId)) {
			return;
		}
		final FpHashMapPoolDef use = prevDef != null ? prevDef : def;
		@SuppressWarnings("unchecked")
		final FpHashMapPoolSlot<K, V> slot = new FpHashMapPoolSlot<>(poolId, keyType, valueType, use.initialCapacity(),
				use.settings(), CLOCK);
		final FpHashMapPoolSlot<?, ?> installed = SLOTS.putIfAbsent(poolId, slot);
		if (installed == null) {
			updateCleanerInterval(use.settings().cleanupIntervalMs());
			ensureCleanerStarted();
		}
	}

	private static <K, V> HashMap<K, V> borrowFromSlot(int poolId) {
		final FpHashMapPoolSlot<K, V> poolSlot = slot(poolId);
		@SuppressWarnings("unchecked")
		final HashMap<K, V> map = (HashMap<K, V>) poolSlot.borrowMap();
		return map;
	}

	@SuppressWarnings("unchecked")
	private static <K, V> FpHashMapPoolSlot<K, V> slot(int poolId) {
		final FpHashMapPoolSlot<?, ?> found = SLOTS.get(poolId);
		if (found == null) {
			throw new IllegalStateException("poolId not registered: " + poolId);
		}
		return (FpHashMapPoolSlot<K, V>) found;
	}

	@SuppressWarnings("unchecked")
	private static <K, V> void releaseUnchecked(int poolId, HashMap<?, ?> map) {
		final FpHashMapPoolSlot<?, ?> found = SLOTS.get(poolId);
		if (found == null) {
			throw new IllegalStateException("poolId not registered: " + poolId);
		}
		((FpHashMapPoolSlot<K, V>) found).release((HashMap<K, V>) map);
	}

	private static void updateCleanerInterval(long intervalMs) {
		cleanerIntervalMs = Math.min(cleanerIntervalMs, intervalMs);
	}

	private static void ensureCleanerStarted() {
		if (CLEANER_STARTED.compareAndSet(false, true)) {
			final Thread t = new Thread(FpHashMapPoolHub::cleanerLoop, "fp-hashmap-pool-cleaner");
			t.setDaemon(true);
			t.start();
			cleanerThread = t;
		}
	}

	private static void cleanerLoop() {
		while (!Thread.currentThread().isInterrupted()) {
			try {
				Thread.sleep(cleanerIntervalMs);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				return;
			}
			cleanupNow();
		}
	}
}
