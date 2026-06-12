package cn.lxdb.plugins.muqingyu.fptoken.pool;

/**
 * {@link FpHashMapPoolHub} 中单槽位对象池的运行参数。
 */
public final class FpHashMapPoolSettings {

	/** 默认：实例自创建起最多复用 3600 秒。 */
	public static final long DEFAULT_MAX_REUSE_LIFETIME_MS = 3_600_000L;

	/** 默认：在空闲池中超过 3600 秒未借出则淘汰。 */
	public static final long DEFAULT_MAX_IDLE_IN_POOL_MS = 3_600_000L;

	/** 默认后台清扫周期 60 秒。 */
	public static final long DEFAULT_CLEANUP_INTERVAL_MS = 60_000L;

	/** 默认每个池槽最多缓存 64 个空闲 HashMap。 */
	public static final int DEFAULT_MAX_IDLE_SIZE = 64;

	public static final FpHashMapPoolSettings DEFAULT = new FpHashMapPoolSettings(DEFAULT_MAX_REUSE_LIFETIME_MS,
			DEFAULT_MAX_IDLE_IN_POOL_MS, DEFAULT_CLEANUP_INTERVAL_MS, DEFAULT_MAX_IDLE_SIZE);

	private final long maxReuseLifetimeMs;
	private final long maxIdleInPoolMs;
	private final long cleanupIntervalMs;
	private final int maxIdleSize;

	public FpHashMapPoolSettings(long maxReuseLifetimeMs, long maxIdleInPoolMs, long cleanupIntervalMs, int maxIdleSize) {
		if (maxReuseLifetimeMs <= 0 || maxIdleInPoolMs <= 0 || cleanupIntervalMs <= 0 || maxIdleSize <= 0) {
			throw new IllegalArgumentException("pool settings must be positive");
		}
		this.maxReuseLifetimeMs = maxReuseLifetimeMs;
		this.maxIdleInPoolMs = maxIdleInPoolMs;
		this.cleanupIntervalMs = cleanupIntervalMs;
		this.maxIdleSize = maxIdleSize;
	}

	public long maxReuseLifetimeMs() {
		return maxReuseLifetimeMs;
	}

	public long maxIdleInPoolMs() {
		return maxIdleInPoolMs;
	}

	public long cleanupIntervalMs() {
		return cleanupIntervalMs;
	}

	public int maxIdleSize() {
		return maxIdleSize;
	}
}
