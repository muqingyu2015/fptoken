package cn.lxdb.plugins.muqingyu.fptoken.dataset.block;

/**
 * PRAL 分段锁池：相对 32 核基准等比缩放。
 * 有效核心数优先取环境变量 {@value #ENV_CPU_CORES}，未配置或无效时取
 * {@link Runtime#availableProcessors()}。
 */
public final class FpPralLockPools {

	private static final String ENV_CPU_CORES = "lxdb_fptoken_cpu_cores";
	private static final int REF_CPU_CORES = 8;
	private static final int EFFECTIVE_CPU_CORES = resolveCpuCores();

	private FpPralLockPools() {
	}

	public static Object[] makePralLock(int countAtRefCores) {
		final Object[] objs = new Object[pralLockCount(countAtRefCores)];
		for (int i = 0; i < objs.length; i++) {
			objs[i] = new Object();
		}
		return objs;
	}

	static int pralLockCount(int countAtRefCores) {
		return Math.max(1, countAtRefCores * EFFECTIVE_CPU_CORES / REF_CPU_CORES);
	}

	private static int resolveCpuCores() {
		final String raw = System.getenv(ENV_CPU_CORES);
		if (raw != null) {
			final String trimmed = raw.trim();
			if (!trimmed.isEmpty()) {
				try {
					final int configured = Integer.parseInt(trimmed);
					if (configured > 0) {
						return configured;
					}
				} catch (NumberFormatException ignored) {
				}
			}
		}
		int cores = Runtime.getRuntime().availableProcessors();
		if (cores < 8) {
			cores = REF_CPU_CORES;
		}
		return cores;
	}
}
