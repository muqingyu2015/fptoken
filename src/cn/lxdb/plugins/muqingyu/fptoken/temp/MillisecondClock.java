package cn.lxdb.plugins.muqingyu.fptoken.temp;

/**
 * 毫秒时钟（供 {@link ScheduledTempCleanup} 使用）。
 */
public final class MillisecondClock {

	public static final MillisecondClock CLOCK = new MillisecondClock();

	private MillisecondClock() {
	}

	public long now() {
		return System.currentTimeMillis();
	}
}
