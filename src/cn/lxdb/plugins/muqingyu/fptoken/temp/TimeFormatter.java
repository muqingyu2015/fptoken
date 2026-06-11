package cn.lxdb.plugins.muqingyu.fptoken.temp;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Locale;
import java.util.TimeZone;

/**
 * 时间格式化（临时目录按小时分组）。
 */
public final class TimeFormatter {

	private static final ThreadLocal<DateFormat> YYYY_MM_DD_HH_MM = ThreadLocal.withInitial(() -> {
		final SimpleDateFormat fmt = new SimpleDateFormat("yyyyMMddHHmm", Locale.ROOT);
		fmt.setTimeZone(TimeZone.getDefault());
		return fmt;
	});

	private TimeFormatter() {
	}

	public static DateFormat get_yyyyMMddHHmm() {
		return YYYY_MM_DD_HH_MM.get();
	}
}
