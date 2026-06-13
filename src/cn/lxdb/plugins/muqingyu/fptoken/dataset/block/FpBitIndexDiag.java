package cn.lxdb.plugins.muqingyu.fptoken.dataset.block;

import java.util.Locale;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;

import cn.lxdb.plugins.muqingyu.fptoken.config.Lucene80FPSearchConfig;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpLog;

/** v7 bitindex 写/查诊断（{@link Lucene80FPSearchConfig#LOG_FP_BITINDEX_DIAG}，DEBUG）。 */
public final class FpBitIndexDiag {

	private static final AtomicLong LOOKUP_TOTAL = new AtomicLong();
	private static final AtomicLong SKIP_INTERCEPT = new AtomicLong();

	private FpBitIndexDiag() {
	}

	public static void stageLenRow(Logger log, String tier, int lenIdx, int entryCount) {
		if (!Lucene80FPSearchConfig.LOG_FP_BITINDEX_DIAG) {
			return;
		}
		final StringBuilder sb = FpLog.kv();
		FpLog.append(sb, "event", "stageLenRow");
		FpLog.append(sb, "tier", tier);
		FpLog.append(sb, "lenIdx", lenIdx);
		FpLog.append(sb, "entryCount", entryCount);
		FpLog.debugLine(log, FpLog.TAG_BITINDEX, sb);
	}

	public static void finalizePool(Logger log, String tier, int lenIdx, String part, long bytes) {
		if (!Lucene80FPSearchConfig.LOG_FP_BITINDEX_DIAG) {
			return;
		}
		final StringBuilder sb = FpLog.kv();
		FpLog.append(sb, "event", "finalizePool");
		FpLog.append(sb, "tier", tier);
		FpLog.append(sb, "lenIdx", lenIdx);
		FpLog.append(sb, "part", part);
		FpLog.append(sb, "bytes", bytes);
		FpLog.debugLine(log, FpLog.TAG_BITINDEX, sb);
	}

	public static void selectiveTierRead(Logger log, String tier, int lenIdx, long skip1Off, long skip2Off, long keysOff,
			long orderOff) {
		if (!Lucene80FPSearchConfig.LOG_FP_BITINDEX_DIAG) {
			return;
		}
		final StringBuilder sb = FpLog.kv();
		FpLog.append(sb, "event", "selectiveTierRead");
		FpLog.append(sb, "tier", tier);
		FpLog.append(sb, "lenIdx", lenIdx);
		FpLog.append(sb, "skip1Off", skip1Off);
		FpLog.append(sb, "skip2Off", skip2Off);
		FpLog.append(sb, "keysOff", keysOff);
		FpLog.append(sb, "orderOff", orderOff);
		FpLog.debugLine(log, FpLog.TAG_BITINDEX, sb);
	}

	public static void skip1LazyLoad(Logger log, int lenIdx, int entryCount, int skip1Count, int globalMin,
			int globalMax) {
		if (!Lucene80FPSearchConfig.LOG_FP_BITINDEX_DIAG) {
			return;
		}
		final StringBuilder sb = FpLog.kv();
		FpLog.append(sb, "event", "skip1LazyLoad");
		FpLog.append(sb, "lenIdx", lenIdx);
		FpLog.append(sb, "entryCount", entryCount);
		FpLog.append(sb, "skip1Count", skip1Count);
		FpLog.append(sb, "globalMin", Integer.toHexString(globalMin));
		FpLog.append(sb, "globalMax", Integer.toHexString(globalMax));
		FpLog.debugLine(log, FpLog.TAG_BITINDEX, sb);
	}

	public static void skip2SegmentLoad(Logger log, int lenIdx, int skip1Segment, int skip2Count) {
		if (!Lucene80FPSearchConfig.LOG_FP_BITINDEX_DIAG) {
			return;
		}
		final StringBuilder sb = FpLog.kv();
		FpLog.append(sb, "event", "skip2SegmentLoad");
		FpLog.append(sb, "lenIdx", lenIdx);
		FpLog.append(sb, "skip1Segment", skip1Segment);
		FpLog.append(sb, "skip2Count", skip2Count);
		FpLog.debugLine(log, FpLog.TAG_BITINDEX, sb);
	}

	public static void lookupOrders(Logger log, int lenIdx, int bucket, String stage, int orderCount) {
		if (!Lucene80FPSearchConfig.LOG_FP_BITINDEX_DIAG) {
			return;
		}
		final long total = LOOKUP_TOTAL.incrementAndGet();
		if (stage.startsWith("skip1") || stage.startsWith("skip2")) {
			SKIP_INTERCEPT.incrementAndGet();
		}
		final long intercept = SKIP_INTERCEPT.get();
		final StringBuilder sb = FpLog.kv();
		FpLog.append(sb, "event", "lookupOrders");
		FpLog.append(sb, "lenIdx", lenIdx);
		FpLog.append(sb, "bucket", Integer.toHexString(bucket));
		FpLog.append(sb, "stage", stage);
		FpLog.append(sb, "orderCount", orderCount);
		FpLog.append(sb, "skipInterceptRate",
				String.format(Locale.ROOT, "%.4f", total == 0L ? 0D : (double) intercept / (double) total));
		FpLog.debugLine(log, FpLog.TAG_BITINDEX, sb);
	}
}
