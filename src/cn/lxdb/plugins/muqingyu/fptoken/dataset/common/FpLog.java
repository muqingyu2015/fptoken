package cn.lxdb.plugins.muqingyu.fptoken.dataset.common;

import cn.lxdb.plugins.muqingyu.fptoken.dataset.block.FpGroupHotNgramBitIndex;

/**
 * FP 模块统一日志格式：{@code [tag] key=value key=value ...}，Log4J 侧用 {@code +} 拼串，不用 {@code {}}。
 */
public final class FpLog {

	public static final String TAG_SEARCH = "fp_search";
	public static final String TAG_WRITE = "fp_write";
	public static final String TAG_REBUILD = "fp_rebuild";
	public static final String TAG_ORIGINAL = "fp_original";
	public static final String TAG_BITINDEX = "fp_bitindex";
	public static final String TAG_TOKEN = "fp_token";

	private FpLog() {
	}

	public static StringBuilder kv() {
		return new StringBuilder(128);
	}

	public static StringBuilder append(StringBuilder sb, String key, Object value) {
		if (sb.length() > 0) {
			sb.append(' ');
		}
		sb.append(key).append('=');
		if (value == null) {
			sb.append("null");
		} else {
			sb.append(value);
		}
		return sb;
	}

	public static String line(String tag, StringBuilder fields) {
		return '[' + tag + ']' + (fields.length() > 0 ? " " + fields : "");
	}

	public static String line(String tag, String key, Object value) {
		return line(tag, append(kv(), key, value));
	}

	/** 查询 trace 关联（{@link cn.lxdb.plugins.muqingyu.fptoken.api.FpSearch#DEBUG_UUID}）。 */
	public static String trace(String traceId, String tag, StringBuilder fields) {
		if (traceId == null || traceId.isEmpty()) {
			return line(tag, fields);
		}
		return "trace=" + traceId + ' ' + line(tag, fields);
	}

	/** 追加 slice 摘要：{@code sliceCount=2 sliceLens=2,6 slice0=...}。 */
	public static void appendSliceSummary(StringBuilder sb, org.apache.lucene.util.BytesRef[] slices) {
		if (slices == null || slices.length == 0) {
			FpLog.append(sb, "sliceCount", 0);
			return;
		}
		FpLog.append(sb, "sliceCount", slices.length);
		sb.append(' ').append("sliceLens=");
		for (int i = 0; i < slices.length; i++) {
			if (i > 0) {
				sb.append(',');
			}
			sb.append(slices[i] == null ? -1 : slices[i].length);
		}
		FpLog.append(sb, "slice0", cn.lxdb.plugins.muqingyu.fptoken.dataset.common.Utils.BytesReftoString(slices[0]));
		if (slices[0] != null && slices[0].length > 0) {
			FpLog.append(sb, "slice0Hex", bytesToHex(slices[0].bytes, slices[0].offset, slices[0].length));
		}
	}

	/** 追加 bucketKey 列表：{@code bucketKeys=lenIdx:bucketHex,...}。 */
	public static void appendBucketKeys(StringBuilder sb, long[] keys) {
		if (keys == null || keys.length == 0) {
			FpLog.append(sb, "bucketKeys", "");
			return;
		}
		sb.append(' ').append("bucketKeys=");
		for (int i = 0; i < keys.length; i++) {
			if (i > 0) {
				sb.append(',');
			}
			final long key = keys[i];
			sb.append(FpGroupHotNgramBitIndex.unpackLenIdx(key)).append(':');
			sb.append(Integer.toHexString(FpGroupHotNgramBitIndex.unpackBucketIndex(key)));
		}
	}

	public static String bytesToHex(byte[] buf, int off, int len) {
		if (buf == null || len <= 0) {
			return "";
		}
		final StringBuilder hex = new StringBuilder(len * 2);
		for (int i = 0; i < len; i++) {
			final int b = buf[off + i] & 0xFF;
			hex.append(Character.forDigit(b >>> 4, 16));
			hex.append(Character.forDigit(b & 0x0F, 16));
		}
		return hex.toString();
	}
}
