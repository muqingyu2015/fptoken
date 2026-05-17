package cn.lxdb.plugins.muqingyu.fptoken.token;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * 二进制字段滑动窗口工具：在给定字节序列上按固定窗长与步长生成若干 {@link WindowTerm}。
 *
 * <p>从 {@code bapi} 迁入本包，语义与历史实现保持一致。</p>
 */
public final class BinarySlidingWindowApi {

	/** 位图类字段默认滑窗宽度（字节）。 */
	public static final int BITSET_WINDOW_SIZE = 64;

	/** 位图类字段默认滑窗步进（字节）。 */
	public static final int BITSET_STEP_SIZE = 32;

	private BinarySlidingWindowApi() {
	}

	/**
	 * 生成滑窗列表：总长不足 {@code windowSize} 时仅一个实际长度窗；否则按步进滑动，并在末尾再对齐一次满窗
	 *（若与已有起点重复则省略）。
	 *
	 * @param source     输入字节
	 * @param offset     有效区间起点
	 * @param length     有效区间长度
	 * @param windowSize 窗宽，必须 &gt; 0
	 * @param stepSize   步长，必须 &gt; 0
	 * @return 窗口列表（可能为空输入时返回空列表）
	 */
	public static List<WindowTerm> slidingWindows(byte[] source, int offset, int length, int windowSize, int stepSize) {
		validate(source, offset, length, windowSize, stepSize);
		if (length == 0) {
			return new ArrayList<>(0);
		}

		int start = offset;
		int endExclusive = offset + length;
		List<WindowTerm> out = new ArrayList<>(estimateWindowCount(length, stepSize) + 1);
		// 记录已添加窗口的起点，避免末尾对齐窗与前面重复
		Set<Integer> seenStarts = new HashSet<>();

		// 总长不足一窗：单窗取全部剩余字节
		if (endExclusive - start < windowSize) {
			addWindowIfNew(out, seenStarts, source, start, endExclusive - start);
			return out;
		}

		// 从左向右步进，每次取满窗
		for (int pos = start; pos + windowSize <= endExclusive; pos += stepSize) {
			addWindowIfNew(out, seenStarts, source, pos, windowSize);
			if (pos + stepSize <= pos) {
				break;
			}
		}
		// 末尾对齐：最后一窗贴终点，若起点未出现过则追加
		int adjStart = endExclusive - windowSize;
		if (adjStart >= start) {
			addWindowIfNew(out, seenStarts, source, adjStart, windowSize);
		}
		return out;
	}

	/**
	 * 若起点未出现过，则拷贝窗口字节并追加 {@link WindowTerm}。
	 */
	private static void addWindowIfNew(List<WindowTerm> out, Set<Integer> seenStarts, byte[] source, int pos, int winLen) {
		if (!seenStarts.add(pos)) {
			return;
		}
		byte[] termBytes = Arrays.copyOfRange(source, pos, pos + winLen);
		out.add(new WindowTerm(termBytes, new ByteRef(source, pos, winLen)));
	}

	/**
	 * 使用默认 64 窗宽、32 步长的滑窗（与位图 FP 字段约定一致）。
	 *
	 * @param source 输入
	 * @param offset 起点
	 * @param length 长度
	 * @return {@link #slidingWindows(byte[], int, int, int, int)} 的结果
	 */
	public static List<WindowTerm> bitsetWindows64Step32(byte[] source, int offset, int length) {
		return slidingWindows(source, offset, length, BITSET_WINDOW_SIZE, BITSET_STEP_SIZE);
	}

	/** 校验参数合法性，非法时抛 {@link IllegalArgumentException}。 */
	private static void validate(byte[] source, int offset, int length, int windowSize, int stepSize) {
		if (source == null) {
			throw new IllegalArgumentException("source must not be null");
		}
		if (offset < 0) {
			throw new IllegalArgumentException("offset must be >= 0");
		}
		if (length < 0) {
			throw new IllegalArgumentException("length must be >= 0");
		}
		if (windowSize <= 0) {
			throw new IllegalArgumentException("windowSize must be > 0");
		}
		if (stepSize <= 0) {
			throw new IllegalArgumentException("stepSize must be > 0");
		}
		if (offset > source.length) {
			throw new IllegalArgumentException("offset must be <= source.length");
		}
		if (offset + length < offset || offset + length > source.length) {
			throw new IllegalArgumentException("offset + length out of range");
		}
	}

	/** 粗略估计窗口数量，用于预分配列表容量。 */
	private static int estimateWindowCount(int length, int stepSize) {
		int estimated = (length + stepSize - 1) / stepSize;
		return Math.max(1, estimated);
	}
}
