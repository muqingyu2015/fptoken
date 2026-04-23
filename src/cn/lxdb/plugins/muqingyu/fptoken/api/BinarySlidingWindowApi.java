package cn.lxdb.plugins.muqingyu.fptoken.api;

import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.ByteRef;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * 二进制字段滑动窗口 API。
 *
 * <p>用途：
 * 1) 生成用于检索过滤（BitSet）的逻辑窗口；
 * 2) 生成用于存储（TermVector）的固定分段。
 *
 * <p>推荐组合（与当前方案一致）：
 * - TermVector：32B 窗口，步长 32B（无重叠）
 * - BitSet：64B 窗口，步长 32B（交叉）
 */
public final class BinarySlidingWindowApi {

    public static final int TERM_VECTOR_WINDOW_SIZE = 32;
    public static final int TERM_VECTOR_STEP_SIZE = 32;
    public static final int BITSET_WINDOW_SIZE = 64;
    public static final int BITSET_STEP_SIZE = 32;

    private BinarySlidingWindowApi() {
    }

    /**
     * 通用滑动窗口入口。
     *
     * <p>边界规则：
     * - 若 {@code length == 0}，返回空列表；
     * - 尾部不足一个完整窗口时，仍保留最后一个“短窗口”；
     * - 输出顺序严格按偏移从小到大。
     */
    public static List<WindowTerm> slidingWindows(
            byte[] source,
            int offset,
            int length,
            int windowSize,
            int stepSize
    ) {
        validate(source, offset, length, windowSize, stepSize);
        if (length == 0) {
            return new ArrayList<>(0);
        }

        int start = offset;
        int endExclusive = offset + length;
        List<WindowTerm> out = new ArrayList<>(estimateWindowCount(length, stepSize));

        for (int pos = start; pos < endExclusive; pos += stepSize) {
            int winLen = Math.min(windowSize, endExclusive - pos);
            if (winLen <= 0) {
                break;
            }
            byte[] termBytes = Arrays.copyOfRange(source, pos, pos + winLen);
            out.add(new WindowTerm(termBytes, new ByteRef(source, pos, winLen)));
            if (winLen < windowSize) {
                break;
            }
            if (pos + stepSize <= pos) {
                break;
            }
        }
        return out;
    }

    /** TermVector：32B 一段，无重叠。 */
    public static List<WindowTerm> termVectors32(
            byte[] source,
            int offset,
            int length
    ) {
        return slidingWindows(source, offset, length, TERM_VECTOR_WINDOW_SIZE, TERM_VECTOR_STEP_SIZE);
    }

    /** BitSet 逻辑窗口：64B 窗口，步长 32B（交叉）。 */
    public static List<WindowTerm> bitsetWindows64Step32(
            byte[] source,
            int offset,
            int length
    ) {
        return slidingWindows(source, offset, length, BITSET_WINDOW_SIZE, BITSET_STEP_SIZE);
    }

    private static void validate(
            byte[] source,
            int offset,
            int length,
            int windowSize,
            int stepSize
    ) {
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

    private static int estimateWindowCount(int length, int stepSize) {
        int estimated = (length + stepSize - 1) / stepSize;
        return Math.max(1, estimated);
    }

    /**
     * 滑窗结果：包含窗口字节和其在原始输入中的范围信息。
     */
    public static final class WindowTerm {
        private final byte[] windowBytes;
        private final ByteRef sourceRef;

        public WindowTerm(byte[] windowBytes, ByteRef sourceRef) {
            if (windowBytes == null) {
                throw new IllegalArgumentException("windowBytes must not be null");
            }
            if (sourceRef == null) {
                throw new IllegalArgumentException("sourceRef must not be null");
            }
            this.windowBytes = Arrays.copyOf(windowBytes, windowBytes.length);
            this.sourceRef = sourceRef;
        }

        public byte[] getWindowBytes() {
            return Arrays.copyOf(windowBytes, windowBytes.length);
        }

        public ByteRef getSourceRef() {
            return sourceRef;
        }
    }
}
