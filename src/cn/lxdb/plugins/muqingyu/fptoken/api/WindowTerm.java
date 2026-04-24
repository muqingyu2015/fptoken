package cn.lxdb.plugins.muqingyu.fptoken.api;

import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.ByteRef;
import java.util.Arrays;

/**
 * 滑窗结果：包含窗口字节和其在原始输入中的范围信息。
 */
public final class WindowTerm {
    private final byte[] windowBytes;
    private final ByteRef sourceRef;

    /**
     * 前置条件：{@code windowBytes != null} 且 {@code sourceRef != null}。
     */
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
