package cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model;

import java.util.Arrays;

/**
 * 指向一段字节区间的轻量引用：底层数组 + 偏移 + 长度。
 *
 * <p>用途是避免把同一份原始二进制反复切片拷贝成很多独立 {@code byte[]}，
 * 让上层在需要时再按引用读取或按需复制。
 */
public final class ByteRef {
    private final byte[] source;
    private final int offset;
    private final int length;

    public ByteRef(byte[] source, int offset, int length) {
        if (source == null) {
            throw new IllegalArgumentException("source must not be null");
        }
        if (offset < 0) {
            throw new IllegalArgumentException("offset must be >= 0");
        }
        if (length < 0) {
            throw new IllegalArgumentException("length must be >= 0");
        }
        if (offset > source.length) {
            throw new IllegalArgumentException("offset must be <= source.length");
        }
        if (offset + length < offset || offset + length > source.length) {
            throw new IllegalArgumentException("offset + length out of range");
        }
        this.source = source;
        this.offset = offset;
        this.length = length;
    }

    public static ByteRef wrap(byte[] source) {
        return new ByteRef(source, 0, source.length);
    }

    public byte[] getSourceUnsafe() {
        return source;
    }

    public int getOffset() {
        return offset;
    }

    public int getLength() {
        return length;
    }

    public byte byteAt(int index) {
        if (index < 0 || index >= length) {
            throw new IllegalArgumentException("index out of range");
        }
        return source[offset + index];
    }

    /** 按当前区间复制为独立数组。 */
    public byte[] copyBytes() {
        return Arrays.copyOfRange(source, offset, offset + length);
    }
}
