package cn.lxdb.plugins.muqingyu.fptoken.token;

import java.util.Arrays;

/**
 * 指向一段字节区间的轻量引用：底层数组 + 偏移 + 长度（不拷贝字节内容）。
 *
 * <p>从 {@code exclusivefp.model} 迁入本包，供 token 模块在零额外依赖下描述滑窗在原文中的位置。</p>
 */
public final class ByteRef {

	/** 底层字节存储（由调用方保证生命周期覆盖本引用使用期）。 */
	private final byte[] source;

	/** {@link #source} 中有效区间的起始下标（含）。 */
	private final int offset;

	/** 从 {@link #offset} 起连续有效字节个数。 */
	private final int length;

	/**
	 * @param source 非 null 字节数组
	 * @param offset   起始下标，0 且 &le; source.length
	 * @param length   长度，非负，且 offset+length &le; source.length
	 */
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

	/**
	 * 将整个数组包装为从 0 到 length 的区间引用。
	 *
	 * @param source 非 null
	 * @return {@link ByteRef}
	 */
	public static ByteRef wrap(byte[] source) {
		return new ByteRef(source, 0, source.length);
	}

	/**
	 * 返回底层数组引用（可变；调用方勿对外泄露除非明确需要零拷贝）。
	 *
	 * @return {@link #source}
	 */
	public byte[] getSourceUnsafe() {
		return source;
	}

	/** @return 区间起点下标 */
	public int getOffset() {
		return offset;
	}

	/** @return 区间长度 */
	public int getLength() {
		return length;
	}

	/**
	 * 读取区间内相对下标处的字节。
	 *
	 * @param index 0..length-1
	 * @return 字节值
	 */
	public byte byteAt(int index) {
		if (index < 0 || index >= length) {
			throw new IllegalArgumentException("index out of range");
		}
		return source[offset + index];
	}

	/**
	 * 拷贝区间内字节为新数组。
	 *
	 * @return 长度为 {@link #length} 的新数组
	 */
	public byte[] copyBytes() {
		return Arrays.copyOfRange(source, offset, offset + length);
	}
}
