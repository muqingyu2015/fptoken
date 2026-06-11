package cn.lxdb.plugins.muqingyu.fptoken.token;

import java.util.Arrays;

/**
 * 单次滑窗的输出：独立拷贝的窗口字节，以及在原始输入中的位置描述 {@link ByteRef}。
 *
 * <p>从 {@code bapi} 迁入本包。</p>
 */
public final class WindowTerm {

	/** 窗口内容的防御性拷贝（与源缓冲脱钩）。 */
	private final byte[] windowBytes;

	/** 窗口在原始 {@code byte[]} 中的 [offset, offset+length) 引用。 */
	private final ByteRef sourceRef;

	/**
	 * @param windowBytes 窗口字节（将被拷贝存储）
	 * @param sourceRef     对应原文区间，不可为 null
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

	/**
	 * @return 窗口字节的新拷贝
	 */
	public byte[] getWindowBytes() {
		return Arrays.copyOf(windowBytes, windowBytes.length);
	}

	/**
	 * @return 原文中的位置引用（非拷贝）
	 */
	public ByteRef getSourceRef() {
		return sourceRef;
	}
}
