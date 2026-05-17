package cn.lxdb.plugins.muqingyu.fptoken.token;

/**
 * Reader 文本如何转为待滑窗的 {@code byte[]}：与外部入参、索引配置中的整型码对齐。
 *
 * <p>约定：{@code 1} = UTF-8；{@code 2} = 十六进制字符串解析为原始字节。</p>
 */
public enum FpTokenBytesMode {

	/** 文本按 UTF-8 编码为字节（与历史行为一致）。 */
	UTF8(1),

	/**
	 * 文本视为十六进制串：忽略空白，每两个十六进制字符对应一个字节。
	 * 例如 {@code "48 65 6c 6c 6f"} → 5 字节。
	 */
	HEX_STRING(2);

	/** 与外部配置/RPC 传递的整型码一致。 */
	private final int code;

	/**
	 * @param code 持久化用枚举码
	 */
	FpTokenBytesMode(int code) {
		this.code = code;
	}

	/**
	 * @return 枚举码（1 或 2）
	 */
	public int getCode() {
		return code;
	}

	/**
	 * 将整型码解析为枚举；未知码抛 {@link IllegalArgumentException}。
	 *
	 * @param code 通常为 1 或 2
	 * @return 对应模式
	 */
	public static FpTokenBytesMode fromCode(int code) {
		for (FpTokenBytesMode m : values()) {
			if (m.code == code) {
				return m;
			}
		}
		throw new IllegalArgumentException("unknown FpTokenBytesMode code: " + code + " (use 1=UTF8, 2=HEX_STRING)");
	}
}
