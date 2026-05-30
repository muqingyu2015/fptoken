package cn.lxdb.plugins.muqingyu.fptoken.config;

/**
 * Lucene 8.0 FP（指纹）检索相关字段命名约定：通过字段名后缀识别是否为 FP token 域，
 * 以便查询/索引侧走专用路径。
 */
public final class Lucene80FPSearchConfig {

	/** 二进制 FP token 字段名后缀片段（与字段名以 {@code _} 拼接）。 */
	public static final String FP_TOKEN_NAME_BIN = "bfp";

	/** 字符串 FP token 字段名后缀片段。 */
	public static final String FP_TOKEN_NAME_STR = "sfp";


	/**
	 * 判断字段名是否为 FP token 域（以 {@code _bfp} 或 {@code _sfp} 结尾）。
	 *
	 * @param field Lucene 字段名，可为 null
	 * @return true 表示按约定视为 FP 字段
	 */
	public static boolean isFpField(String field) {
		if (field != null
				&& (field.endsWith("_" + FP_TOKEN_NAME_BIN) || field.endsWith("_" + FP_TOKEN_NAME_STR))) {
			return true;
		}
		return false;
	}


	public static final int NGRAM_MIN = 1;

	public static final int NGRAM_MAX = 6;

	/** 热词频率挖掘阈值（{@link cn.lxdb.plugins.muqingyu.fptoken.dataset.block.FpGroupHotNgramRebuild}）及「按档计数」上限。 */
	public static final int HOT_TIER_TERM_COUNT_THRESHOLD = 16;

	public static final int BUCKETS = 256;

	public static final short DEFAULT_INDEX_ID = 0;

	public static boolean PRINT_DEBUG=true;
}
