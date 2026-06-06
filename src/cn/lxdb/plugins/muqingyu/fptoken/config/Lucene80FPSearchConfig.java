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


	/** 2~{@link #NGRAM_MAX} 字节 ngram 的桶数（须为 2 的幂）。 */
	public static final int BUCKETS = 512;

//	/** 1 字节 ngram 仅 256 种取值，单独桶行宽度，避免与 {@link #BUCKETS} 对齐浪费。 */
//	public static final int BUCKETS_LEN1 = 256;

	/** {@code choose[][]} 第二维上界（≥ 各行 {@link #bucketsForLengthIndex(int)} 最大值）。 */
	public static int maxBucketsPerRow() {
		return BUCKETS;
	}

	/** ngram 字节长度 {@code 1..NGRAM_MAX} 对应的桶数。 */
	public static int bucketsForNgramLen(int ngramLen) {
		return  BUCKETS;//ngramLen <= 1 ? BUCKETS_LEN1 :
	}

	/** 位图行下标 {@code 0..NGRAM_MAX-1}（对应 ngram 长度 {@code lenIdx+1}）。 */
	public static int bucketsForLengthIndex(int lenIdx) {
		return bucketsForNgramLen(lenIdx + 1);
	}

	/** 交错落盘时 {@code (lenIdx, bucket)} 的线性对序号。 */
	public static int bankPairIndex(int lenIdx, int bucket) {
		int pair = 0;
		for (int i = 0; i < lenIdx; i++) {
			pair += bucketsForLengthIndex(i);
		}
		return pair + bucket;
	}

	public static int totalBankPairs() {
		int total = 0;
		for (int li = 0; li < NGRAM_MAX; li++) {
			total += bucketsForLengthIndex(li);
		}
		return total;
	}

	/**
	 * 查询侧是否对 {@link cn.lxdb.plugins.muqingyu.fptoken.dataset.block.FpGroupHotNgramBitIndex#BUCKET_HASH_COUNT}
	 * 路桶位图做 AND。旧段仅写过 {@link cn.lxdb.plugins.muqingyu.fptoken.dataset.block.FpGroupHotNgramBitIndex#bucketIndex1}
	 * 时置 false，否则 bitHit 恒为 0。
	 */
	public static boolean BITSET_MULTI_HASH_AND = true;

	public static final short DEFAULT_INDEX_ID = 0;

	public static boolean PRINT_DEBUG = false;

	/** 写段时检测 writefp 是否仍按最终 term BytesRef 严格升序；乱序时打 WARN。 */
	public static boolean CHECK_TERM_WRITE_ORDER = true;

	/**
	 * 热词重建诊断：每组合并后打一行 {@code fp_ngram_diag}（便于 grep）；键表异常时另打 {@code fp_ngram_diag_KEY_SUSPECT}。
	 */
	public static boolean LOG_FP_NGRAM_DIAG = true;

	/** 位图类字段默认滑窗宽度（字节）。 */
	public static final int BITSET_WINDOW_SIZE = 32;

	/** 位图类字段默认滑窗步进（字节）。 */
	public static final int BITSET_STEP_SIZE = BITSET_WINDOW_SIZE-NGRAM_MAX;

}
