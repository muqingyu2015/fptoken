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




//	/** 2~{@link #NGRAM_MAX} 字节 ngram 的桶数（须为 2 的幂）。 */
//	public static final int BUCKETS = 512;

//	/** 1 字节 ngram 仅 256 种取值，单独桶行宽度，避免与 {@link #BUCKETS} 对齐浪费。 */
//	public static final int BUCKETS_LEN1 = 256;
//
//	/** {@code choose[][]} 第二维上界（≥ 各行 {@link #bucketsForLengthIndex(int)} 最大值）。 */
//	public static int maxBucketsPerRow() {
//		return BUCKETS;
//	}
//
//	/** ngram 字节长度 {@code 1..NGRAM_MAX} 对应的桶数。 */
//	public static int bucketsForNgramLen(int ngramLen) {
//		return  BUCKETS;//ngramLen <= 1 ? BUCKETS_LEN1 :
//	}
//
//	/** 位图行下标 {@code 0..NGRAM_MAX-1}（对应 ngram 长度 {@code lenIdx+1}）。 */
//	public static int bucketsForLengthIndex(int lenIdx) {
//		return bucketsForNgramLen(lenIdx + 1);
//	}

//	/** 交错落盘时 {@code (lenIdx, bucket)} 的线性对序号。 */
//	public static int bankPairIndex(int lenIdx, int bucket) {
//		int pair = 0;
//		for (int i = 0; i < lenIdx; i++) {
//			pair += bucketsForLengthIndex(i);
//		}
//		return pair + bucket;
//	}
//
//	public static int totalBankPairs() {
//		int total = 0;
//		for (int li = 0; li < NGRAM_MAX; li++) {
//			total += bucketsForLengthIndex(li);
//		}
//		return total;
//	}

//	/**
//	 * 查询侧是否对 {@link cn.lxdb.plugins.muqingyu.fptoken.dataset.block.FpGroupHotNgramBitIndex#BUCKET_HASH_COUNT}
//	 * 路桶位图做 AND。旧段仅写过 {@link cn.lxdb.plugins.muqingyu.fptoken.dataset.block.FpGroupHotNgramBitIndex#bucketIndex1}
//	 * 时置 false，否则 bitHit 恒为 0。
//	 */
//	public static boolean BITSET_MULTI_HASH_AND = true;

	public static final short DEFAULT_INDEX_ID = 0;

	public static boolean PRINT_DEBUG = false;

	/**
	 * 查询链路 trace（默认 false，DEBUG 级输出；灌数据/生产保持关闭）。
	 * 写段摘要不受此开关影响，仍为 INFO。
	 */
	public static boolean LOG_FP_SEARCH = false;

//	/**
//	 * selective 未命中时是否用全量 {@code fpBits(..., null, null)} 继续查询（默认 false，避免掩盖 selective 读盘问题）。
//	 */
//	public static boolean SELECTIVE_FP_BITS_FALLBACK = false;
//
//	/**
//	 * selective 异常时额外全量读一次，<b>仅打对比日志、不改变查询结果</b>，用于区分
//	 * {@code selectiveIoBroken}（全量有 order、selective 无）与 {@code ngramAbsent}（全量也无）。
//	 */
//	public static boolean SELECTIVE_FP_BITS_DIAG_COMPARE = true;

	/** 写段时检测 writefp 是否仍按最终 term BytesRef 严格升序；乱序时打 WARN。 */
	public static boolean CHECK_TERM_WRITE_ORDER = false;

	/** 写段 flush INFO（{@code fp_rebuild}/{@code fp_original}/{@code fp_bitindex}）抽样率：每 N 次打 1 条；≤1 表示全量。 */
	public static int FLUSH_LOG_SAMPLE_RATE = 100;

	/** 写段 flush 耗时 ≥ 此毫秒数时必打 INFO，不受 {@link #FLUSH_LOG_SAMPLE_RATE} 影响。 */
	public static int FLUSH_LOG_SLOW_MS = 500;

	/** common 组内 term 数 ≥ 此值时打 {@code commonAccumWarn}（WARN，全量）。 */
	public static int COMMON_ACCUM_WARN_THRESHOLD = FpTokenBlockLevelPolicy.OVER_WRITE_TOP_CNT+2048;

	/** common 超阈值后，每再增此 many term 重复打一次诊断（直至 flush）。 */
	public static int COMMON_ACCUM_WARN_STEP = 10000;

	/** 是否启用 common 超量来源诊断（{@link cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpCommonAccumDiag}）。 */
	public static boolean LOG_COMMON_ACCUM_WARN = true;

//	/**
//	 * 热词重建诊断：每组合并后打一行 {@code fp_ngram_diag}（便于 grep）；键表异常时另打 {@code fp_ngram_diag_KEY_SUSPECT}。
//	 */
//	public static boolean LOG_FP_NGRAM_DIAG = true;

	/** 位图类字段默认滑窗宽度（字节）。 */
	public static final int BITSET_WINDOW_SIZE = 32;

	/** 位图类字段默认滑窗步进（字节）。 */
	public static final int BITSET_STEP_SIZE = BITSET_WINDOW_SIZE-NGRAM_MAX;

	/**
	 * common merge 统计 hot 命中次数时，单 term 窗口数上界：
	 * {@code NGRAM_MAX × NGRAM_MAX × BITSET_WINDOW_SIZE}（当前 1152）。
	 */
	public static int commonHotHitMaxWindows() {
		return NGRAM_MAX * NGRAM_MAX * BITSET_WINDOW_SIZE;
	}

	/** {@link #commonHotHitMaxWindows()} 内 hot 命中日志分档步长（每组命中次数）。 */
	public static final int COMMON_HOT_HIT_TIER_BAND_SIZE = 50;

}
