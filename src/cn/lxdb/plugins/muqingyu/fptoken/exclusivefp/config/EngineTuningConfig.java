package cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.config;

/**
 * 项目统一的默认参数与运行时调优参数配置。
 *
 * <p>设计原则:
 * <ul>
 *   <li>对外 API 默认参数(Facade)与内部挖掘器参数(Miner)集中管理,避免散落在不同类中。</li>
 *   <li>本类仅提供默认值与通用公式,不包含调用时的业务入参(如 minSupport/minItemsetSize)。</li>
 *   <li>若要调整整体性能行为,优先在本类修改并通过测试矩阵验证回归。</li>
 * </ul>
 */
public final class EngineTuningConfig {

    private EngineTuningConfig() {
    }

    // ===== Facade 默认参数(ExclusiveFrequentItemsetSelector) =====

    /** 默认最大项集长度(item 个数)。 */
    public static final int DEFAULT_MAX_ITEMSET_SIZE = 6;
    /** 默认候选项上限。 */
    public static final int DEFAULT_MAX_CANDIDATE_COUNT = 150_000;
    /** 默认参与 Beam 扩展的频繁 1-项上限。 */
    public static final int DEFAULT_MAX_FREQUENT_TERM_COUNT = 1200;
    /** 默认每个前缀每层最大扩展分支数。 */
    public static final int DEFAULT_MAX_BRANCHING_FACTOR = 12;
    /** Facade 层默认 Beam 宽度(用于自适应分段的基准档位)。 */
    public static final int FACADE_DEFAULT_BEAM_WIDTH = 32;
    /** 默认两阶段挑选中 1-opt 替换尝试上限。 */
    public static final int DEFAULT_MAX_SWAP_TRIALS = 100;
    /** 互斥挑选中"净收益"默认最小阈值(estimatedSaving - termCost*len)。 */
    public static final int DEFAULT_MIN_NET_GAIN = 0;
    /** 互斥挑选中每个 term 的粗略字典开销估计(字节)。 */
    public static final int PICKER_ESTIMATED_BYTES_PER_TERM = 2;

    // ===== Miner 内部运行参数(BeamFrequentItemsetMiner) =====

    /** 调用方传入非法 beamWidth 时使用的回退值。 */
    public static final int MINER_FALLBACK_BEAM_WIDTH = 64;
    /** depth > 2 时的 beam 收缩除数。 */
    public static final int BEAM_WIDTH_DIVISOR_1 = 2;
    /** depth > 4 时的 beam 收缩除数。 */
    public static final int BEAM_WIDTH_DIVISOR_2 = 4;
    /** BitSet 每个 word 的 bit 数。 */
    public static final int BITS_PER_WORD = 64;
    /** 临时 BitSet 的最小初始容量。 */
    public static final int MIN_BITSET_CAPACITY = 64;
    /** 超时检查间隔(循环步数)。 */
    public static final int TIMEOUT_CHECK_INTERVAL = 100;
    /** 结果列表初始容量上限,避免大参数触发过大初始分配。 */
    public static final int MAX_INITIAL_CANDIDATE_CAPACITY = 10_000;
    /** 候选堆门槛激活比例:达到上限该比例后才启用"乐观收益剪枝"。 */
    public static final double CANDIDATE_FLOOR_ACTIVATION_RATIO = 0.35d;
    /** 候选乐观收益衰减比例(越小越激进)。 */
    public static final double CANDIDATE_OPTIMISTIC_DECAY = 0.85d;

    // ===== 索引与检索融合策略参数 =====

    // ===== 采样参数 =====

    /** 采样比率：只使用文档总数的该比例做挖掘（近似），最后回算完整 doclist。 */
    public static final double DEFAULT_SAMPLE_RATIO = 0.30d;
    /** 最小采样文档数，避免太少时挖掘失效。 */
    public static final int DEFAULT_MIN_SAMPLE_COUNT = 64;
    /** 采样支持度缩放因子。0.0=自动按采样比例缩放（默认，推荐），1.0=不缩放。 */
    public static final double DEFAULT_SAMPLING_SUPPORT_SCALE = 0.0d;
    /** 采样总开关默认值。 */
    public static final boolean DEFAULT_SAMPLING_ENABLED = true;

    // ===== 行处理 API / Runner / Loader 默认参数 =====

    /** 默认 n-gram 起始长度。 */
    public static final int DEFAULT_NGRAM_START = 2;
    /** 默认 n-gram 结束长度。 */
    public static final int DEFAULT_NGRAM_END = 4;
    /** 默认热点词阈值（count > threshold 才进入 hot term）。 */
    public static final int DEFAULT_HOT_TERM_THRESHOLD_EXCLUSIVE = 12;
    /** runner 默认最小支持度。 */
    public static final int DEFAULT_RUNNER_MIN_SUPPORT = 50;
    /** runner 默认最小项集长度。 */
    public static final int DEFAULT_RUNNER_MIN_ITEMSET_SIZE = 2;

    /** skip-index 哈希层默认最小 n-gram。 */
    public static final int DEFAULT_SKIP_HASH_MIN_GRAM = 2;
    /** skip-index 哈希层默认最大 n-gram。 */
    public static final int DEFAULT_SKIP_HASH_MAX_GRAM = 4;

    /** 行加载每文件最大行数。 */
    public static final int DEFAULT_MAX_LINES_PER_FILE = 32000;
    /** 行加载每行最大字节数。 */
    public static final int DEFAULT_MAX_BYTES_PER_LINE = 64;

    /** 过滤"过热词"默认文档覆盖率上限（1.0 表示不过滤）。 */
    public static final double DEFAULT_MAX_DOC_COVERAGE_RATIO = 1.0d;
    /** 高频命中阈值(命中率 >= 该值倾向频繁项集路由)。 */
    public static final double LOOKUP_HIGH_HIT_RATE_THRESHOLD = 0.35d;
    /** 低频命中阈值(命中率 <= 该值倾向 skipping 路由)。 */
    public static final double LOOKUP_LOW_HIT_RATE_THRESHOLD = 0.05d;

    /**
     * 默认评分函数公式:长度和支持度都高的前缀优先。
     *
     * @param length 项集长度
     * @param support 支持度
     * @return 分数
     */
    public static int defaultScore(int length, int support) {
        return (length - 1) * support;
    }
}
