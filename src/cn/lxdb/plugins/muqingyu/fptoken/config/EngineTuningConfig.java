package cn.lxdb.plugins.muqingyu.fptoken.config;

/**
 * 项目统一的默认参数与运行时调优参数配置。
 *
 * <p>设计原则：
 * <ul>
 *   <li>对外 API 默认参数（Facade）与内部挖掘器参数（Miner）集中管理，避免散落在不同类中。</li>
 *   <li>本类仅提供默认值与通用公式，不包含调用时的业务入参（如 minSupport/minItemsetSize）。</li>
 *   <li>若要调整整体性能行为，优先在本类修改并通过测试矩阵验证回归。</li>
 * </ul>
 */
public final class EngineTuningConfig {

    private EngineTuningConfig() {
    }

    // ===== Facade 默认参数（ExclusiveFrequentItemsetSelector） =====

    /** 默认最大项集长度（item 个数）。 */
    public static final int DEFAULT_MAX_ITEMSET_SIZE = 6;
    /** 默认候选项上限。 */
    public static final int DEFAULT_MAX_CANDIDATE_COUNT = 200_000;
    /** 默认参与 Beam 扩展的频繁 1-项上限。 */
    public static final int DEFAULT_MAX_FREQUENT_TERM_COUNT = 1500;
    /** 默认每个前缀每层最大扩展分支数。 */
    public static final int DEFAULT_MAX_BRANCHING_FACTOR = 16;
    /** Facade 层默认 Beam 宽度（用于自适应分段的基准档位）。 */
    public static final int FACADE_DEFAULT_BEAM_WIDTH = 32;
    /** 默认两阶段挑选中 1-opt 替换尝试上限。 */
    public static final int DEFAULT_MAX_SWAP_TRIALS = 100;
    /** 互斥挑选中“净收益”默认最小阈值（estimatedSaving - termCost*len）。 */
    public static final int DEFAULT_MIN_NET_GAIN = 8;
    /** 互斥挑选中每个 term 的粗略字典开销估计（字节）。 */
    public static final int PICKER_ESTIMATED_BYTES_PER_TERM = 2;

    // ===== Miner 内部运行参数（BeamFrequentItemsetMiner） =====

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
    /** 超时检查间隔（循环步数）。 */
    public static final int TIMEOUT_CHECK_INTERVAL = 100;
    /** 结果列表初始容量上限，避免大参数触发过大初始分配。 */
    public static final int MAX_INITIAL_CANDIDATE_CAPACITY = 10_000;
    /** 候选堆门槛激活比例：达到上限该比例后才启用“乐观收益剪枝”。 */
    public static final double CANDIDATE_FLOOR_ACTIVATION_RATIO = 0.35d;
    /** 候选乐观收益衰减比例（越小越激进）。 */
    public static final double CANDIDATE_OPTIMISTIC_DECAY = 0.85d;

    // ===== 索引与检索融合策略参数 =====

    /** 过滤“过热词”默认文档覆盖率上限（1.0 表示不过滤）。 */
    public static final double DEFAULT_MAX_DOC_COVERAGE_RATIO = 0.985d;
    /** 高频命中阈值（命中率 >= 该值倾向频繁项集路由）。 */
    public static final double LOOKUP_HIGH_HIT_RATE_THRESHOLD = 0.35d;
    /** 低频命中阈值（命中率 <= 该值倾向 skipping 路由）。 */
    public static final double LOOKUP_LOW_HIT_RATE_THRESHOLD = 0.05d;

    /**
     * 默认评分函数公式：长度和支持度都高的前缀优先。
     *
     * @param length 项集长度
     * @param support 支持度
     * @return 分数
     */
    public static int defaultScore(int length, int support) {
        return (length - 1) * support;
    }
}
