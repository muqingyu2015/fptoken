package cn.lxdb.plugins.muqingyu.fptoken;

import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.config.SelectorConfig;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.config.EngineTuningConfig;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.index.TermTidsetIndex;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.miner.BeamFrequentItemsetMiner;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.CandidateItemset;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.ByteRef;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.DocTerms;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.ExclusiveSelectionResult;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.FrequentItemsetMiningResult;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.SelectedGroup;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.hints.PremergeHintCandidateResolver;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.picker.TwoPhaseExclusiveItemsetPicker;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.protocol.ModuleDataContracts;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.util.ByteArrayUtils;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.util.IntOpenAddressingSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * 互斥频繁项集选择的门面(Facade):对调用方屏蔽索引、挖掘与挑选的实现细节。
 *
 * <p><b>处理流程</b>(固定单一路径,便于线上行为一致、排障简单):
 * <ol>
 *   <li>将 {@link DocTerms} 列表建成垂直索引 {@link TermTidsetIndex}(词 → 文档位图)。</li>
 *   <li>在 tidset 上做 {@link BeamFrequentItemsetMiner} 近似挖掘,得到 {@link CandidateItemset} 列表及统计。</li>
 *   <li>用 {@link TwoPhaseExclusiveItemsetPicker} 做词级互斥选择(贪心初解 + 预算内 1-opt 替换)。</li>
 *   <li>将 termId 还原为词字节、doc 位图转为 docId 列表,封装为 {@link SelectedGroup}。</li>
 * </ol>
 *
 * <p><b>输入与索引约定</b>:
 * <ul>
 *   <li>{@link TermTidsetIndex} 中 {@link BitSet} 的下标与 {@link DocTerms#getDocId()} 一致;因此项集支持度 = 位图基数,
 *       交集即频繁性判定。</li>
 *   <li>若 {@code rows} 为 null 或空,或建索引后无任何词,则返回空 {@link ExclusiveSelectionResult#getGroups()},
 *       统计字段按「无挖掘」语义置零({@code maxCandidateCount} 仍为本次调用入参,便于日志对齐)。</li>
 * </ul>
 *
 * <p><b>默认性能参数</b>:见 {@link EngineTuningConfig}。下列取值面向典型流量索引场景:<b>每批约 1 万条 pcap 记录</b>、单条约 1024 字节、<b>128 字节滑动窗口</b>,
 * 窗口内以 <b>1/2/3 字节</b> 为 item 构造 {@link DocTerms},再在本管道中做互斥频繁项集挖掘,供检索与压缩使用。
 * 若批次规模、步长或词表构造方式不同,请相应调大/调小常量或与 SLA 一并评估。
 *
 * <p><b>调用方显式参数建议</b>(无法由本类代为默认,因与批次强相关):
 * <ul>
 *   <li>{@code minSupport}:按「窗口」条数(即 {@code rows.size()})设绝对支持度。文档量约 <i>包数 × 每包窗口数</i> 时,可从窗口数的 <b>0.05%~1%</b> 试起,
 *       再按误报/漏报调;极稀疏模式可提高下限,极稠密可降低。</li>
 *   <li>{@code minItemsetSize}:压缩与短语质量通常从 <b>2</b> 起(跳过单 item 项集);若只要更长模板可提到 3。</li>
 * </ul>
 *
 * <p><b>避免误用</b>：</p>
 * <ul>
 *   <li>默认采样/挑选参数可走静态 setter；若需并发隔离，请优先用
 *       {@link SelectionRequest.Builder#executionTuning(ExecutionTuning)} 传入请求级调参。</li>
 *   <li>请求级调参为不可变对象，适合多线程/多租户并行处理不同分片。</li>
 *   <li>{@code rows} 中 {@code docId} 必须可作为位图下标使用（建议非负且可控范围内）。</li>
 * </ul>
 *
 * @author muqingyu
 */
public final class ExclusiveFrequentItemsetSelector {
    private static final Logger LOG = Logger.getLogger(ExclusiveFrequentItemsetSelector.class.getName());
    private static final String DEBUG_LOG_PROPERTY = "fptoken.selector.debugLog";
    private static final String PROP_SAMPLE_RATIO = "fptoken.selector.sampleRatio";
    private static final String PROP_MIN_SAMPLE_COUNT = "fptoken.selector.minSampleCount";
    private static final String PROP_SAMPLING_SUPPORT_SCALE = "fptoken.selector.samplingSupportScale";
    private static final String PROP_TESTING_FAIL_SAMPLING_PATH = "fptoken.selector.testing.failSamplingPath";
    private static final String ENV_SAMPLE_RATIO = "FPTOKEN_SELECTOR_SAMPLE_RATIO";
    private static final String ENV_MIN_SAMPLE_COUNT = "FPTOKEN_SELECTOR_MIN_SAMPLE_COUNT";
    private static final String ENV_SAMPLING_SUPPORT_SCALE = "FPTOKEN_SELECTOR_SAMPLING_SUPPORT_SCALE";

    public enum LookupRoute {
        FREQUENT_ITEMSET,
        DATA_SKIPPING,
        HYBRID
    }

    private ExclusiveFrequentItemsetSelector() {
    }

    /**
     * 使用 {@link EngineTuningConfig#DEFAULT_MAX_ITEMSET_SIZE}、
     * {@link EngineTuningConfig#DEFAULT_MAX_CANDIDATE_COUNT},仅返回互斥词组列表。
     *
     * @param rows 文档及词列表;见类注释「输入与索引约定」
     * @param minSupport 最小支持度(文档数),须为正整数
     * @param minItemsetSize 输出项集最小长度(词个数),须为正整数
     * @return 互斥条件下的 {@link SelectedGroup} 列表,可能为空
     */
    public static List<SelectedGroup> selectExclusiveBestItemsets(
            List<DocTerms> rows,
            int minSupport,
            int minItemsetSize
    ) {
        return selectExclusiveBestItemsetsWithStats(
                rows,
                minSupport,
                minItemsetSize,
                EngineTuningConfig.DEFAULT_MAX_ITEMSET_SIZE,
                EngineTuningConfig.DEFAULT_MAX_CANDIDATE_COUNT
        ).getGroups();
    }

    /**
     * 可读性更友好的别名入口:语义与 {@link #selectExclusiveBestItemsets(List, int, int)} 完全一致。
     *
     * <p>保留原有 API 以兼容既有调用方;新调用方可优先使用此名称。
     */
    public static List<SelectedGroup> findMutuallyExclusivePatterns(
            List<DocTerms> inputDocuments,
            int minimumRequiredSupport,
            int minimumPatternLength
    ) {
        return selectExclusiveBestItemsets(inputDocuments, minimumRequiredSupport, minimumPatternLength);
    }

    /**
     * 指定最大项集长度与候选上限,仅返回互斥词组列表。
     *
     * @param rows 文档及词列表
     * @param minSupport 最小支持度
     * @param minItemsetSize 最小项集长度
     * @param maxItemsetSize 最大项集长度(与最小长度等一并由 {@link SelectorConfig} 校验)
     * @param maxCandidateCount 候选项数量上限
     * @return 互斥条件下的 {@link SelectedGroup} 列表,可能为空
     */
    public static List<SelectedGroup> selectExclusiveBestItemsets(
            List<DocTerms> rows,
            int minSupport,
            int minItemsetSize,
            int maxItemsetSize,
            int maxCandidateCount
    ) {
        return selectExclusiveBestItemsetsWithStats(rows, minSupport, minItemsetSize, maxItemsetSize, maxCandidateCount)
                .getGroups();
    }

    /**
     * 可读性更友好的别名入口:语义与
     * {@link #selectExclusiveBestItemsets(List, int, int, int, int)} 完全一致。
     */
    public static List<SelectedGroup> findMutuallyExclusivePatterns(
            List<DocTerms> inputDocuments,
            int minimumRequiredSupport,
            int minimumPatternLength,
            int maximumPatternLength,
            int maximumIntermediateResults
    ) {
        return selectExclusiveBestItemsets(
                inputDocuments,
                minimumRequiredSupport,
                minimumPatternLength,
                maximumPatternLength,
                maximumIntermediateResults
        );
    }

    /**
     * 使用默认 {@code maxItemsetSize}、{@code maxCandidateCount},返回词组与完整统计。
     *
     * @param rows 文档及词列表
     * @param minSupport 最小支持度
     * @param minItemsetSize 最小项集长度
     * <p><b>前置条件</b>：{@code minSupport >= 1}、{@code minItemsetSize >= 1}。
     * 当 {@code rows == null || rows.isEmpty()} 时返回空结果（兼容历史语义）。</p>
     * @return 结果与统计;统计含义见 {@link ExclusiveSelectionResult}
     */
    public static ExclusiveSelectionResult selectExclusiveBestItemsetsWithStats(
            List<DocTerms> rows,
            int minSupport,
            int minItemsetSize
    ) {
        return selectExclusiveBestItemsetsWithStats(
                rows,
                minSupport,
                minItemsetSize,
                EngineTuningConfig.DEFAULT_MAX_ITEMSET_SIZE,
                EngineTuningConfig.DEFAULT_MAX_CANDIDATE_COUNT
        );
    }

    /**
     * 完整流程入口:索引 → 挖掘 → 互斥挑选 → 组装对外结果。
     *
     * @param rows 文档及词列表;null 或 empty 时返回空组与零统计({@code maxCandidateCount} 仍为入参)
     * @param minSupport 最小支持度
     * @param minItemsetSize 最小项集长度
     * @param maxItemsetSize 最大项集长度
     * @param maxCandidateCount 候选项上限
     * @return {@link ExclusiveSelectionResult},含 {@link SelectedGroup} 与挖掘阶段计数
     */
    // ===== 采样参数（默认从 EngineTuningConfig 读取，可通过 setter 覆盖） =====

    private static volatile RuntimeTuningSnapshot runtimeTuningSnapshot = loadRuntimeTuningSnapshotFromExternal();
    /**
     * 兼容保留：当前实现固定走采样路径，本方法不再切换执行分支。
     *
     * @param enabled 历史参数（忽略）
     */
    public static void setSamplingEnabled(boolean enabled) {
        // no-op: sampling-only pipeline.
    }

    /**
     * 设置采样比率。
     * @param ratio 0.0~1.0，默认 {@link EngineTuningConfig#DEFAULT_SAMPLE_RATIO}
     *              小于 0 按 0 处理，大于 1 按 1 处理（内部自动 clamp）。
     */
    public static void setSampleRatio(double ratio) {
        RuntimeTuningSnapshot current = runtimeTuningSnapshot;
        runtimeTuningSnapshot = normalizeRuntimeTuningSnapshot(
                ratio,
                current.minSampleCount,
                current.samplingSupportScale,
                current.pickerMinNetGain,
                current.pickerEstimatedBytesPerTerm,
                current.pickerCoverageRewardPerTerm,
                current.maxDocCoverageRatio,
                current.pickerScoringWeights
        );
    }

    /**
     * 设置最小采样文档数。
     * @param count 正整数，默认 {@link EngineTuningConfig#DEFAULT_MIN_SAMPLE_COUNT}
     */
    public static void setMinSampleCount(int count) {
        RuntimeTuningSnapshot current = runtimeTuningSnapshot;
        runtimeTuningSnapshot = normalizeRuntimeTuningSnapshot(
                current.sampleRatio,
                count,
                current.samplingSupportScale,
                current.pickerMinNetGain,
                current.pickerEstimatedBytesPerTerm,
                current.pickerCoverageRewardPerTerm,
                current.maxDocCoverageRatio,
                current.pickerScoringWeights
        );
    }

    /**
     * 设置采样支持度缩放因子。
     * @param scale 0.0=自动按采样比例缩放（默认），1.0=不缩放
     *              其他值表示显式缩放系数（例如 0.5 表示支持度减半）。
     */
    public static void setSamplingSupportScale(double scale) {
        RuntimeTuningSnapshot current = runtimeTuningSnapshot;
        runtimeTuningSnapshot = normalizeRuntimeTuningSnapshot(
                current.sampleRatio,
                current.minSampleCount,
                scale,
                current.pickerMinNetGain,
                current.pickerEstimatedBytesPerTerm,
                current.pickerCoverageRewardPerTerm,
                current.maxDocCoverageRatio,
                current.pickerScoringWeights
        );
    }

    /**
     * 重新加载外部化采样配置（系统属性 / 环境变量）。
     *
     * <p>优先级：System Property > Environment Variable > 当前代码内值。</p>
     *
     * <p>支持键：
     * {@code fptoken.selector.sampleRatio} /
     * {@code fptoken.selector.minSampleCount} /
     * {@code fptoken.selector.samplingSupportScale}。</p>
     */
    public static synchronized void reloadSamplingConfig() {
        runtimeTuningSnapshot = applyExternalSamplingOverrides(runtimeTuningSnapshot);
    }

    /** 当前采样比率（便于测试与运行期观测）。 */
    public static double getSampleRatio() {
        return runtimeTuningSnapshot.sampleRatio;
    }

    /** 当前最小采样文档数（便于测试与运行期观测）。 */
    public static int getMinSampleCount() {
        return runtimeTuningSnapshot.minSampleCount;
    }

    /** 当前采样支持度缩放因子（便于测试与运行期观测）。 */
    public static double getSamplingSupportScale() {
        return runtimeTuningSnapshot.samplingSupportScale;
    }

    /** 当前互斥挑选净收益阈值。 */
    public static int getPickerMinNetGain() {
        return runtimeTuningSnapshot.pickerMinNetGain;
    }

    /** 设置互斥挑选净收益阈值。 */
    public static void setPickerMinNetGain(int minNetGain) {
        RuntimeTuningSnapshot current = runtimeTuningSnapshot;
        runtimeTuningSnapshot = normalizeRuntimeTuningSnapshot(
                current.sampleRatio,
                current.minSampleCount,
                current.samplingSupportScale,
                minNetGain,
                current.pickerEstimatedBytesPerTerm,
                current.pickerCoverageRewardPerTerm,
                current.maxDocCoverageRatio,
                current.pickerScoringWeights
        );
    }

    /** 当前互斥挑选的每 term 字典成本估计（字节）。 */
    public static int getPickerEstimatedBytesPerTerm() {
        return runtimeTuningSnapshot.pickerEstimatedBytesPerTerm;
    }

    /** 设置互斥挑选的每 term 字典成本估计（字节）。 */
    public static void setPickerEstimatedBytesPerTerm(int estimatedBytesPerTerm) {
        RuntimeTuningSnapshot current = runtimeTuningSnapshot;
        runtimeTuningSnapshot = normalizeRuntimeTuningSnapshot(
                current.sampleRatio,
                current.minSampleCount,
                current.samplingSupportScale,
                current.pickerMinNetGain,
                estimatedBytesPerTerm,
                current.pickerCoverageRewardPerTerm,
                current.maxDocCoverageRatio,
                current.pickerScoringWeights
        );
    }

    /** 当前互斥挑选每 term 覆盖奖励。 */
    public static int getPickerCoverageRewardPerTerm() {
        return runtimeTuningSnapshot.pickerCoverageRewardPerTerm;
    }

    /** 设置互斥挑选每 term 覆盖奖励。 */
    public static void setPickerCoverageRewardPerTerm(int coverageRewardPerTerm) {
        RuntimeTuningSnapshot current = runtimeTuningSnapshot;
        runtimeTuningSnapshot = normalizeRuntimeTuningSnapshot(
                current.sampleRatio,
                current.minSampleCount,
                current.samplingSupportScale,
                current.pickerMinNetGain,
                current.pickerEstimatedBytesPerTerm,
                coverageRewardPerTerm,
                current.maxDocCoverageRatio,
                current.pickerScoringWeights
        );
    }

    /** 获取互斥挑选评分权重（长度/支持度/业务价值/成本等）。 */
    public static TwoPhaseExclusiveItemsetPicker.ScoringWeights getPickerScoringWeights() {
        return TwoPhaseExclusiveItemsetPicker.getRuntimeScoringWeights();
    }

    /** 设置互斥挑选评分权重（运行时动态生效）。 */
    public static void setPickerScoringWeights(TwoPhaseExclusiveItemsetPicker.ScoringWeights scoringWeights) {
        TwoPhaseExclusiveItemsetPicker.setRuntimeScoringWeights(scoringWeights);
    }

    /** 重新加载互斥挑选评分权重（System Property / Environment Variable）。 */
    public static void reloadPickerScoringWeights() {
        TwoPhaseExclusiveItemsetPicker.reloadRuntimeScoringWeights();
    }

    /** 重置互斥挑选评分权重为默认值。 */
    public static void resetPickerScoringWeightsToDefaults() {
        TwoPhaseExclusiveItemsetPicker.resetRuntimeScoringWeightsToDefaults();
    }

    /**
     * 按抽样比例计算采样阶段使用的最小支持度。
     *
     * <p>规则：
     * <ul>
     *   <li>当 {@code supportScale <= 0} 时，自动按 {@code sampledDocCount / fullDocCount} 等比例缩放。</li>
     *   <li>当 {@code supportScale > 0} 时，使用调用方给定的显式缩放系数。</li>
     *   <li>结果最小为 1。</li>
     * </ul>
     */
    public static int computeSampledMinSupport(
            int fullMinSupport,
            int fullDocCount,
            int sampledDocCount,
            double supportScale
    ) {
        if (fullMinSupport <= 0) {
            throw new IllegalArgumentException("fullMinSupport must be > 0");
        }
        if (fullDocCount <= 0) {
            throw new IllegalArgumentException("fullDocCount must be > 0");
        }
        if (sampledDocCount <= 0 || sampledDocCount > fullDocCount) {
            throw new IllegalArgumentException("sampledDocCount must be in [1, fullDocCount]");
        }
        double autoRatio = (double) sampledDocCount / (double) fullDocCount;
        double effectiveScale = supportScale <= 0.0d
                ? autoScaleWithConfidenceUpperBound(
                        autoRatio,
                        fullDocCount,
                        EngineTuningConfig.DEFAULT_SAMPLING_CONFIDENCE_Z
                )
                : supportScale;
        return Math.max(1, (int) Math.round(fullMinSupport * effectiveScale));
    }

    /**
     * 自动采样缩放：在比例缩放基础上加入置信上界边际（正态近似）。
     *
     * <p>scale = r + z * sqrt(r(1-r)/N)，其中：
     * r=sampled/full，N=fullDocCount，z 为置信系数。</p>
     */
    private static double autoScaleWithConfidenceUpperBound(double ratio, int fullDocCount, double z) {
        if (fullDocCount <= 0) {
            return ratio;
        }
        double variance = ratio * (1.0d - ratio) / (double) fullDocCount;
        if (variance <= 0.0d) {
            return ratio;
        }
        double margin = z * Math.sqrt(variance);
        return Math.min(1.0d, ratio + margin);
    }

    /**
     * 计算本次抽样目标样本量。
     *
     * <p>规则：按 {@code ceil(docCount * sampleRatio)} 先算比例样本，再与 {@code minSampleCount} 取较大值，
     * 最后不超过 {@code docCount}。</p>
     */
    public static int computeTargetSampleSize(int docCount, double sampleRatio, int minSampleCount) {
        if (docCount <= 0) {
            throw new IllegalArgumentException("docCount must be > 0");
        }
        double clampedRatio = Math.max(0.0d, Math.min(1.0d, sampleRatio));
        int safeMinSampleCount = Math.max(1, minSampleCount);
        int byRatio = (int) Math.ceil(docCount * clampedRatio);
        return Math.min(docCount, Math.max(safeMinSampleCount, byRatio));
    }

    public static ExclusiveSelectionResult selectExclusiveBestItemsetsWithStats(
            List<DocTerms> rows,
            int minSupport,
            int minItemsetSize,
            int maxItemsetSize,
            int maxCandidateCount
    ) {
        // 兼容历史语义：legacy 多参数入口对 null rows 返回空结果而非抛异常。
        if (rows == null) {
            return emptyResult(maxCandidateCount);
        }
        return selectExclusiveBestItemsetsWithStats(
                SelectionRequest.builder(rows)
                        .minSupport(minSupport)
                        .minItemsetSize(minItemsetSize)
                        .maxItemsetSize(maxItemsetSize)
                        .maxCandidateCount(maxCandidateCount)
                        .build());
    }

    /**
     * merge 场景扩展入口：可传入前置高频提示候选，提示会在本轮全量数据上二次回算后再参与互斥挑选。
     *
     * <p><b>前置条件</b>：{@code minSupport >= 1}、{@code minItemsetSize >= 1}、
     * {@code maxItemsetSize >= minItemsetSize}、{@code maxCandidateCount >= 1}、
     * {@code hintBoostWeight >= 0}。
     * 当 {@code rows == null || rows.isEmpty()} 时返回空结果（兼容历史语义）。</p>
     */
    public static ExclusiveSelectionResult selectExclusiveBestItemsetsWithStats(
            List<DocTerms> rows,
            int minSupport,
            int minItemsetSize,
            int maxItemsetSize,
            int maxCandidateCount,
            List<PremergeHintCandidate> premergeHints,
            int hintBoostWeight
    ) {
        // 兼容历史语义：legacy 多参数入口对 null rows 返回空结果而非抛异常。
        if (rows == null) {
            return emptyResult(maxCandidateCount);
        }
        return selectExclusiveBestItemsetsWithStats(
                SelectionRequest.builder(rows)
                        .minSupport(minSupport)
                        .minItemsetSize(minItemsetSize)
                        .maxItemsetSize(maxItemsetSize)
                        .maxCandidateCount(maxCandidateCount)
                        .premergeHints(premergeHints)
                        .hintBoostWeight(hintBoostWeight)
                        .build());
    }

    /**
     * 配置对象入口：避免参数爆炸，统一选择流程入口。
     *
     * <p><b>前置条件</b>：{@code request != null} 且 {@code request.rows != null}。
     * 其余参数约束通过 {@link SelectorConfig} 统一校验（最小支持度、项集长度、候选上限）。</p>
     */
    public static ExclusiveSelectionResult selectExclusiveBestItemsetsWithStats(SelectionRequest request) {
        validateSelectionRequestPreconditions(request);
        boolean debugLog = isDebugLogEnabled();
        long totalStartNs = System.nanoTime();
        RuntimeTuningSnapshot tuning = resolveTuningSnapshot(request);
        if (request.rows.isEmpty()) {
            return emptyResult(request.maxCandidateCount);
        }

        // 参数统一走 SelectorConfig 校验，确保异常语义与历史一致。
        SelectorConfig.of(
                request.minSupport,
                request.minItemsetSize,
                request.maxItemsetSize,
                request.maxCandidateCount
        );

        int[] allDocIds = collectDistinctDocIds(request.rows);
        int docCount = allDocIds.length;
        int sampleSize = computeTargetSampleSize(docCount, tuning.sampleRatio, tuning.minSampleCount);
        if (debugLog) {
            LOG.log(Level.INFO,
                    "[fptoken-selector] start docs={0}, minSupport={1}, minItemsetSize={2}, maxItemsetSize={3}, maxCandidateCount={4}, sampleRatio={5}, minSampleCount={6}, targetSampleSize={7}, premergeHints={8}, hintBoostWeight={9}",
                    new Object[] {
                            docCount,
                            request.minSupport,
                            request.minItemsetSize,
                            request.maxItemsetSize,
                            request.maxCandidateCount,
                            tuning.sampleRatio,
                            tuning.minSampleCount,
                            sampleSize,
                            request.premergeHints.size(),
                            request.hintBoostWeight
                    });
        }

        // Phase 1: 在全量数据上建索引
        // 使用 buildWithSupportBounds 过滤低频词，显著降低词汇量和内存
        // 当不需要过滤时（minSupport=1），退化到 build() 单次扫描
        long indexStartNs = System.nanoTime();
        TermTidsetIndex fullIndex = TermTidsetIndex.buildWithSupportBounds(
                request.rows, request.minSupport, tuning.maxDocCoverageRatio);
        long indexBuildMs = nanosToMillis(System.nanoTime() - indexStartNs);
        List<ByteRef> termVocabulary = fullIndex.getIdToTermRefsUnsafe();
        if (termVocabulary.isEmpty()) {
            return emptyResult(request.maxCandidateCount);
        }
        List<BitSet> fullTidsets = fullIndex.getTidsetsByTermIdUnsafe();
        if (debugLog) {
            LOG.log(Level.INFO,
                    "[fptoken-selector] full-index ready vocab={0}, filteredBySupportBounds=true",
                    new Object[] { termVocabulary.size() });
        }

        // Phase 2: 构造挖掘输入（采样源或全量源）
        long miningInputStartNs = System.nanoTime();
        MiningInputBuildOutcome miningInputOutcome = buildMiningInputWithFallback(
                allDocIds,
                fullTidsets,
                docCount,
                sampleSize,
                request.minSupport,
                tuning.samplingSupportScale,
                debugLog
        );
        long miningInputBuildMs = nanosToMillis(System.nanoTime() - miningInputStartNs);
        MiningInput miningInput = miningInputOutcome.miningInput;
        if (debugLog) {
            LOG.log(Level.INFO,
                    "[fptoken-selector] mining-input mode={0}, miningDocCount={1}, miningMinSupport={2}",
                    new Object[] {
                            miningInput.usesSampling ? "sampled" : "full",
                            miningInput.miningDocCount,
                            miningInput.miningMinSupport
                    });
        }
        // Phase 3: 在统一的数据源抽象上挖掘（核心算法不感知采样/非采样）
        SelectorConfig miningConfig = SelectorConfig.of(
                miningInput.miningMinSupport,
                request.minItemsetSize,
                request.maxItemsetSize,
                request.maxCandidateCount);
        MiningPlan miningPlan = buildMiningPlan(miningConfig, miningInput.miningDocCount, termVocabulary.size());
        long miningStartNs = System.nanoTime();
        boolean sampledMiningFallbackToFull = false;
        FrequentItemsetMiningResult miningStats;
        try {
            miningStats = mineCandidates(
                    new ModuleDataContracts.TidsetMiningInput(miningInput.miningTidsets),
                    miningPlan
            );
        } catch (RuntimeException samplingFailure) {
            if (!miningInput.usesSampling) {
                throw samplingFailure;
            }
            if (debugLog) {
                LOG.log(Level.WARNING,
                        "[fptoken-selector] sampled-mining failed, fallback to full-mining. reason={0}",
                        new Object[] { samplingFailure.toString() });
            }
            miningInput = new MiningInput(fullTidsets, docCount, request.minSupport, false);
            SelectorConfig fullConfig = SelectorConfig.of(
                    request.minSupport,
                    request.minItemsetSize,
                    request.maxItemsetSize,
                    request.maxCandidateCount
            );
            MiningPlan fullPlan = buildMiningPlan(fullConfig, docCount, termVocabulary.size());
            miningStats = mineCandidates(
                    new ModuleDataContracts.TidsetMiningInput(miningInput.miningTidsets),
                    fullPlan
            );
            sampledMiningFallbackToFull = true;
        }
        long miningMs = nanosToMillis(System.nanoTime() - miningStartNs);
        List<CandidateItemset> minedCandidates = miningStats.getCandidates();
        if (debugLog) {
            LOG.log(Level.INFO,
                    "[fptoken-selector] mining done generatedCandidates={0}, intersections={1}, truncatedByCandidateLimit={2}, minedCandidates={3}",
                    new Object[] {
                            miningStats.getGeneratedCandidateCount(),
                            miningStats.getIntersectionCount(),
                            miningStats.isTruncatedByCandidateLimit(),
                            minedCandidates.size()
                    });
        }
        long hintMergeStartNs = System.nanoTime();
        List<CandidateItemset> hintCandidates = PremergeHintCandidateResolver.mapHintCandidatesToTermIds(
                request.premergeHints,
                termVocabulary
        );
        List<CandidateItemset> mergedCandidates = PremergeHintCandidateResolver.mergeAndDedupCandidates(
                minedCandidates,
                hintCandidates
        );
        long hintMergeMs = nanosToMillis(System.nanoTime() - hintMergeStartNs);
        if (debugLog) {
            LOG.log(Level.INFO,
                    "[fptoken-selector] candidate-merge mined={0}, hintMapped={1}, mergedDistinct={2}",
                    new Object[] { minedCandidates.size(), hintCandidates.size(), mergedCandidates.size() });
        }
        if (mergedCandidates.isEmpty()) {
            long totalMs = nanosToMillis(System.nanoTime() - totalStartNs);
            return buildResult(
                    Collections.<SelectedGroup>emptyList(),
                    miningStats,
                    request.maxCandidateCount,
                    buildDiagnostics(
                            docCount,
                            sampleSize,
                            miningInput,
                            miningInputOutcome.sampledInputBuildFallbackToFull,
                            sampledMiningFallbackToFull,
                            indexBuildMs,
                            miningInputBuildMs,
                            miningMs,
                            hintMergeMs,
                            0L,
                            0L,
                            totalMs,
                            request.premergeHints.size(),
                            hintCandidates.size(),
                            mergedCandidates.size()
                    )
            );
        }
        // 6) 回算：在全量 tidsets 上重新计算候选组合的完整 doclist
        //    挖掘阶段得到的 termIds 就是全量索引的 termId，无需映射
        if (debugLog) {
            LOG.log(Level.INFO,
                    "[fptoken-selector] recompute-start mergedCandidates={0}, fullMinSupport={1}",
                    new Object[] { mergedCandidates.size(), request.minSupport });
        }
        long recomputeStartNs = System.nanoTime();
        List<CandidateItemset> recomputed = recomputeOnFullData(
                mergedCandidates, fullTidsets, request.minSupport);
        if (!hintCandidates.isEmpty() && request.hintBoostWeight > 0) {
            recomputed = PremergeHintCandidateResolver.applyHintBoost(
                    recomputed,
                    hintCandidates,
                    request.hintBoostWeight
            );
            if (debugLog) {
                LOG.log(Level.INFO,
                        "[fptoken-selector] hint-boost applied hintCandidates={0}, boostWeight={1}, recomputedAfterBoost={2}",
                        new Object[] { hintCandidates.size(), request.hintBoostWeight, recomputed.size() });
            }
        }
        long recomputeMs = nanosToMillis(System.nanoTime() - recomputeStartNs);
        if (recomputed.isEmpty()) {
            long totalMs = nanosToMillis(System.nanoTime() - totalStartNs);
            return buildResult(
                    Collections.<SelectedGroup>emptyList(),
                    miningStats,
                    request.maxCandidateCount,
                    buildDiagnostics(
                            docCount,
                            sampleSize,
                            miningInput,
                            miningInputOutcome.sampledInputBuildFallbackToFull,
                            sampledMiningFallbackToFull,
                            indexBuildMs,
                            miningInputBuildMs,
                            miningMs,
                            hintMergeMs,
                            recomputeMs,
                            0L,
                            totalMs,
                            request.premergeHints.size(),
                            hintCandidates.size(),
                            mergedCandidates.size()
                    )
            );
        }
        // 7) 互斥挑选
        long pickStartNs = System.nanoTime();
        List<CandidateItemset> selectedCandidates = pickExclusiveCandidates(
                recomputed,
                termVocabulary.size(),
                tuning.pickerMinNetGain,
                tuning.pickerEstimatedBytesPerTerm,
                tuning.pickerCoverageRewardPerTerm,
                tuning.pickerScoringWeights
        );
        long pickMs = nanosToMillis(System.nanoTime() - pickStartNs);
        List<SelectedGroup> selectedGroups = toSelectedGroups(selectedCandidates, termVocabulary);
        if (debugLog) {
            LOG.log(Level.INFO,
                    "[fptoken-selector] pick-done recomputed={0}, selected={1}, outputGroups={2}",
                    new Object[] { recomputed.size(), selectedCandidates.size(), selectedGroups.size() });
        }
        long totalMs = nanosToMillis(System.nanoTime() - totalStartNs);
        return buildResult(
                selectedGroups,
                miningStats,
                request.maxCandidateCount,
                buildDiagnostics(
                        docCount,
                        sampleSize,
                        miningInput,
                        miningInputOutcome.sampledInputBuildFallbackToFull,
                        sampledMiningFallbackToFull,
                        indexBuildMs,
                        miningInputBuildMs,
                        miningMs,
                        hintMergeMs,
                        recomputeMs,
                        pickMs,
                        totalMs,
                        request.premergeHints.size(),
                        hintCandidates.size(),
                        mergedCandidates.size()
                )
        );
    }

    /** 随机选择文档子集的 docId。 */
    private static int[] sampleDocIds(int[] allDocIds, int sampleSize) {
        int docCount = allDocIds.length;
        int[] all = Arrays.copyOf(allDocIds, allDocIds.length);
        // Fisher-Yates 部分洗牌，只洗前 sampleSize 个
        Random rnd = new Random(42);
        for (int i = 0; i < sampleSize && i < docCount - 1; i++) {
            int j = i + rnd.nextInt(docCount - i);
            int tmp = all[i];
            all[i] = all[j];
            all[j] = tmp;
        }
        int[] out = new int[sampleSize];
        System.arraycopy(all, 0, out, 0, sampleSize);
        java.util.Arrays.sort(out);
        return out;
    }

    private static BitSet buildSampleMask(int[] sampledDocIds, int docCount) {
        BitSet sampleMask = new BitSet(docCount);
        for (int sid : sampledDocIds) {
            sampleMask.set(sid);
        }
        return sampleMask;
    }

    private static List<BitSet> buildSampledTidsets(List<BitSet> fullTidsets, BitSet sampleMask) {
        List<BitSet> sampledTidsets = new ArrayList<>(fullTidsets.size());
        for (BitSet full : fullTidsets) {
            BitSet sampled = (BitSet) full.clone();
            sampled.and(sampleMask);
            sampledTidsets.add(sampled);
        }
        return sampledTidsets;
    }

    /**
     * 统一采样/非采样输入构建：
     * <ul>
     *   <li>sampleSize >= docCount：直接使用全量 tidsets + 全量 minSupport（非采样路径）。</li>
     *   <li>sampleSize < docCount：生成采样 tidsets + 缩放后的 sampledMinSupport（采样路径）。</li>
     * </ul>
     *
     * <p>上层挖掘逻辑只消费 {@link MiningInput}，无需感知数据源类型。</p>
     */
    private static MiningInput buildMiningInput(
            int[] allDocIds,
            List<BitSet> fullTidsets,
            int docCount,
            int sampleSize,
            int minSupport,
            double samplingSupportScale
    ) {
        if (sampleSize >= docCount) {
            return new MiningInput(fullTidsets, docCount, minSupport, false);
        }
        if (isTestingFailSamplingPathEnabled()) {
            throw new IllegalStateException("Injected sampling path failure for testing");
        }
        int[] sampledDocIds = sampleDocIds(allDocIds, sampleSize);
        BitSet sampleMask = buildSampleMask(sampledDocIds, docCount);
        List<BitSet> sampledTidsets = buildSampledTidsets(fullTidsets, sampleMask);
        int sampledMinSupport = computeSampledMinSupport(
                minSupport,
                docCount,
                sampleSize,
                samplingSupportScale
        );
        return new MiningInput(sampledTidsets, sampleSize, sampledMinSupport, true);
    }

    private static MiningInputBuildOutcome buildMiningInputWithFallback(
            int[] allDocIds,
            List<BitSet> fullTidsets,
            int docCount,
            int sampleSize,
            int minSupport,
            double samplingSupportScale,
            boolean debugLog
    ) {
        try {
            return new MiningInputBuildOutcome(
                    buildMiningInput(
                            allDocIds,
                            fullTidsets,
                            docCount,
                            sampleSize,
                            minSupport,
                            samplingSupportScale
                    ),
                    false
            );
        } catch (RuntimeException samplingFailure) {
            if (sampleSize >= docCount) {
                throw samplingFailure;
            }
            if (debugLog) {
                LOG.log(Level.WARNING,
                        "[fptoken-selector] build-sampled-input failed, fallback to full-input. reason={0}",
                        new Object[] { samplingFailure.toString() });
            }
            return new MiningInputBuildOutcome(new MiningInput(fullTidsets, docCount, minSupport, false), true);
        }
    }

    private static ExclusiveSelectionResult.SelectionDiagnostics buildDiagnostics(
            int docCount,
            int targetSampleSize,
            MiningInput miningInput,
            boolean sampledInputBuildFallbackToFull,
            boolean sampledMiningFallbackToFull,
            long indexBuildMs,
            long miningInputBuildMs,
            long miningMs,
            long hintMergeMs,
            long recomputeMs,
            long pickMs,
            long totalMs,
            int premergeHintInputCount,
            int mappedHintCandidateCount,
            int mergedDistinctCandidateCount
    ) {
        return ExclusiveSelectionResult.SelectionDiagnostics.of(
                targetSampleSize < docCount,
                miningInput.usesSampling,
                sampledInputBuildFallbackToFull,
                sampledMiningFallbackToFull,
                targetSampleSize,
                miningInput.miningDocCount,
                indexBuildMs,
                miningInputBuildMs,
                miningMs,
                hintMergeMs,
                recomputeMs,
                pickMs,
                totalMs,
                premergeHintInputCount,
                mappedHintCandidateCount,
                mergedDistinctCandidateCount
        );
    }

    private static long nanosToMillis(long nanos) {
        return nanos / 1_000_000L;
    }

    private static boolean isTestingFailSamplingPathEnabled() {
        return Boolean.parseBoolean(System.getProperty(PROP_TESTING_FAIL_SAMPLING_PATH, "false"));
    }

    private static boolean isDebugLogEnabled() {
        return Boolean.parseBoolean(System.getProperty(DEBUG_LOG_PROPERTY, "false"));
    }

    private static int[] collectDistinctDocIds(List<DocTerms> rows) {
        IntOpenAddressingSet docIds = new IntOpenAddressingSet(rows.size());
        for (int rowIndex = 0; rowIndex < rows.size(); rowIndex++) {
            DocTerms row = rows.get(rowIndex);
            if (row == null) {
                throw new IllegalArgumentException("rows[" + rowIndex + "] must not be null");
            }
            int docId = row.getDocId();
            if (docId < 0) {
                throw new IllegalArgumentException("docId must be >= 0, got " + docId);
            }
            docIds.add(docId);
        }
        int[] out = docIds.toArray();
        Arrays.sort(out);
        return out;
    }

    /** 在全量 tidsets 上重新计算候选组合的 doclist，并过滤掉不满足 minSupport 的。 */
    private static List<CandidateItemset> recomputeOnFullData(
            List<CandidateItemset> candidates,
            List<BitSet> fullTidsets,
            int minSupport
    ) {
        RecomputeOnFullDataResult result = recomputeOnFullDataWithPrefixPruningStats(candidates, fullTidsets, minSupport);
        if (isDebugLogEnabled() && result.prefixPrunedCandidates > 0) {
            LOG.log(
                    Level.INFO,
                    "[fptoken-selector] recompute-prefix-pruned candidates={0}, minSupport={1}",
                    new Object[] { result.prefixPrunedCandidates, minSupport }
            );
        }
        return result.recomputedCandidates;
    }

    private static RecomputeOnFullDataResult recomputeOnFullDataWithPrefixPruningStats(
            List<CandidateItemset> candidates,
            List<BitSet> fullTidsets,
            int minSupport
    ) {
        List<CandidateItemset> out = new ArrayList<>(candidates.size());
        BitSet scratch = new BitSet(64);
        Map<IntArrayKey, Integer> prefixSupportCache = new HashMap<>();
        int prefixPrunedCandidates = 0;
        for (CandidateItemset candidate : candidates) {
            int[] termIds = candidate.getTermIdsUnsafe();
            boolean prunedByKnownPrefix = false;
            for (int prefixLen = 1; prefixLen < termIds.length; prefixLen++) {
                IntArrayKey key = new IntArrayKey(Arrays.copyOf(termIds, prefixLen));
                Integer cachedPrefixSupport = prefixSupportCache.get(key);
                if (cachedPrefixSupport != null && cachedPrefixSupport.intValue() < minSupport) {
                    prunedByKnownPrefix = true;
                    break;
                }
            }
            if (prunedByKnownPrefix) {
                prefixPrunedCandidates++;
                continue;
            }
            scratch.clear();
            scratch.or(fullTidsets.get(termIds[0]));
            for (int i = 1; i < termIds.length && scratch.cardinality() >= minSupport; i++) {
                scratch.and(fullTidsets.get(termIds[i]));
                IntArrayKey prefixKey = new IntArrayKey(Arrays.copyOf(termIds, i + 1));
                prefixSupportCache.put(prefixKey, Integer.valueOf(scratch.cardinality()));
            }
            int fullSupport = scratch.cardinality();
            prefixSupportCache.put(new IntArrayKey(Arrays.copyOf(termIds, termIds.length)), Integer.valueOf(fullSupport));
            if (fullSupport >= minSupport) {
                BitSet docBits = (BitSet) scratch.clone();
                out.add(CandidateItemset.trusted(termIds, docBits, fullSupport));
            }
        }
        return new RecomputeOnFullDataResult(out, prefixPrunedCandidates);
    }

    private static final class RecomputeOnFullDataResult {
        private final List<CandidateItemset> recomputedCandidates;
        private final int prefixPrunedCandidates;

        private RecomputeOnFullDataResult(List<CandidateItemset> recomputedCandidates, int prefixPrunedCandidates) {
            this.recomputedCandidates = recomputedCandidates;
            this.prefixPrunedCandidates = prefixPrunedCandidates;
        }
    }


    /**
     * 可读性更友好的别名入口:语义与
     * {@link #selectExclusiveBestItemsetsWithStats(List, int, int, int, int)} 完全一致。
     */
    public static ExclusiveSelectionResult findMutuallyExclusivePatternsWithStats(
            List<DocTerms> inputDocuments,
            int minimumRequiredSupport,
            int minimumPatternLength,
            int maximumPatternLength,
            int maximumIntermediateResults
    ) {
        return selectExclusiveBestItemsetsWithStats(
                inputDocuments,
                minimumRequiredSupport,
                minimumPatternLength,
                maximumPatternLength,
                maximumIntermediateResults
        );
    }

    /**
     * 调用 Beam 挖掘器,并传入门面层固定的扩展/束宽参数。
     *
     * <p><b>模块协议</b>：index 侧仅输出
     * {@link ModuleDataContracts.TidsetMiningInput}，miner 侧仅消费该协议并产出候选。</p>
     *
     * @param tidsetsByTermId index 输出的 tidset（termId 对齐）
     * @param plan 支持度与长度、候选上限及自适应挖掘参数
     * @return 候选项与统计
     */
    private static FrequentItemsetMiningResult mineCandidates(
            ModuleDataContracts.TidsetMiningInput miningInput,
            MiningPlan plan
    ) {
        BeamFrequentItemsetMiner miner = new BeamFrequentItemsetMiner();
        return miner.mineWithStats(
                miningInput,
                plan.config,
                plan.maxFrequentTermCount,
                plan.maxBranchingFactor,
                plan.beamWidth,
                plan.maxIdleLevels,
                plan.maxRuntimeMillis
        );
    }

    /**
     * 两阶段互斥挑选;词典大小用于分配词占用位图。
     *
     * <p><b>模块协议</b>：miner 侧仅输出候选列表，本阶段通过
     * {@link ModuleDataContracts.PickerInput} 接收候选与词典上界。</p>
     *
     * @param candidates 挖掘得到的候选项（顺序会影响替换阶段扫描次序）
     * @param dictionarySize {@code idToTerm.size()}，即 termId 上界（不含）参考
     * @return 互斥子集
     */
    private static List<CandidateItemset> pickExclusiveCandidates(
            List<CandidateItemset> candidates,
            int dictionarySize,
            int minNetGain,
            int estimatedBytesPerTerm,
            int coverageRewardPerTerm,
            TwoPhaseExclusiveItemsetPicker.ScoringWeights scoringWeights
    ) {
        TwoPhaseExclusiveItemsetPicker picker = new TwoPhaseExclusiveItemsetPicker();
        return picker.pick(
                candidates,
                dictionarySize,
                adaptiveMaxSwapTrials(candidates.size()),
                minNetGain,
                estimatedBytesPerTerm,
                coverageRewardPerTerm,
                scoringWeights
        );
    }

    /**
     * 基于词汇量做分段:小词表收窄 beam,超大词表放宽 beam。
     */
    private static int adaptiveBeamWidth(int vocabularySize) {
        if (vocabularySize < 500) {
            return 24;
        }
        if (vocabularySize < 2000) {
            return EngineTuningConfig.FACADE_DEFAULT_BEAM_WIDTH;
        }
        if (vocabularySize < 5000) {
            return 48;
        }
        return 64;
    }

    /**
     * 词汇量越大,允许参与扩展的高频词上限越高,避免过早截断。
     */
    private static int adaptiveMaxFrequentTermCount(int vocabularySize) {
        if (vocabularySize < 500) {
            return 500;
        }
        if (vocabularySize < 2000) {
            return EngineTuningConfig.DEFAULT_MAX_FREQUENT_TERM_COUNT;
        }
        if (vocabularySize < 5000) {
            return 2000;
        }
        return 3000;
    }

    private static int adaptiveMaxBranchingFactor(int vocabularySize) {
        if (vocabularySize < 800) {
            return 12;
        }
        if (vocabularySize < 3000) {
            return EngineTuningConfig.DEFAULT_MAX_BRANCHING_FACTOR;
        }
        if (vocabularySize < 8000) {
            return 20;
        }
        return 28;
    }

    private static int adaptiveMaxIdleLevels(int vocabularySize) {
        if (vocabularySize < 1200) {
            return 1;
        }
        if (vocabularySize < 4000) {
            return 2;
        }
        return 3;
    }

    private static long adaptiveMaxRuntimeMillis(int docCount, int vocabularySize) {
        long scale = (long) docCount + (long) vocabularySize;
        if (scale < 8000L) {
            return 1_500L;
        }
        if (scale < 20_000L) {
            return 3_000L;
        }
        if (scale < 60_000L) {
            return 6_000L;
        }
        return 10_000L;
    }

    private static RuntimeTuningSnapshot defaultRuntimeTuningSnapshot() {
        return normalizeRuntimeTuningSnapshot(
                EngineTuningConfig.DEFAULT_SAMPLE_RATIO,
                EngineTuningConfig.DEFAULT_MIN_SAMPLE_COUNT,
                EngineTuningConfig.DEFAULT_SAMPLING_SUPPORT_SCALE,
                EngineTuningConfig.PICKER_DEFAULT_MIN_NET_GAIN,
                EngineTuningConfig.PICKER_ESTIMATED_BYTES_PER_TERM,
                EngineTuningConfig.PICKER_DEFAULT_COVERAGE_REWARD_PER_TERM,
                EngineTuningConfig.DEFAULT_MAX_DOC_COVERAGE_RATIO,
                TwoPhaseExclusiveItemsetPicker.getRuntimeScoringWeights()
        );
    }

    private static RuntimeTuningSnapshot loadRuntimeTuningSnapshotFromExternal() {
        return applyExternalSamplingOverrides(defaultRuntimeTuningSnapshot());
    }

    private static RuntimeTuningSnapshot applyExternalSamplingOverrides(RuntimeTuningSnapshot base) {
        double sampleRatio = resolveDoubleExternalValue(
                PROP_SAMPLE_RATIO,
                ENV_SAMPLE_RATIO,
                base.sampleRatio
        );
        int minSampleCount = resolveIntExternalValue(
                PROP_MIN_SAMPLE_COUNT,
                ENV_MIN_SAMPLE_COUNT,
                base.minSampleCount
        );
        double samplingSupportScale = resolveDoubleExternalValue(
                PROP_SAMPLING_SUPPORT_SCALE,
                ENV_SAMPLING_SUPPORT_SCALE,
                base.samplingSupportScale
        );
        return normalizeRuntimeTuningSnapshot(
                sampleRatio,
                minSampleCount,
                samplingSupportScale,
                base.pickerMinNetGain,
                base.pickerEstimatedBytesPerTerm,
                base.pickerCoverageRewardPerTerm,
                base.maxDocCoverageRatio,
                base.pickerScoringWeights
        );
    }

    private static double resolveDoubleExternalValue(String propertyKey, String envKey, double fallback) {
        String raw = readExternalRawValue(propertyKey, envKey);
        if (raw == null) {
            return fallback;
        }
        try {
            return Double.parseDouble(raw.trim());
        } catch (NumberFormatException ignored) {
            return fallback;
        }
    }

    private static int resolveIntExternalValue(String propertyKey, String envKey, int fallback) {
        String raw = readExternalRawValue(propertyKey, envKey);
        if (raw == null) {
            return fallback;
        }
        try {
            return Integer.parseInt(raw.trim());
        } catch (NumberFormatException ignored) {
            return fallback;
        }
    }

    private static String readExternalRawValue(String propertyKey, String envKey) {
        String fromProperty = System.getProperty(propertyKey);
        if (fromProperty != null && !fromProperty.trim().isEmpty()) {
            return fromProperty;
        }
        String fromEnv = System.getenv(envKey);
        if (fromEnv != null && !fromEnv.trim().isEmpty()) {
            return fromEnv;
        }
        return null;
    }

    /**
     * 统一运行时调参校验入口：所有 setter 与默认值初始化都走这里，避免校验规则散落。
     */
    private static RuntimeTuningSnapshot normalizeRuntimeTuningSnapshot(
            double sampleRatio,
            int minSampleCount,
            double samplingSupportScale,
            int pickerMinNetGain,
            int pickerEstimatedBytesPerTerm,
            int pickerCoverageRewardPerTerm,
            double maxDocCoverageRatio,
            TwoPhaseExclusiveItemsetPicker.ScoringWeights pickerScoringWeights
    ) {
        double normalizedSampleRatio = Math.max(0.0d, Math.min(1.0d, sampleRatio));
        int normalizedMinSampleCount = Math.max(1, minSampleCount);
        double normalizedSamplingSupportScale = samplingSupportScale;
        if (pickerMinNetGain < 0) {
            throw new IllegalArgumentException("minNetGain must be >= 0");
        }
        if (pickerEstimatedBytesPerTerm <= 0) {
            throw new IllegalArgumentException("estimatedBytesPerTerm must be > 0");
        }
        if (pickerCoverageRewardPerTerm < 0) {
            throw new IllegalArgumentException("coverageRewardPerTerm must be >= 0");
        }
        double normalizedMaxDocCoverageRatio = maxDocCoverageRatio <= 0d
                ? 1d
                : Math.min(1d, maxDocCoverageRatio);
        return new RuntimeTuningSnapshot(
                normalizedSampleRatio,
                normalizedMinSampleCount,
                normalizedSamplingSupportScale,
                pickerMinNetGain,
                pickerEstimatedBytesPerTerm,
                pickerCoverageRewardPerTerm,
                normalizedMaxDocCoverageRatio,
                pickerScoringWeights
        );
    }

    private static RuntimeTuningSnapshot captureRuntimeTuningSnapshot() {
        RuntimeTuningSnapshot current = runtimeTuningSnapshot;
        return normalizeRuntimeTuningSnapshot(
                current.sampleRatio,
                current.minSampleCount,
                current.samplingSupportScale,
                current.pickerMinNetGain,
                current.pickerEstimatedBytesPerTerm,
                current.pickerCoverageRewardPerTerm,
                current.maxDocCoverageRatio,
                TwoPhaseExclusiveItemsetPicker.getRuntimeScoringWeights()
        );
    }

    private static RuntimeTuningSnapshot resolveTuningSnapshot(SelectionRequest request) {
        ExecutionTuning t = request.executionTuning;
        return normalizeRuntimeTuningSnapshot(
                t.sampleRatio,
                t.minSampleCount,
                t.samplingSupportScale,
                t.pickerMinNetGain,
                t.pickerEstimatedBytesPerTerm,
                t.pickerCoverageRewardPerTerm,
                t.maxDocCoverageRatio,
                t.pickerScoringWeights
        );
    }

    private static void validateSelectionRequestPreconditions(SelectionRequest request) {
        Objects.requireNonNull(request, "request");
        Objects.requireNonNull(request.rows, "request.rows");
        Objects.requireNonNull(request.executionTuning, "request.executionTuning");
    }

    /**
     * 候选少时减少 1-opt 尝试,候选多时提高上限但保留硬上界避免拖慢整体延迟。
     */
    private static int adaptiveMaxSwapTrials(int candidateCount) {
        if (candidateCount < 500) {
            return 30;
        }
        if (candidateCount < 2000) {
            return EngineTuningConfig.DEFAULT_MAX_SWAP_TRIALS;
        }
        if (candidateCount < 5000) {
            return 150;
        }
        return Math.min(300, candidateCount / 20);
    }

    public static LookupRoute routeLookupByHitRate(double estimatedHitRate) {
        if (estimatedHitRate <= EngineTuningConfig.LOOKUP_LOW_HIT_RATE_THRESHOLD) {
            return LookupRoute.DATA_SKIPPING;
        }
        if (estimatedHitRate >= EngineTuningConfig.LOOKUP_HIGH_HIT_RATE_THRESHOLD) {
            return LookupRoute.FREQUENT_ITEMSET;
        }
        return LookupRoute.HYBRID;
    }

    private static MiningPlan buildMiningPlan(SelectorConfig config, int docCount, int vocabularySize) {
        return new MiningPlan(
                config,
                adaptiveMaxFrequentTermCount(vocabularySize),
                adaptiveMaxBranchingFactor(vocabularySize),
                adaptiveBeamWidth(vocabularySize),
                adaptiveMaxIdleLevels(vocabularySize),
                adaptiveMaxRuntimeMillis(docCount, vocabularySize)
        );
    }

    /** 空输入或无词时的统一返回形态,避免调用方判 null。 */
    private static ExclusiveSelectionResult emptyResult(int maxCandidateCount) {
        return new ExclusiveSelectionResult(
                Collections.<SelectedGroup>emptyList(),
                0,
                0,
                0,
                maxCandidateCount,
                false
        );
    }

    /**
     * 将挖掘统计原样并入对外结果;{@code maxCandidateCount} 使用本次调用入参而非配置内字段,便于日志对比。
     */
    private static ExclusiveSelectionResult buildResult(
            List<SelectedGroup> groups,
            FrequentItemsetMiningResult miningResult,
            int maxCandidateCount,
            ExclusiveSelectionResult.SelectionDiagnostics diagnostics
    ) {
        return new ExclusiveSelectionResult(
                groups,
                miningResult.getFrequentTermCount(),
                miningResult.getGeneratedCandidateCount(),
                miningResult.getIntersectionCount(),
                maxCandidateCount,
                miningResult.isTruncatedByCandidateLimit(),
                diagnostics
        );
    }

    /**
     * 将内部 {@link CandidateItemset} 转为对外 {@link SelectedGroup}。
     * <ul>
     *   <li>词字节通过 {@link ByteArrayUtils#copy(byte[])} 拷贝,避免暴露索引内部数组引用。</li>
     *   <li>doc 列表由 {@link #bitSetToDocIds(BitSet)} 按升序 docId 输出。</li>
     *   <li>还原词前校验 {@code termId ∈ [0, idToTerm.size())},避免损坏的候选项触发 {@link List#get} 越界。</li>
     * </ul>
     */
    private static List<SelectedGroup> toSelectedGroups(
            List<CandidateItemset> selectedCandidates,
            List<ByteRef> termVocabulary
    ) {
        int vocabularySize = termVocabulary.size();
        int selectedCount = selectedCandidates.size();
        List<SelectedGroup> out = new ArrayList<>(selectedCount);
        for (int selectedIndex = 0; selectedIndex < selectedCount; selectedIndex++) {
            CandidateItemset candidate = selectedCandidates.get(selectedIndex);
            int[] candidateTermIds = candidate.getTermIdsUnsafe();
            List<ByteRef> terms = new ArrayList<>(candidateTermIds.length);
            for (int i = 0; i < candidateTermIds.length; i++) {
                int termId = candidateTermIds[i];
                assertTermIdWithinVocabulary(termId, vocabularySize);
                terms.add(termVocabulary.get(termId));
            }
            out.add(new SelectedGroup(
                    terms,
                    bitSetToDocIds(candidate.getDocBitsUnsafe()),
                    candidate.getSupport(),
                    candidate.getEstimatedSaving()
            ));
        }
        return out;
    }

    private static void assertTermIdWithinVocabulary(int termId, int vocabularySize) {
        if (termId < 0 || termId >= vocabularySize) {
            throw new IllegalArgumentException(
                    "termId out of bounds: " + termId + ", expected [0, " + vocabularySize + ")");
        }
    }

    /**
     * 将项集命中的文档集合(位图)转为 docId 列表。
     * <p>前提:位下标与 {@link DocTerms#getDocId()} 一致(与 {@link TermTidsetIndex} 构建方式一致)。
     *
     * @param docBits 命中文档位图,可为空集
     * @return 升序 docId 列表
     */
    private static List<Integer> bitSetToDocIds(BitSet docBits) {
        int cardinality = docBits.cardinality();
        if (cardinality == 0) {
            return Collections.emptyList();
        }
        // 直接使用ArrayList预分配,减少扩容
        List<Integer> out = new ArrayList<>(cardinality);
        for (int i = docBits.nextSetBit(0); i >= 0; i = docBits.nextSetBit(i + 1)) {
            out.add(i);
        }
        return out;
    }

    private static final class MiningPlan {
        private final SelectorConfig config;
        private final int maxFrequentTermCount;
        private final int maxBranchingFactor;
        private final int beamWidth;
        private final int maxIdleLevels;
        private final long maxRuntimeMillis;

        private MiningPlan(
                SelectorConfig config,
                int maxFrequentTermCount,
                int maxBranchingFactor,
                int beamWidth,
                int maxIdleLevels,
                long maxRuntimeMillis
        ) {
            this.config = config;
            this.maxFrequentTermCount = maxFrequentTermCount;
            this.maxBranchingFactor = maxBranchingFactor;
            this.beamWidth = beamWidth;
            this.maxIdleLevels = maxIdleLevels;
            this.maxRuntimeMillis = maxRuntimeMillis;
        }
    }

    /**
     * 单次调用的运行时调参快照，避免在深层方法中隐式读取可变静态字段。
     */
    private static final class RuntimeTuningSnapshot {
        private final double sampleRatio;
        private final int minSampleCount;
        private final double samplingSupportScale;
        private final int pickerMinNetGain;
        private final int pickerEstimatedBytesPerTerm;
        private final int pickerCoverageRewardPerTerm;
        private final double maxDocCoverageRatio;
        private final TwoPhaseExclusiveItemsetPicker.ScoringWeights pickerScoringWeights;

        private RuntimeTuningSnapshot(
                double sampleRatio,
                int minSampleCount,
                double samplingSupportScale,
                int pickerMinNetGain,
                int pickerEstimatedBytesPerTerm,
                int pickerCoverageRewardPerTerm,
                double maxDocCoverageRatio,
                TwoPhaseExclusiveItemsetPicker.ScoringWeights pickerScoringWeights
        ) {
            this.sampleRatio = sampleRatio;
            this.minSampleCount = minSampleCount;
            this.samplingSupportScale = samplingSupportScale;
            this.pickerMinNetGain = pickerMinNetGain;
            this.pickerEstimatedBytesPerTerm = pickerEstimatedBytesPerTerm;
            this.pickerCoverageRewardPerTerm = pickerCoverageRewardPerTerm;
            this.maxDocCoverageRatio = maxDocCoverageRatio;
            this.pickerScoringWeights = Objects.requireNonNull(pickerScoringWeights, "pickerScoringWeights");
        }
    }

    /**
     * 选择入口配置对象：聚合可调参数，避免多重重载导致的参数爆炸。
     */
    public static final class SelectionRequest {
        private final List<DocTerms> rows;
        private final int minSupport;
        private final int minItemsetSize;
        private final int maxItemsetSize;
        private final int maxCandidateCount;
        private final List<PremergeHintCandidate> premergeHints;
        private final int hintBoostWeight;
        private final ExecutionTuning executionTuning;

        private SelectionRequest(
                List<DocTerms> rows,
                int minSupport,
                int minItemsetSize,
                int maxItemsetSize,
                int maxCandidateCount,
                List<PremergeHintCandidate> premergeHints,
                int hintBoostWeight,
                ExecutionTuning executionTuning
        ) {
            this.rows = Collections.unmodifiableList(new ArrayList<DocTerms>(
                    Objects.requireNonNull(rows, "rows")));
            this.minSupport = minSupport;
            this.minItemsetSize = minItemsetSize;
            this.maxItemsetSize = maxItemsetSize;
            this.maxCandidateCount = maxCandidateCount;
            this.premergeHints = premergeHints == null
                    ? Collections.<PremergeHintCandidate>emptyList()
                    : Collections.unmodifiableList(new ArrayList<PremergeHintCandidate>(premergeHints));
            if (hintBoostWeight < 0) {
                throw new IllegalArgumentException("hintBoostWeight must be >= 0");
            }
            this.hintBoostWeight = hintBoostWeight;
            this.executionTuning = executionTuning != null
                    ? executionTuning
                    : ExecutionTuning.fromCurrentRuntimeDefaults();
        }

        public static Builder builder(List<DocTerms> rows) {
            return new Builder(rows);
        }

        public static final class Builder {
            private final List<DocTerms> rows;
            private int minSupport = 1;
            private int minItemsetSize = 1;
            private int maxItemsetSize = EngineTuningConfig.DEFAULT_MAX_ITEMSET_SIZE;
            private int maxCandidateCount = EngineTuningConfig.DEFAULT_MAX_CANDIDATE_COUNT;
            private List<PremergeHintCandidate> premergeHints = Collections.emptyList();
            private int hintBoostWeight = 0;
            private ExecutionTuning executionTuning = ExecutionTuning.fromCurrentRuntimeDefaults();

            private Builder(List<DocTerms> rows) {
                this.rows = rows;
            }

            public Builder minSupport(int value) {
                this.minSupport = value;
                return this;
            }

            public Builder minItemsetSize(int value) {
                this.minItemsetSize = value;
                return this;
            }

            public Builder maxItemsetSize(int value) {
                this.maxItemsetSize = value;
                return this;
            }

            public Builder maxCandidateCount(int value) {
                this.maxCandidateCount = value;
                return this;
            }

            public Builder premergeHints(List<PremergeHintCandidate> value) {
                this.premergeHints = value;
                return this;
            }

            public Builder hintBoostWeight(int value) {
                this.hintBoostWeight = value;
                return this;
            }

            public Builder executionTuning(ExecutionTuning value) {
                this.executionTuning = value;
                return this;
            }

            public SelectionRequest build() {
                return new SelectionRequest(
                        rows,
                        minSupport,
                        minItemsetSize,
                        maxItemsetSize,
                        maxCandidateCount,
                        premergeHints,
                        hintBoostWeight,
                        executionTuning
                );
            }
        }
    }

    /**
     * 单次请求级调参对象：用于无状态并发场景下替代全局静态配置。
     */
    public static final class ExecutionTuning {
        private final double sampleRatio;
        private final int minSampleCount;
        private final double samplingSupportScale;
        private final int pickerMinNetGain;
        private final int pickerEstimatedBytesPerTerm;
        private final int pickerCoverageRewardPerTerm;
        private final double maxDocCoverageRatio;
        private final TwoPhaseExclusiveItemsetPicker.ScoringWeights pickerScoringWeights;

        public ExecutionTuning(
                double sampleRatio,
                int minSampleCount,
                double samplingSupportScale,
                int pickerMinNetGain,
                int pickerEstimatedBytesPerTerm,
                int pickerCoverageRewardPerTerm,
                double maxDocCoverageRatio,
                TwoPhaseExclusiveItemsetPicker.ScoringWeights pickerScoringWeights
        ) {
            this.sampleRatio = sampleRatio;
            this.minSampleCount = minSampleCount;
            this.samplingSupportScale = samplingSupportScale;
            this.pickerMinNetGain = pickerMinNetGain;
            this.pickerEstimatedBytesPerTerm = pickerEstimatedBytesPerTerm;
            this.pickerCoverageRewardPerTerm = pickerCoverageRewardPerTerm;
            this.maxDocCoverageRatio = maxDocCoverageRatio;
            this.pickerScoringWeights = Objects.requireNonNull(pickerScoringWeights, "pickerScoringWeights");
        }

        public static ExecutionTuning fromCurrentRuntimeDefaults() {
            RuntimeTuningSnapshot snapshot = captureRuntimeTuningSnapshot();
            return new ExecutionTuning(
                    snapshot.sampleRatio,
                    snapshot.minSampleCount,
                    snapshot.samplingSupportScale,
                    snapshot.pickerMinNetGain,
                    snapshot.pickerEstimatedBytesPerTerm,
                    snapshot.pickerCoverageRewardPerTerm,
                    snapshot.maxDocCoverageRatio,
                    snapshot.pickerScoringWeights
            );
        }
    }

    private static final class MiningInput {
        private final List<BitSet> miningTidsets;
        private final int miningDocCount;
        private final int miningMinSupport;
        private final boolean usesSampling;

        private MiningInput(
                List<BitSet> miningTidsets,
                int miningDocCount,
                int miningMinSupport,
                boolean usesSampling
        ) {
            this.miningTidsets = miningTidsets;
            this.miningDocCount = miningDocCount;
            this.miningMinSupport = miningMinSupport;
            this.usesSampling = usesSampling;
        }
    }

    private static final class MiningInputBuildOutcome {
        private final MiningInput miningInput;
        private final boolean sampledInputBuildFallbackToFull;

        private MiningInputBuildOutcome(MiningInput miningInput, boolean sampledInputBuildFallbackToFull) {
            this.miningInput = miningInput;
            this.sampledInputBuildFallbackToFull = sampledInputBuildFallbackToFull;
        }
    }

    /** merge 阶段前置提示：仅词项序列。 */
    public static final class PremergeHintCandidate {
        private final List<ByteRef> termRefs;
        private final int qualityScore;

        public PremergeHintCandidate(List<ByteRef> termRefs) {
            this(termRefs, 1);
        }

        public PremergeHintCandidate(List<ByteRef> termRefs, int qualityScore) {
            this.termRefs = Collections.unmodifiableList(new ArrayList<>(termRefs));
            if (qualityScore <= 0) {
                throw new IllegalArgumentException("qualityScore must be > 0");
            }
            this.qualityScore = qualityScore;
        }

        public List<ByteRef> getTermRefs() {
            return termRefs;
        }

        public int getQualityScore() {
            return qualityScore;
        }
    }

    /**
     * 兼容保留：历史测试通过反射校验该键类型的 equals/hash 语义。
     * 新实现中的 hint 去重键已迁移到独立组件。
     */
    private static final class IntArrayKey {
        private final int[] values;
        private final int hash;

        private IntArrayKey(int[] values) {
            this.values = Arrays.copyOf(values, values.length);
            this.hash = Arrays.hashCode(this.values);
        }

        @Override
        public int hashCode() {
            return hash;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof IntArrayKey)) {
                return false;
            }
            return Arrays.equals(values, ((IntArrayKey) obj).values);
        }
    }

}
