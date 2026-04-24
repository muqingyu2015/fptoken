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
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.picker.TwoPhaseExclusiveItemsetPicker;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.util.ByteArrayUtils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

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
 *   <li>采样配置（如 {@code setSampleRatio}）是本类静态全局状态，不是“每次调用隔离”。</li>
 *   <li>若多线程/多租户场景需要不同采样参数，请在调用层做串行化或隔离实例封装。</li>
 *   <li>{@code rows} 中 {@code docId} 必须可作为位图下标使用（建议非负且可控范围内）。</li>
 * </ul>
 *
 * @author muqingyu
 */
public final class ExclusiveFrequentItemsetSelector {

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

    private static double sampleRatio = EngineTuningConfig.DEFAULT_SAMPLE_RATIO;
    private static int minSampleCount = EngineTuningConfig.DEFAULT_MIN_SAMPLE_COUNT;
    private static double samplingSupportScale = EngineTuningConfig.DEFAULT_SAMPLING_SUPPORT_SCALE;
    private static int pickerMinNetGain = EngineTuningConfig.PICKER_DEFAULT_MIN_NET_GAIN;
    private static int pickerEstimatedBytesPerTerm = EngineTuningConfig.PICKER_ESTIMATED_BYTES_PER_TERM;
    private static int pickerCoverageRewardPerTerm = EngineTuningConfig.PICKER_DEFAULT_COVERAGE_REWARD_PER_TERM;
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
        sampleRatio = Math.max(0.0, Math.min(1.0, ratio));
    }

    /**
     * 设置最小采样文档数。
     * @param count 正整数，默认 {@link EngineTuningConfig#DEFAULT_MIN_SAMPLE_COUNT}
     */
    public static void setMinSampleCount(int count) {
        minSampleCount = Math.max(1, count);
    }

    /**
     * 设置采样支持度缩放因子。
     * @param scale 0.0=自动按采样比例缩放（默认），1.0=不缩放
     *              其他值表示显式缩放系数（例如 0.5 表示支持度减半）。
     */
    public static void setSamplingSupportScale(double scale) {
        samplingSupportScale = scale;
    }

    /**
     * 保留方法签名以兼容旧调用方。
     * 采样参数不再从 properties 文件读取，请改用 {@link #setSampleRatio(double)}、
     * {@link #setMinSampleCount(int)}、{@link #setSamplingSupportScale(double)} 在代码中配置。
     */
    @Deprecated
    public static synchronized void reloadSamplingConfig() {
        // no-op: sampling is code-configured only.
    }

    /** 当前采样比率（便于测试与运行期观测）。 */
    public static double getSampleRatio() {
        return sampleRatio;
    }

    /** 当前最小采样文档数（便于测试与运行期观测）。 */
    public static int getMinSampleCount() {
        return minSampleCount;
    }

    /** 当前采样支持度缩放因子（便于测试与运行期观测）。 */
    public static double getSamplingSupportScale() {
        return samplingSupportScale;
    }

    /** 当前互斥挑选净收益阈值。 */
    public static int getPickerMinNetGain() {
        return pickerMinNetGain;
    }

    /** 设置互斥挑选净收益阈值。 */
    public static void setPickerMinNetGain(int minNetGain) {
        if (minNetGain < 0) {
            throw new IllegalArgumentException("minNetGain must be >= 0");
        }
        pickerMinNetGain = minNetGain;
    }

    /** 当前互斥挑选的每 term 字典成本估计（字节）。 */
    public static int getPickerEstimatedBytesPerTerm() {
        return pickerEstimatedBytesPerTerm;
    }

    /** 设置互斥挑选的每 term 字典成本估计（字节）。 */
    public static void setPickerEstimatedBytesPerTerm(int estimatedBytesPerTerm) {
        if (estimatedBytesPerTerm <= 0) {
            throw new IllegalArgumentException("estimatedBytesPerTerm must be > 0");
        }
        pickerEstimatedBytesPerTerm = estimatedBytesPerTerm;
    }

    /** 当前互斥挑选每 term 覆盖奖励。 */
    public static int getPickerCoverageRewardPerTerm() {
        return pickerCoverageRewardPerTerm;
    }

    /** 设置互斥挑选每 term 覆盖奖励。 */
    public static void setPickerCoverageRewardPerTerm(int coverageRewardPerTerm) {
        if (coverageRewardPerTerm < 0) {
            throw new IllegalArgumentException("coverageRewardPerTerm must be >= 0");
        }
        pickerCoverageRewardPerTerm = coverageRewardPerTerm;
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
        double effectiveScale = supportScale <= 0.0d ? autoRatio : supportScale;
        return Math.max(1, (int) Math.round(fullMinSupport * effectiveScale));
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
        return selectExclusiveBestItemsetsWithStats(
                rows,
                minSupport,
                minItemsetSize,
                maxItemsetSize,
                maxCandidateCount,
                Collections.<PremergeHintCandidate>emptyList(),
                0
        );
    }

    /**
     * merge 场景扩展入口：可传入前置高频提示候选，提示会在本轮全量数据上二次回算后再参与互斥挑选。
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
        if (rows == null || rows.isEmpty()) {
            return emptyResult(maxCandidateCount);
        }
        if (hintBoostWeight < 0) {
            throw new IllegalArgumentException("hintBoostWeight must be >= 0");
        }

        // 参数统一走 SelectorConfig 校验，确保异常语义与历史一致。
        new SelectorConfig(
                minSupport,
                minItemsetSize,
                maxItemsetSize,
                maxCandidateCount
        );

        int[] allDocIds = collectDistinctDocIds(rows);
        int docCount = allDocIds.length;
        int sampleSize = computeTargetSampleSize(docCount, sampleRatio, minSampleCount);

        // Phase 1: 在全量数据上建索引
        // 使用 buildWithSupportBounds 过滤低频词，显著降低词汇量和内存
        // 当不需要过滤时（minSupport=1），退化到 build() 单次扫描
        TermTidsetIndex fullIndex = TermTidsetIndex.buildWithSupportBounds(
                rows, minSupport, adaptiveMaxDocCoverageRatio());
        List<ByteRef> termVocabulary = fullIndex.getIdToTermRefsUnsafe();
        if (termVocabulary.isEmpty()) {
            return emptyResult(maxCandidateCount);
        }
        List<BitSet> fullTidsets = fullIndex.getTidsetsByTermIdUnsafe();

        // === 采样路径（固定执行）===
        // 1) 随机选择采样文档的 docId
        int[] sampledDocIds = sampleDocIds(allDocIds, sampleSize);
        // 2) 创建采样掩码，从全量 tidsets 中位过滤出采样文档
        BitSet sampleMask = buildSampleMask(sampledDocIds, docCount);
        // 3) 对每个 term 的 tidset 做位过滤（clone + and），产生采样 tidsets
        List<BitSet> sampledTidsets = buildSampledTidsets(fullTidsets, sampleMask);
        // 4) minSupport 按采样比例缩放：采样集小，用更低阈值确保能挖到模式
        //    回算阶段会再用全量 minSupport 过滤，所以缩放不会引入虚假组合
        int sampledMinSupport = computeSampledMinSupport(
                minSupport,
                docCount,
                sampleSize,
                samplingSupportScale
        );
        // 5) 在采样 tidsets 上挖掘
        SelectorConfig sampledConfig = new SelectorConfig(
                sampledMinSupport, minItemsetSize, maxItemsetSize, maxCandidateCount);
        MiningPlan sampledPlan = buildMiningPlan(sampledConfig, sampleSize, termVocabulary.size());
        FrequentItemsetMiningResult miningStats = mineCandidates(sampledTidsets, sampledPlan);
        List<CandidateItemset> minedCandidates = miningStats.getCandidates();
        List<CandidateItemset> hintCandidates = mapHintCandidatesToTermIds(premergeHints, termVocabulary);
        List<CandidateItemset> mergedCandidates = mergeAndDedupCandidates(minedCandidates, hintCandidates);
        if (mergedCandidates.isEmpty()) {
            return buildResult(Collections.<SelectedGroup>emptyList(), miningStats, maxCandidateCount);
        }
        // 6) 回算：在全量 tidsets 上重新计算候选组合的完整 doclist
        //    挖掘阶段得到的 termIds 就是全量索引的 termId，无需映射
        List<CandidateItemset> recomputed = recomputeOnFullData(
                mergedCandidates, fullTidsets, minSupport);
        if (!hintCandidates.isEmpty() && hintBoostWeight > 0) {
            recomputed = applyHintBoost(recomputed, hintCandidates, hintBoostWeight);
        }
        if (recomputed.isEmpty()) {
            return buildResult(Collections.<SelectedGroup>emptyList(), miningStats, maxCandidateCount);
        }
        // 7) 互斥挑选
        List<CandidateItemset> selectedCandidates = pickExclusiveCandidates(
                recomputed, termVocabulary.size(), adaptiveMinNetGain());
        List<SelectedGroup> selectedGroups = toSelectedGroups(selectedCandidates, termVocabulary);
        return buildResult(selectedGroups, miningStats, maxCandidateCount);
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

    private static int[] collectDistinctDocIds(List<DocTerms> rows) {
        HashSet<Integer> docIds = new HashSet<>(rows.size());
        for (int rowIndex = 0; rowIndex < rows.size(); rowIndex++) {
            DocTerms row = rows.get(rowIndex);
            if (row == null) {
                throw new IllegalArgumentException("rows[" + rowIndex + "] must not be null");
            }
            int docId = row.getDocId();
            if (docId < 0) {
                throw new IllegalArgumentException("docId must be >= 0, got " + docId);
            }
            docIds.add(Integer.valueOf(docId));
        }
        int[] out = new int[docIds.size()];
        int idx = 0;
        for (Integer docId : docIds) {
            out[idx++] = docId.intValue();
        }
        Arrays.sort(out);
        return out;
    }

    /** 在全量 tidsets 上重新计算候选组合的 doclist，并过滤掉不满足 minSupport 的。 */
    private static List<CandidateItemset> recomputeOnFullData(
            List<CandidateItemset> candidates,
            List<BitSet> fullTidsets,
            int minSupport
    ) {
        List<CandidateItemset> out = new ArrayList<>(candidates.size());
        BitSet scratch = new BitSet(64);
        for (CandidateItemset candidate : candidates) {
            int[] termIds = candidate.getTermIdsUnsafe();
            scratch.clear();
            scratch.or(fullTidsets.get(termIds[0]));
            for (int i = 1; i < termIds.length && scratch.cardinality() >= minSupport; i++) {
                scratch.and(fullTidsets.get(termIds[i]));
            }
            int fullSupport = scratch.cardinality();
            if (fullSupport >= minSupport) {
                BitSet docBits = (BitSet) scratch.clone();
                out.add(CandidateItemset.trusted(termIds, docBits, fullSupport));
            }
        }
        return out;
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
     * @param index 已构建的垂直索引
     * @param config 支持度与长度、候选上限
     * @return 候选项与统计
     */
    private static FrequentItemsetMiningResult mineCandidates(List<BitSet> tidsetsByTermId, MiningPlan plan) {
        BeamFrequentItemsetMiner miner = new BeamFrequentItemsetMiner();
        return miner.mineWithStats(
                tidsetsByTermId,
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
     * @param candidates 挖掘得到的候选项(顺序会影响替换阶段扫描次序)
     * @param dictionarySize {@code idToTerm.size()},即 termId 上界(不含)参考
     * @return 互斥子集
     */
    private static List<CandidateItemset> pickExclusiveCandidates(
            List<CandidateItemset> candidates,
            int dictionarySize,
            int minNetGain
    ) {
        TwoPhaseExclusiveItemsetPicker picker = new TwoPhaseExclusiveItemsetPicker();
        return picker.pick(
                candidates,
                dictionarySize,
                adaptiveMaxSwapTrials(candidates.size()),
                minNetGain,
                adaptivePickerEstimatedBytesPerTerm(),
                adaptivePickerCoverageRewardPerTerm()
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

    private static double adaptiveMaxDocCoverageRatio() {
        return EngineTuningConfig.DEFAULT_MAX_DOC_COVERAGE_RATIO;
    }

    private static int adaptiveMinNetGain() {
        return pickerMinNetGain;
    }

    private static int adaptivePickerEstimatedBytesPerTerm() {
        return pickerEstimatedBytesPerTerm;
    }

    private static int adaptivePickerCoverageRewardPerTerm() {
        return pickerCoverageRewardPerTerm;
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

    private static List<CandidateItemset> mapHintCandidatesToTermIds(
            List<PremergeHintCandidate> hints,
            List<ByteRef> termVocabulary
    ) {
        if (hints == null || hints.isEmpty()) {
            return Collections.emptyList();
        }
        Map<IntArrayKey, CandidateItemset> out = new HashMap<>();
        for (int i = 0; i < hints.size(); i++) {
            PremergeHintCandidate hint = hints.get(i);
            if (hint == null || hint.getTermRefs().isEmpty()) {
                continue;
            }
            int[] termIds = mapHintTermsToTermIds(hint.getTermRefs(), termVocabulary);
            if (termIds.length == 0) {
                continue;
            }
            IntArrayKey key = new IntArrayKey(termIds);
            if (!out.containsKey(key)) {
                out.put(key, CandidateItemset.trusted(termIds, new BitSet(), 0));
            }
        }
        return new ArrayList<>(out.values());
    }

    private static int[] mapHintTermsToTermIds(List<ByteRef> hintTerms, List<ByteRef> termVocabulary) {
        int[] mapped = new int[hintTerms.size()];
        int used = 0;
        for (int i = 0; i < hintTerms.size(); i++) {
            ByteRef hint = hintTerms.get(i);
            int termId = findTermIdInVocabulary(hint, termVocabulary);
            if (termId >= 0) {
                mapped[used++] = termId;
            }
        }
        if (used == 0) {
            return new int[0];
        }
        int[] termIds = Arrays.copyOf(mapped, used);
        Arrays.sort(termIds);
        int uniqueCount = 1;
        for (int i = 1; i < termIds.length; i++) {
            if (termIds[i] != termIds[i - 1]) {
                termIds[uniqueCount++] = termIds[i];
            }
        }
        return Arrays.copyOf(termIds, uniqueCount);
    }

    private static int findTermIdInVocabulary(ByteRef hint, List<ByteRef> termVocabulary) {
        for (int i = 0; i < termVocabulary.size(); i++) {
            if (equalsRef(hint, termVocabulary.get(i))) {
                return i;
            }
        }
        return -1;
    }

    private static boolean equalsRef(ByteRef a, ByteRef b) {
        if (a.getLength() != b.getLength()) {
            return false;
        }
        byte[] as = a.getSourceUnsafe();
        byte[] bs = b.getSourceUnsafe();
        int ao = a.getOffset();
        int bo = b.getOffset();
        for (int i = 0; i < a.getLength(); i++) {
            if (as[ao + i] != bs[bo + i]) {
                return false;
            }
        }
        return true;
    }

    private static List<CandidateItemset> mergeAndDedupCandidates(
            List<CandidateItemset> mined,
            List<CandidateItemset> hinted
    ) {
        if ((mined == null || mined.isEmpty()) && (hinted == null || hinted.isEmpty())) {
            return Collections.emptyList();
        }
        Map<IntArrayKey, CandidateItemset> merged = new HashMap<>();
        if (mined != null) {
            for (int i = 0; i < mined.size(); i++) {
                CandidateItemset c = mined.get(i);
                merged.put(new IntArrayKey(c.getTermIdsUnsafe()), c);
            }
        }
        if (hinted != null) {
            for (int i = 0; i < hinted.size(); i++) {
                CandidateItemset c = hinted.get(i);
                IntArrayKey key = new IntArrayKey(c.getTermIdsUnsafe());
                if (!merged.containsKey(key)) {
                    merged.put(key, c);
                }
            }
        }
        return new ArrayList<>(merged.values());
    }

    private static List<CandidateItemset> applyHintBoost(
            List<CandidateItemset> recomputed,
            List<CandidateItemset> hintCandidates,
            int hintBoostWeight
    ) {
        Set<IntArrayKey> hintKeys = new HashSet<>();
        for (int i = 0; i < hintCandidates.size(); i++) {
            hintKeys.add(new IntArrayKey(hintCandidates.get(i).getTermIdsUnsafe()));
        }
        List<CandidateItemset> out = new ArrayList<>(recomputed.size());
        for (int i = 0; i < recomputed.size(); i++) {
            CandidateItemset c = recomputed.get(i);
            IntArrayKey key = new IntArrayKey(c.getTermIdsUnsafe());
            if (hintKeys.contains(key)) {
                int boost = hintBoostWeight * Math.max(1, c.length());
                out.add(c.withPriorityBoost(boost));
            } else {
                out.add(c);
            }
        }
        return out;
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
            int maxCandidateCount
    ) {
        return new ExclusiveSelectionResult(
                groups,
                miningResult.getFrequentTermCount(),
                miningResult.getGeneratedCandidateCount(),
                miningResult.getIntersectionCount(),
                maxCandidateCount,
                miningResult.isTruncatedByCandidateLimit()
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

    /** merge 阶段前置提示：仅词项序列。 */
    public static final class PremergeHintCandidate {
        private final List<ByteRef> termRefs;

        public PremergeHintCandidate(List<ByteRef> termRefs) {
            this.termRefs = Collections.unmodifiableList(new ArrayList<>(termRefs));
        }

        public List<ByteRef> getTermRefs() {
            return termRefs;
        }
    }

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
