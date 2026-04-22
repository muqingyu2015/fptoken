package cn.lxdb.plugins.muqingyu.fptoken;

import cn.lxdb.plugins.muqingyu.fptoken.config.SelectorConfig;
import cn.lxdb.plugins.muqingyu.fptoken.config.EngineTuningConfig;
import cn.lxdb.plugins.muqingyu.fptoken.index.TermTidsetIndex;
import cn.lxdb.plugins.muqingyu.fptoken.miner.BeamFrequentItemsetMiner;
import cn.lxdb.plugins.muqingyu.fptoken.model.CandidateItemset;
import cn.lxdb.plugins.muqingyu.fptoken.model.DocTerms;
import cn.lxdb.plugins.muqingyu.fptoken.model.ExclusiveSelectionResult;
import cn.lxdb.plugins.muqingyu.fptoken.model.FrequentItemsetMiningResult;
import cn.lxdb.plugins.muqingyu.fptoken.model.SelectedGroup;
import cn.lxdb.plugins.muqingyu.fptoken.picker.TwoPhaseExclusiveItemsetPicker;
import cn.lxdb.plugins.muqingyu.fptoken.util.ByteArrayUtils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

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
    /** 采样开关：设为 false 则回退为全量挖掘（用于对比测试）。 */
    private static boolean samplingEnabled = true;
    /** 采样配置文件路径（可通过 -Dfptoken.config.file 覆盖）。 */
    private static final String DEFAULT_CONFIG_FILE = "config/fptoken.properties";
    private static final String CONFIG_PATH_PROPERTY = "fptoken.config.file";
    private static final String SAMPLE_RATIO_KEY = "fptoken.sampling.sampleRatio";
    private static final String MIN_SAMPLE_COUNT_KEY = "fptoken.sampling.minSampleCount";
    private static final String SUPPORT_SCALE_KEY = "fptoken.sampling.supportScale";

    static {
        reloadSamplingConfig();
    }

    /**
     * 设置采样开关（用于对比测试）。
     * @param enabled true 启用采样（默认），false 全量挖掘
     */
    public static void setSamplingEnabled(boolean enabled) {
        samplingEnabled = enabled;
    }

    /**
     * 设置采样比率。
     * @param ratio 0.0~1.0，默认 {@link EngineTuningConfig#DEFAULT_SAMPLE_RATIO}
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
     */
    public static void setSamplingSupportScale(double scale) {
        samplingSupportScale = scale;
    }

    /**
     * 从配置文件重载采样参数。未配置项保持当前值。
     * 默认读取 {@code config/fptoken.properties}，
     * 也可通过 JVM 参数 {@code -Dfptoken.config.file=/path/to/file} 指定。
     */
    public static synchronized void reloadSamplingConfig() {
        String configPath = System.getProperty(CONFIG_PATH_PROPERTY, DEFAULT_CONFIG_FILE);
        Path file = Paths.get(configPath);
        if (!Files.exists(file) || !Files.isRegularFile(file)) {
            return;
        }
        Properties properties = new Properties();
        try (java.io.Reader reader = Files.newBufferedReader(file)) {
            properties.load(reader);
            String ratio = properties.getProperty(SAMPLE_RATIO_KEY);
            if (ratio != null && !ratio.trim().isEmpty()) {
                setSampleRatio(Double.parseDouble(ratio.trim()));
            }
            String minCount = properties.getProperty(MIN_SAMPLE_COUNT_KEY);
            if (minCount != null && !minCount.trim().isEmpty()) {
                setMinSampleCount(Integer.parseInt(minCount.trim()));
            }
            String supportScale = properties.getProperty(SUPPORT_SCALE_KEY);
            if (supportScale != null && !supportScale.trim().isEmpty()) {
                setSamplingSupportScale(Double.parseDouble(supportScale.trim()));
            }
        } catch (Exception ignored) {
            // 配置文件为可选能力：解析失败时保留当前值，不中断主流程。
        }
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

    public static ExclusiveSelectionResult selectExclusiveBestItemsetsWithStats(
            List<DocTerms> rows,
            int minSupport,
            int minItemsetSize,
            int maxItemsetSize,
            int maxCandidateCount
    ) {
        if (rows == null || rows.isEmpty()) {
            return emptyResult(maxCandidateCount);
        }

        SelectorConfig config = new SelectorConfig(
                minSupport,
                minItemsetSize,
                maxItemsetSize,
                maxCandidateCount
        );

        int[] allDocIds = collectDistinctDocIds(rows);
        int docCount = allDocIds.length;
        int sampleSize = Math.max(minSampleCount, (int) Math.ceil(docCount * sampleRatio));

        // Phase 1: 在全量数据上建索引
        // 使用 buildWithSupportBounds 过滤低频词，显著降低词汇量和内存
        // 当不需要过滤时（minSupport=1），退化到 build() 单次扫描
        TermTidsetIndex fullIndex = TermTidsetIndex.buildWithSupportBounds(
                rows, minSupport, adaptiveMaxDocCoverageRatio());
        List<byte[]> termVocabulary = fullIndex.getIdToTermUnsafe();
        if (termVocabulary.isEmpty()) {
            return emptyResult(maxCandidateCount);
        }
        List<BitSet> fullTidsets = fullIndex.getTidsetsByTermIdUnsafe();

        // 小样本集上采样收益很低且方差更高，直接走全量更稳。
        if (!samplingEnabled || sampleRatio <= 0.0d || minSupport <= 1 || docCount < 200 || sampleSize >= docCount) {
            // === 全量路径：直接在全量索引上挖掘 ===
            MiningPlan miningPlan = buildMiningPlan(config, docCount, termVocabulary.size());
            FrequentItemsetMiningResult miningStats = mineCandidates(fullTidsets, miningPlan);
            return processAfterMining(termVocabulary, miningStats, config, maxCandidateCount);
        }

        // === 采样路径 ===
        // 1) 随机选择采样文档的 docId
        int[] sampledDocIds = sampleDocIds(allDocIds, sampleSize);
        // 2) 创建采样掩码，从全量 tidsets 中位过滤出采样文档
        BitSet sampleMask = new BitSet(docCount);
        for (int sid : sampledDocIds) {
            sampleMask.set(sid);
        }
        // 3) 对每个 term 的 tidset 做位过滤（clone + and），产生采样 tidsets
        List<BitSet> sampledTidsets = new ArrayList<>(fullTidsets.size());
        for (BitSet full : fullTidsets) {
            BitSet s = (BitSet) full.clone();
            s.and(sampleMask);
            sampledTidsets.add(s);
        }
        // 4) minSupport 按采样比例缩放：采样集小，用更低阈值确保能挖到模式
        //    回算阶段会再用全量 minSupport 过滤，所以缩放不会引入虚假组合
        double observedSampleRatio = (double) sampleSize / (double) docCount;
        double effectiveScale = (samplingSupportScale <= 0.0d) ? observedSampleRatio : samplingSupportScale;
        int sampledMinSupport = Math.max(1, (int) Math.round(minSupport * effectiveScale));
        // 5) 在采样 tidsets 上挖掘
        SelectorConfig sampledConfig = new SelectorConfig(
                sampledMinSupport, minItemsetSize, maxItemsetSize, maxCandidateCount);
        MiningPlan sampledPlan = buildMiningPlan(sampledConfig, sampleSize, termVocabulary.size());
        FrequentItemsetMiningResult miningStats = mineCandidates(sampledTidsets, sampledPlan);
        List<CandidateItemset> minedCandidates = miningStats.getCandidates();
        if (minedCandidates.isEmpty()) {
            return buildResult(Collections.<SelectedGroup>emptyList(), miningStats, maxCandidateCount);
        }
        // 6) 回算：在全量 tidsets 上重新计算候选组合的完整 doclist
        //    挖掘阶段得到的 termIds 就是全量索引的 termId，无需映射
        List<CandidateItemset> recomputed = recomputeOnFullData(
                minedCandidates, fullTidsets, minSupport);
        if (recomputed.isEmpty()) {
            return buildResult(Collections.<SelectedGroup>emptyList(), miningStats, maxCandidateCount);
        }
        // 7) 互斥挑选
        List<CandidateItemset> selectedCandidates = pickExclusiveCandidates(
                recomputed, termVocabulary.size(), adaptiveMinNetGain());
        List<SelectedGroup> selectedGroups = toSelectedGroups(selectedCandidates, termVocabulary);
        return buildResult(selectedGroups, miningStats, maxCandidateCount);
    }

    /** 采样完成后处理候选：回算 doclist + 互斥挑选。 */
    private static ExclusiveSelectionResult processAfterMining(
            List<byte[]> termVocabulary,
            FrequentItemsetMiningResult miningStats,
            SelectorConfig config,
            int maxCandidateCount
    ) {
        List<CandidateItemset> minedCandidates = miningStats.getCandidates();
        if (minedCandidates.isEmpty()) {
            return buildResult(Collections.<SelectedGroup>emptyList(), miningStats, maxCandidateCount);
        }
        List<CandidateItemset> selectedCandidates = pickExclusiveCandidates(
                minedCandidates,
                termVocabulary.size(),
                adaptiveMinNetGain()
        );
        List<SelectedGroup> selectedGroups = toSelectedGroups(selectedCandidates, termVocabulary);
        return buildResult(selectedGroups, miningStats, maxCandidateCount);
    }

    /** 从 rows 中提取采样 docId 对应的文档子集，重建 DocTerms（保持 docId 不变）。 */
    private static List<DocTerms> extractSampledRows(List<DocTerms> rows, int[] sampledDocIds) {
        // 由于 DocTerms 不可变，无法直接拷贝，只能根据 docId 重新查找并重建
        // sampledDocIds 已排序，可以二分查找或使用 HashSet
        java.util.Set<Integer> sampledSet = new java.util.HashSet<>();
        for (int sid : sampledDocIds) sampledSet.add(sid);
        List<DocTerms> out = new ArrayList<>(sampledDocIds.length);
        for (DocTerms row : rows) {
            if (sampledSet.contains(row.getDocId())) {
                // 构造一个新的 DocTerms 副本
                // 注意：DocTerms 的构造器会校验参数，直接使用原始数据
                out.add(row);
            }
        }
        return out;
    }

    /**
     * 建立采样索引 termId → 全量索引 termId 的映射。
     * 使用 HashMap 加速查找（O(n) vs O(n^2) 线性扫描）。
     * @return 数组，下标为采样 termId，值为全量 termId（-1 表示未找到）
     */
    private static int[] buildTermIdMap(List<byte[]> sampledVocab, List<byte[]> fullVocab) {
        int[] map = new int[sampledVocab.size()];
        java.util.Arrays.fill(map, -1);
        // 构建全量词表的反向索引：词字节 → termId
        java.util.Map<cn.lxdb.plugins.muqingyu.fptoken.util.ByteArrayKey, Integer> fullMap =
            new java.util.HashMap<>(fullVocab.size());
        for (int j = 0; j < fullVocab.size(); j++) {
            fullMap.put(new cn.lxdb.plugins.muqingyu.fptoken.util.ByteArrayKey(fullVocab.get(j)), j);
        }
        // 用采样词查反向索引
        for (int i = 0; i < sampledVocab.size(); i++) {
            cn.lxdb.plugins.muqingyu.fptoken.util.ByteArrayKey key =
                new cn.lxdb.plugins.muqingyu.fptoken.util.ByteArrayKey(sampledVocab.get(i));
            Integer fullId = fullMap.get(key);
            if (fullId != null) {
                map[i] = fullId.intValue();
            }
        }
        return map;
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
     * 带 termId 映射的回算版本：采样 termId → 全量 termId → 在全量 tidsets 上做交集。
     */
    private static List<CandidateItemset> recomputeOnFullData(
            List<CandidateItemset> candidates,
            List<BitSet> fullTidsets,
            int[] sampleToFullMap,
            int minSupport
    ) {
        List<CandidateItemset> out = new ArrayList<>(candidates.size());
        BitSet scratch = new BitSet(64);
        for (CandidateItemset candidate : candidates) {
            int[] sampleTermIds = candidate.getTermIdsUnsafe();
            scratch.clear();
            int firstFullId = sampleToFullMap[sampleTermIds[0]];
            if (firstFullId < 0) continue;
            scratch.or(fullTidsets.get(firstFullId));
            boolean valid = true;
            for (int i = 1; i < sampleTermIds.length && valid; i++) {
                int fullId = sampleToFullMap[sampleTermIds[i]];
                if (fullId < 0) { valid = false; break; }
                scratch.and(fullTidsets.get(fullId));
                if (scratch.cardinality() < minSupport) { valid = false; break; }
            }
            if (valid) {
                int fullSupport = scratch.cardinality();
                if (fullSupport >= minSupport) {
                    BitSet docBits = (BitSet) scratch.clone();
                    out.add(CandidateItemset.trusted(sampleTermIds, docBits, fullSupport));
                }
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
                minNetGain
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
        return EngineTuningConfig.DEFAULT_MIN_NET_GAIN;
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
            List<byte[]> termVocabulary
    ) {
        int vocabularySize = termVocabulary.size();
        int selectedCount = selectedCandidates.size();
        List<SelectedGroup> out = new ArrayList<>(selectedCount);
        for (int selectedIndex = 0; selectedIndex < selectedCount; selectedIndex++) {
            CandidateItemset candidate = selectedCandidates.get(selectedIndex);
            int[] candidateTermIds = candidate.getTermIdsUnsafe();
            List<byte[]> terms = new ArrayList<>(candidateTermIds.length);
            for (int i = 0; i < candidateTermIds.length; i++) {
                int termId = candidateTermIds[i];
                assertTermIdWithinVocabulary(termId, vocabularySize);
                terms.add(ByteArrayUtils.copy(termVocabulary.get(termId)));
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
}
