package cn.lxdb.plugins.muqingyu.fptoken.api;

import cn.lxdb.plugins.muqingyu.fptoken.ExclusiveFrequentItemsetSelector;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.config.EngineTuningConfig;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.DocTerms;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.ExclusiveSelectionResult;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.SelectedGroup;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.util.ByteArrayKey;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.util.ByteArrayUtils;
import cn.lxdb.plugins.muqingyu.fptoken.runner.ngram.ByteNgramTokenizer;
import cn.lxdb.plugins.muqingyu.fptoken.runner.result.LineFileProcessingResult;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * 对外行记录处理 API：仅基于 rows 进行互斥频繁项集处理与派生数据生成。
 *
 * <p>参数约定：</p>
 * <ul>
 *   <li>{@code rows} 不可为 null；传入 null 会直接抛出 {@link NullPointerException}。</li>
 *   <li>若 {@code rows} 为空，返回空结果结构（不抛异常）。</li>
 *   <li>每个 {@link DocTerms} 不可为 null，且其 {@code docId} 应为非负整数（由底层选择器校验）。</li>
 *   <li>本 API 不会修改调用方传入的 rows；内部会先做防御性拷贝。</li>
 * </ul>
 *
 * <p><b>避免误用</b>：若上游传入的是“原始行字节”（每条只有 1 个 term），请使用带 n-gram 参数的入口
 * （如 {@link #processRowsWithNgram(List, int, int, int, int, int)}）或在
 * {@link ProcessingOptions} 中显式设置 n-gram 区间；否则会按默认 n-gram 配置执行切词。</p>
 */
public final class ExclusiveFpRowsProcessingApi {
    /**
     * 接口易用版入口：使用项目默认参数处理 rows。
     *
     * <p>默认参数来自 {@link EngineTuningConfig}：
     * minSupport/minItemsetSize/hotTermThreshold/ngram/skip-hash 区间统一走代码内默认值。
     * 适用于“先跑通流程、再逐步调参”的调用方式。</p>
     *
     * <p><b>限制说明</b>：{@code rows} 不可为 null；空列表会返回空结果。</p>
     */
    public static LineFileProcessingResult processRows(List<DocTerms> rows) {
        return processRows(rows, ProcessingOptions.defaults());
    }

    /**
     * 接口易用版入口：通过 {@link ProcessingOptions} 统一传参，减少重载选择成本。
     *
     * <p>该入口会对 options 做完整校验（支持度、项集长度、n-gram 区间、skip-hash 区间）。</p>
     *
     * <p><b>限制说明</b>：{@code rows}/{@code options} 均不可为 null。</p>
     */
    public static LineFileProcessingResult processRows(
            List<DocTerms> rows,
            ProcessingOptions options
    ) {
        Objects.requireNonNull(rows, "rows");
        ProcessingOptions resolved = Objects.requireNonNull(options, "options");
        validateProcessingOptions(resolved);
        return processRowsInternal(rows, resolved);
    }

    /**
     * 返回一份默认配置快照，调用方可按需覆盖后再传入 {@link #processRows(List, ProcessingOptions)}。
     */
    public static ProcessingOptions defaultOptions() {
        return ProcessingOptions.defaults();
    }

    /**
     * 返回“压缩优先”参数快照。
     *
     * <p>该档位会提升组合发现能力（更高采样、更大候选与项集上限），
     * 适合在 CPU 预算允许时追求更高的高频组合层占比。</p>
     */
    public static ProcessingOptions compressionFocusedOptions() {
        return ProcessingOptions.compressionFocusedDefaults();
    }


    private ExclusiveFpRowsProcessingApi() {
    }

    /**
     * 行级处理总入口（对外 API）。
     *
     * <p>该方法接收上游（如 LXDB）准备好的 {@code List<DocTerms>}，完成以下流程：</p>
     * <ol>
     *   <li>对输入 rows 做防御性复制，避免修改调用方对象。</li>
     *   <li>执行互斥高频项挖掘，得到 {@link ExclusiveSelectionResult}。</li>
     *   <li>基于同一批 rows 构建最终三层索引输入：
     *     <ul>
     *       <li>{@code highFreqMutexGroupPostings}：高频互斥组合倒排（一个 group 内多个 term 共享 docId 列表）</li>
     *       <li>{@code highFreqSingleTermPostings}：高频单 term 倒排</li>
     *       <li>{@code lowHitForwardRows}：排除高频层后剩余的低命中正排行（供 skip index 使用）</li>
     *     </ul>
     *   </li>
     * </ol>
     *
     * <p><b>建议使用方式</b>：</p>
     * <pre>{@code
     * LineFileProcessingResult result =
     *         ExclusiveFpRowsProcessingApi.processRows(rows, 80, 2, 16);
     *
     * LineFileProcessingResult.FinalIndexData finalData = result.getFinalIndexData();
     * List<SelectedGroup> groupPostings = finalData.getHighFreqMutexGroupPostings();
     * List<LineFileProcessingResult.HotTermDocList> termPostings = finalData.getHighFreqSingleTermPostings();
     * List<DocTerms> lowHitRows = finalData.getLowHitForwardRows();
     * }</pre>
     *
     * <p><b>参数说明</b>：</p>
     * <ul>
     *   <li>{@code rows}：输入文档行；每个 {@link DocTerms} 代表一条记录（docId + terms）。</li>
     *   <li>{@code minSupport}：最小支持度；越大结果越“保守”，候选规模更小。</li>
     *   <li>{@code minItemsetSize}：最小项集长度；通常建议 {@code >= 2}。</li>
     *   <li>{@code hotTermThresholdExclusive}：高频单 term 阈值（严格 {@code count > threshold} 才进入 highFreqSingleTermPostings）。</li>
     * </ul>
     *
     * <p><b>返回值说明</b>：</p>
     * <ul>
     *   <li>{@link LineFileProcessingResult#getFinalIndexData()} 是业务主出口（推荐优先使用）。</li>
     *   <li>{@link LineFileProcessingResult#getSelectionResult()} 提供挖掘统计与兼容访问。</li>
     *   <li>{@link LineFileProcessingResult#getLoadedRows()} 为处理时使用的输入快照。</li>
     * </ul>
     *
     * <p><b>注意事项</b>：</p>
     * <ul>
     *   <li>方法内部会设置采样参数（代码配置），无需依赖外部 properties 文件。</li>
     *   <li>若 rows 为空，会返回空结果结构；调用方可直接按空集合处理。</li>
     *   <li>若 rows 中存在 null 元素或负 docId，会在后续选择器阶段抛出参数异常。</li>
     * </ul>
     */
    public static LineFileProcessingResult processRows(
            List<DocTerms> rows,
            int minSupport,
            int minItemsetSize,
            int hotTermThresholdExclusive
    ) {
        return processRows(
                rows,
                ProcessingOptions.defaults()
                        .withMinSupport(minSupport)
                        .withMinItemsetSize(minItemsetSize)
                        .withHotTermThresholdExclusive(hotTermThresholdExclusive)
        );
    }

    /**
     * 行级处理总入口（可配置分词 n-gram 区间）。
     *
     * <p>当 rows 每条仅包含一段原始字节时，会在方法内部执行 n-gram 切割；
     * 当 rows 已经是多 term 形式时，保留其现状以兼容历史调用。</p>
     */
    public static LineFileProcessingResult processRowsWithNgram(
            List<DocTerms> rows,
            int ngramStart,
            int ngramEnd,
            int minSupport,
            int minItemsetSize,
            int hotTermThresholdExclusive
    ) {
        return processRows(
                rows,
                ProcessingOptions.defaults()
                        .withNgramRange(ngramStart, ngramEnd)
                        .withMinSupport(minSupport)
                        .withMinItemsetSize(minItemsetSize)
                        .withHotTermThresholdExclusive(hotTermThresholdExclusive)
        );
    }

    /**
     * 行级处理总入口（可配置 skip-index n-gram 区间）。
     *
     * <p>相比四参数版本，新增 {@code skipHashMinGram}/{@code skipHashMaxGram}，
     * 用于控制 skip-index BitSet 哈希层构建区间（例如 2~6）。</p>
     */
    public static LineFileProcessingResult processRows(
            List<DocTerms> rows,
            int minSupport,
            int minItemsetSize,
            int hotTermThresholdExclusive,
            int skipHashMinGram,
            int skipHashMaxGram
    ) {
        return processRows(
                rows,
                ProcessingOptions.defaults()
                        .withMinSupport(minSupport)
                        .withMinItemsetSize(minItemsetSize)
                        .withHotTermThresholdExclusive(hotTermThresholdExclusive)
                        .withSkipHashGramRange(skipHashMinGram, skipHashMaxGram)
        );
    }

    /**
     * 行级处理总入口（同时可配置分词 n-gram 区间与 skip-index 哈希层区间）。
     *
     * <p>该方法保留为兼容重载，内部统一委托到 {@link ProcessingOptions} 路径。</p>
     */
    public static LineFileProcessingResult processRowsWithNgramAndSkipHash(
            List<DocTerms> rows,
            int ngramStart,
            int ngramEnd,
            int minSupport,
            int minItemsetSize,
            int hotTermThresholdExclusive,
            int skipHashMinGram,
            int skipHashMaxGram
    ) {
        return processRows(
                rows,
                ProcessingOptions.defaults()
                        .withNgramRange(ngramStart, ngramEnd)
                        .withMinSupport(minSupport)
                        .withMinItemsetSize(minItemsetSize)
                        .withHotTermThresholdExclusive(hotTermThresholdExclusive)
                        .withSkipHashGramRange(skipHashMinGram, skipHashMaxGram)
        );
    }

    private static LineFileProcessingResult processRowsInternal(
            List<DocTerms> rows,
            ProcessingOptions options
    ) {
        applySamplingConfig(options);
        applySelectorTuningConfig(options);

        // 处理层不要修改调用方传入的 rows，这里先做一份防御性拷贝。
        List<DocTerms> loadedRows = copyRows(rows);
        List<DocTerms> tokenizedRows = tokenizeRowsForMining(
                loadedRows,
                options.getNgramStart(),
                options.getNgramEnd()
        );
        ExclusiveSelectionResult result = selectWithDefaultSelectorTuning(
                tokenizedRows,
                options.getMinSupport(),
                options.getMinItemsetSize(),
                options.getMaxItemsetSize(),
                options.getMaxCandidateCount()
        );
        LineFileProcessingResult.DerivedData derived =
                buildDerivedData(tokenizedRows, result, options.getHotTermThresholdExclusive());
        // 返回结构中会聚合出最终三元结果（新命名）：
        // highFreqMutexGroupPostings + highFreqSingleTermPostings + lowHitForwardRows。
        return new LineFileProcessingResult(
                loadedRows, result, derived,
                options.getSkipHashMinGram(),
                options.getSkipHashMaxGram()
        );
    }

    private static void validateProcessingOptions(ProcessingOptions options) {
        if (options.getMinSupport() <= 0) {
            throw new IllegalArgumentException("minSupport must be > 0");
        }
        if (options.getMinItemsetSize() <= 0) {
            throw new IllegalArgumentException("minItemsetSize must be > 0");
        }
        if (options.getMaxItemsetSize() <= 0) {
            throw new IllegalArgumentException("maxItemsetSize must be > 0");
        }
        if (options.getMaxItemsetSize() < options.getMinItemsetSize()) {
            throw new IllegalArgumentException("maxItemsetSize must be >= minItemsetSize");
        }
        if (options.getMaxCandidateCount() <= 0) {
            throw new IllegalArgumentException("maxCandidateCount must be > 0");
        }
        if (options.getSampleRatio() < 0.0d || options.getSampleRatio() > 1.0d) {
            throw new IllegalArgumentException("sampleRatio must be in [0,1]");
        }
        if (options.getMinSampleCount() <= 0) {
            throw new IllegalArgumentException("minSampleCount must be > 0");
        }
        if (options.getPickerMinNetGain() < 0) {
            throw new IllegalArgumentException("pickerMinNetGain must be >= 0");
        }
        if (options.getPickerEstimatedBytesPerTerm() <= 0) {
            throw new IllegalArgumentException("pickerEstimatedBytesPerTerm must be > 0");
        }
        if (options.getPickerCoverageRewardPerTerm() < 0) {
            throw new IllegalArgumentException("pickerCoverageRewardPerTerm must be >= 0");
        }
        validateNgramRange(options.getNgramStart(), options.getNgramEnd());
        validateSkipHashGramRange(options.getSkipHashMinGram(), options.getSkipHashMaxGram());
    }

    private static void validateSkipHashGramRange(int skipHashMinGram, int skipHashMaxGram) {
        if (skipHashMinGram < 2) {
            throw new IllegalArgumentException("skipHashMinGram must be >= 2");
        }
        if (skipHashMaxGram < skipHashMinGram) {
            throw new IllegalArgumentException("skipHashMaxGram must be >= skipHashMinGram");
        }
    }

    private static void applySamplingConfig(ProcessingOptions options) {
        // 代码内配置抽样参数（不依赖 properties 文件）
        ExclusiveFrequentItemsetSelector.setSampleRatio(options.getSampleRatio());
        ExclusiveFrequentItemsetSelector.setMinSampleCount(options.getMinSampleCount());
        ExclusiveFrequentItemsetSelector.setSamplingSupportScale(options.getSamplingSupportScale());
    }

    private static void applySelectorTuningConfig(ProcessingOptions options) {
        ExclusiveFrequentItemsetSelector.setPickerMinNetGain(options.getPickerMinNetGain());
        ExclusiveFrequentItemsetSelector.setPickerEstimatedBytesPerTerm(options.getPickerEstimatedBytesPerTerm());
        ExclusiveFrequentItemsetSelector.setPickerCoverageRewardPerTerm(options.getPickerCoverageRewardPerTerm());
    }

    private static ExclusiveSelectionResult selectWithDefaultSelectorTuning(
            List<DocTerms> rows,
            int minSupport,
            int minItemsetSize,
            int maxItemsetSize,
            int maxCandidateCount
    ) {
        return ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(
                rows,
                minSupport,
                minItemsetSize,
                maxItemsetSize,
                maxCandidateCount
        );
    }

    public static LineFileProcessingResult.DerivedData buildDerivedData(
            List<DocTerms> miningRows,
            ExclusiveSelectionResult result,
            int hotTermThresholdExclusive
    ) {
        // cutRes 基于输入行做浅结构复制，后续只移除词项，不改变 docId。
        List<DocTerms> cutRes = copyRows(miningRows);
        // hotTerms 在“原始 cutRes”上统计，语义是“先统计，再剔除 selected 词项”。
        List<LineFileProcessingResult.HotTermDocList> hotTerms = buildHotTerms(cutRes, hotTermThresholdExclusive);
        // selectedTerms 代表本轮已经被最终挑中的互斥项，必须从两个派生视图同步去掉。
        Set<ByteArrayKey> selectedTerms = collectSelectedTerms(result);
        List<DocTerms> filteredCutRes = removeSelectedTermsFromCutRes(cutRes, selectedTerms);
        List<LineFileProcessingResult.HotTermDocList> filteredHotTerms =
                removeSelectedTermsFromHotTerms(hotTerms, selectedTerms);
        return new LineFileProcessingResult.DerivedData(
                hotTermThresholdExclusive, filteredCutRes, filteredHotTerms);
    }

    private static List<DocTerms> copyRows(List<DocTerms> rows) {
        List<DocTerms> out = new ArrayList<DocTerms>(rows.size());
        for (DocTerms row : rows) {
            out.add(new DocTerms(row.getDocId(), row.getTermsUnsafe()));
        }
        return out;
    }

    private static List<DocTerms> tokenizeRowsForMining(
            List<DocTerms> rows,
            int ngramStart,
            int ngramEnd
    ) {
        List<DocTerms> out = new ArrayList<DocTerms>(rows.size());
        for (DocTerms row : rows) {
            List<byte[]> terms = row.getTermsUnsafe();
            if (terms.size() == 1) {
                out.add(new DocTerms(
                        row.getDocId(),
                        ByteNgramTokenizer.tokenize(terms.get(0), ngramStart, ngramEnd)));
            } else {
                // 兼容历史：如果调用方已提供 term 列表，则不重复切词。
                out.add(new DocTerms(row.getDocId(), terms));
            }
        }
        return out;
    }

    private static void validateNgramRange(int ngramStart, int ngramEnd) {
        if (ngramStart <= 0) {
            throw new IllegalArgumentException("ngramStart must be > 0");
        }
        if (ngramEnd < ngramStart) {
            throw new IllegalArgumentException("ngramEnd must be >= ngramStart");
        }
    }

    private static List<LineFileProcessingResult.HotTermDocList> buildHotTerms(
            List<DocTerms> rows,
            int hotTermThresholdExclusive
    ) {
        // LinkedHashSet 保证 docId 去重且保持首次出现顺序，便于稳定回放与测试断言。
        Map<ByteArrayKey, LinkedHashSet<Integer>> termToDocs = new LinkedHashMap<ByteArrayKey, LinkedHashSet<Integer>>();
        for (DocTerms row : rows) {
            int docId = row.getDocId();
            for (byte[] term : row.getTermsUnsafe()) {
                int hash = ByteArrayUtils.hash(term);
                ByteArrayKey lookupKey = ByteArrayKey.forLookup(term, hash);
                LinkedHashSet<Integer> docs = termToDocs.get(lookupKey);
                if (docs == null) {
                    docs = new LinkedHashSet<Integer>();
                    termToDocs.put(new ByteArrayKey(term), docs);
                }
                docs.add(docId);
            }
        }

        List<LineFileProcessingResult.HotTermDocList> out =
                new ArrayList<LineFileProcessingResult.HotTermDocList>(termToDocs.size());
        for (Map.Entry<ByteArrayKey, LinkedHashSet<Integer>> entry : termToDocs.entrySet()) {
            List<Integer> docIds = new ArrayList<Integer>(entry.getValue());
            // 阈值语义是严格大于（count > threshold）。
            if (docIds.size() > hotTermThresholdExclusive) {
                out.add(new LineFileProcessingResult.HotTermDocList(entry.getKey().bytes(), docIds));
            }
        }
        // 展示层排序：先按出现文档数降序，再按字节序升序保证同 count 稳定顺序。
        Collections.sort(out, new Comparator<LineFileProcessingResult.HotTermDocList>() {
            @Override
            public int compare(
                    LineFileProcessingResult.HotTermDocList a,
                    LineFileProcessingResult.HotTermDocList b
            ) {
                int c = b.getCount() - a.getCount();
                if (c != 0) {
                    return c;
                }
                return ByteArrayUtils.compareUnsigned(a.getTerm(), b.getTerm());
            }
        });
        return out;
    }

    private static Set<ByteArrayKey> collectSelectedTerms(ExclusiveSelectionResult result) {
        Set<ByteArrayKey> out = new LinkedHashSet<ByteArrayKey>();
        for (SelectedGroup g : result.getGroups()) {
            for (byte[] term : g.getTerms()) {
                int hash = ByteArrayUtils.hash(term);
                ByteArrayKey lookupKey = ByteArrayKey.forLookup(term, hash);
                if (!out.contains(lookupKey)) {
                    out.add(new ByteArrayKey(term));
                }
            }
        }
        return out;
    }

    private static List<DocTerms> removeSelectedTermsFromCutRes(
            List<DocTerms> cutRes,
            Set<ByteArrayKey> selectedTerms
    ) {
        // 空集走直返路径，避免不必要分配。
        if (selectedTerms.isEmpty()) {
            return cutRes;
        }
        List<DocTerms> out = new ArrayList<DocTerms>(cutRes.size());
        for (DocTerms row : cutRes) {
            List<byte[]> rowTerms = row.getTermsUnsafe();
            List<byte[]> filtered = new ArrayList<byte[]>(rowTerms.size());
            for (byte[] term : rowTerms) {
                int hash = ByteArrayUtils.hash(term);
                ByteArrayKey lookupKey = ByteArrayKey.forLookup(term, hash);
                if (!selectedTerms.contains(lookupKey)) {
                    filtered.add(term);
                }
            }
            out.add(new DocTerms(row.getDocId(), filtered));
        }
        return out;
    }

    private static List<LineFileProcessingResult.HotTermDocList> removeSelectedTermsFromHotTerms(
            List<LineFileProcessingResult.HotTermDocList> hotTerms,
            Set<ByteArrayKey> selectedTerms
    ) {
        // 空集走直返路径，避免不必要分配。
        if (selectedTerms.isEmpty()) {
            return hotTerms;
        }
        List<LineFileProcessingResult.HotTermDocList> out =
                new ArrayList<LineFileProcessingResult.HotTermDocList>(hotTerms.size());
        for (LineFileProcessingResult.HotTermDocList item : hotTerms) {
            byte[] term = item.getTerm();
            int hash = ByteArrayUtils.hash(term);
            ByteArrayKey lookupKey = ByteArrayKey.forLookup(term, hash);
            if (!selectedTerms.contains(lookupKey)) {
                out.add(item);
            }
        }
        return out;
    }

    /**
     * 中间步骤测试入口：用于对 copy/tokenize/derive 各步骤做独立断言。
     *
     * <p>仅做方法透传，不改变任何业务语义。</p>
     */
    public static final class IntermediateSteps {
        private IntermediateSteps() {
        }

        public static List<DocTerms> copyRows(List<DocTerms> rows) {
            return ExclusiveFpRowsProcessingApi.copyRows(rows);
        }

        public static List<DocTerms> tokenizeRowsForMining(
                List<DocTerms> rows,
                int ngramStart,
                int ngramEnd
        ) {
            return ExclusiveFpRowsProcessingApi.tokenizeRowsForMining(rows, ngramStart, ngramEnd);
        }

        public static List<LineFileProcessingResult.HotTermDocList> buildHotTerms(
                List<DocTerms> rows,
                int hotTermThresholdExclusive
        ) {
            return ExclusiveFpRowsProcessingApi.buildHotTerms(rows, hotTermThresholdExclusive);
        }

        public static Set<ByteArrayKey> collectSelectedTerms(ExclusiveSelectionResult result) {
            return ExclusiveFpRowsProcessingApi.collectSelectedTerms(result);
        }

        public static List<DocTerms> removeSelectedTermsFromCutRes(
                List<DocTerms> cutRes,
                Set<ByteArrayKey> selectedTerms
        ) {
            return ExclusiveFpRowsProcessingApi.removeSelectedTermsFromCutRes(cutRes, selectedTerms);
        }

        public static List<LineFileProcessingResult.HotTermDocList> removeSelectedTermsFromHotTerms(
                List<LineFileProcessingResult.HotTermDocList> hotTerms,
                Set<ByteArrayKey> selectedTerms
        ) {
            return ExclusiveFpRowsProcessingApi.removeSelectedTermsFromHotTerms(hotTerms, selectedTerms);
        }

        public static void validateProcessingOptions(ProcessingOptions options) {
            ExclusiveFpRowsProcessingApi.validateProcessingOptions(options);
        }
    }

    /**
     * 统一处理参数对象：
     * 既支持“全默认”，也支持链式覆盖，减少重载心智负担。
     *
     * <p>本对象是不可变对象；每个 withXxx() 都会返回新的配置实例。</p>
     */
    public static final class ProcessingOptions {
        private final int minSupport;
        private final int minItemsetSize;
        private final int maxItemsetSize;
        private final int maxCandidateCount;
        private final int hotTermThresholdExclusive;
        private final int ngramStart;
        private final int ngramEnd;
        private final int skipHashMinGram;
        private final int skipHashMaxGram;
        private final double sampleRatio;
        private final int minSampleCount;
        private final double samplingSupportScale;
        private final int pickerMinNetGain;
        private final int pickerEstimatedBytesPerTerm;
        private final int pickerCoverageRewardPerTerm;

        private ProcessingOptions(
                int minSupport,
                int minItemsetSize,
                int maxItemsetSize,
                int maxCandidateCount,
                int hotTermThresholdExclusive,
                int ngramStart,
                int ngramEnd,
                int skipHashMinGram,
                int skipHashMaxGram,
                double sampleRatio,
                int minSampleCount,
                double samplingSupportScale,
                int pickerMinNetGain,
                int pickerEstimatedBytesPerTerm,
                int pickerCoverageRewardPerTerm
        ) {
            this.minSupport = minSupport;
            this.minItemsetSize = minItemsetSize;
            this.maxItemsetSize = maxItemsetSize;
            this.maxCandidateCount = maxCandidateCount;
            this.hotTermThresholdExclusive = hotTermThresholdExclusive;
            this.ngramStart = ngramStart;
            this.ngramEnd = ngramEnd;
            this.skipHashMinGram = skipHashMinGram;
            this.skipHashMaxGram = skipHashMaxGram;
            this.sampleRatio = sampleRatio;
            this.minSampleCount = minSampleCount;
            this.samplingSupportScale = samplingSupportScale;
            this.pickerMinNetGain = pickerMinNetGain;
            this.pickerEstimatedBytesPerTerm = pickerEstimatedBytesPerTerm;
            this.pickerCoverageRewardPerTerm = pickerCoverageRewardPerTerm;
        }

        public static ProcessingOptions defaults() {
            return new ProcessingOptions(
                    EngineTuningConfig.DEFAULT_RUNNER_MIN_SUPPORT,
                    EngineTuningConfig.DEFAULT_RUNNER_MIN_ITEMSET_SIZE,
                    EngineTuningConfig.DEFAULT_MAX_ITEMSET_SIZE,
                    EngineTuningConfig.DEFAULT_MAX_CANDIDATE_COUNT,
                    EngineTuningConfig.DEFAULT_HOT_TERM_THRESHOLD_EXCLUSIVE,
                    EngineTuningConfig.DEFAULT_NGRAM_START,
                    EngineTuningConfig.DEFAULT_NGRAM_END,
                    EngineTuningConfig.DEFAULT_SKIP_HASH_MIN_GRAM,
                    EngineTuningConfig.DEFAULT_SKIP_HASH_MAX_GRAM,
                    EngineTuningConfig.DEFAULT_SAMPLE_RATIO,
                    EngineTuningConfig.DEFAULT_MIN_SAMPLE_COUNT,
                    EngineTuningConfig.DEFAULT_SAMPLING_SUPPORT_SCALE,
                    EngineTuningConfig.PICKER_DEFAULT_MIN_NET_GAIN,
                    EngineTuningConfig.PICKER_ESTIMATED_BYTES_PER_TERM,
                    EngineTuningConfig.PICKER_DEFAULT_COVERAGE_REWARD_PER_TERM
            );
        }

        public static ProcessingOptions compressionFocusedDefaults() {
            return new ProcessingOptions(
                    EngineTuningConfig.DEFAULT_RUNNER_MIN_SUPPORT,
                    EngineTuningConfig.DEFAULT_RUNNER_MIN_ITEMSET_SIZE,
                    EngineTuningConfig.COMPRESSION_FOCUSED_MAX_ITEMSET_SIZE,
                    EngineTuningConfig.COMPRESSION_FOCUSED_MAX_CANDIDATE_COUNT,
                    EngineTuningConfig.DEFAULT_HOT_TERM_THRESHOLD_EXCLUSIVE,
                    EngineTuningConfig.DEFAULT_NGRAM_START,
                    EngineTuningConfig.DEFAULT_NGRAM_END,
                    EngineTuningConfig.DEFAULT_SKIP_HASH_MIN_GRAM,
                    EngineTuningConfig.DEFAULT_SKIP_HASH_MAX_GRAM,
                    EngineTuningConfig.COMPRESSION_FOCUSED_SAMPLE_RATIO,
                    EngineTuningConfig.COMPRESSION_FOCUSED_MIN_SAMPLE_COUNT,
                    EngineTuningConfig.COMPRESSION_FOCUSED_SAMPLING_SUPPORT_SCALE,
                    EngineTuningConfig.COMPRESSION_FOCUSED_PICKER_MIN_NET_GAIN,
                    EngineTuningConfig.COMPRESSION_FOCUSED_PICKER_ESTIMATED_BYTES_PER_TERM,
                    EngineTuningConfig.COMPRESSION_FOCUSED_PICKER_COVERAGE_REWARD_PER_TERM
            );
        }

        public ProcessingOptions withMinSupport(int value) {
            return new ProcessingOptions(
                    value, minItemsetSize, maxItemsetSize, maxCandidateCount, hotTermThresholdExclusive,
                    ngramStart, ngramEnd, skipHashMinGram, skipHashMaxGram,
                    sampleRatio, minSampleCount, samplingSupportScale,
                    pickerMinNetGain, pickerEstimatedBytesPerTerm, pickerCoverageRewardPerTerm
            );
        }

        public ProcessingOptions withMinItemsetSize(int value) {
            return new ProcessingOptions(
                    minSupport, value, maxItemsetSize, maxCandidateCount, hotTermThresholdExclusive,
                    ngramStart, ngramEnd, skipHashMinGram, skipHashMaxGram,
                    sampleRatio, minSampleCount, samplingSupportScale,
                    pickerMinNetGain, pickerEstimatedBytesPerTerm, pickerCoverageRewardPerTerm
            );
        }

        public ProcessingOptions withMaxItemsetSize(int value) {
            return new ProcessingOptions(
                    minSupport, minItemsetSize, value, maxCandidateCount, hotTermThresholdExclusive,
                    ngramStart, ngramEnd, skipHashMinGram, skipHashMaxGram,
                    sampleRatio, minSampleCount, samplingSupportScale,
                    pickerMinNetGain, pickerEstimatedBytesPerTerm, pickerCoverageRewardPerTerm
            );
        }

        public ProcessingOptions withMaxCandidateCount(int value) {
            return new ProcessingOptions(
                    minSupport, minItemsetSize, maxItemsetSize, value, hotTermThresholdExclusive,
                    ngramStart, ngramEnd, skipHashMinGram, skipHashMaxGram,
                    sampleRatio, minSampleCount, samplingSupportScale,
                    pickerMinNetGain, pickerEstimatedBytesPerTerm, pickerCoverageRewardPerTerm
            );
        }

        public ProcessingOptions withHotTermThresholdExclusive(int value) {
            return new ProcessingOptions(
                    minSupport, minItemsetSize, maxItemsetSize, maxCandidateCount, value,
                    ngramStart, ngramEnd, skipHashMinGram, skipHashMaxGram,
                    sampleRatio, minSampleCount, samplingSupportScale,
                    pickerMinNetGain, pickerEstimatedBytesPerTerm, pickerCoverageRewardPerTerm
            );
        }

        public ProcessingOptions withNgramRange(int start, int end) {
            return new ProcessingOptions(
                    minSupport, minItemsetSize, maxItemsetSize, maxCandidateCount, hotTermThresholdExclusive,
                    start, end, skipHashMinGram, skipHashMaxGram,
                    sampleRatio, minSampleCount, samplingSupportScale,
                    pickerMinNetGain, pickerEstimatedBytesPerTerm, pickerCoverageRewardPerTerm
            );
        }

        public ProcessingOptions withSkipHashGramRange(int minGram, int maxGram) {
            return new ProcessingOptions(
                    minSupport, minItemsetSize, maxItemsetSize, maxCandidateCount, hotTermThresholdExclusive,
                    ngramStart, ngramEnd, minGram, maxGram,
                    sampleRatio, minSampleCount, samplingSupportScale,
                    pickerMinNetGain, pickerEstimatedBytesPerTerm, pickerCoverageRewardPerTerm
            );
        }

        public ProcessingOptions withSampleRatio(double value) {
            return new ProcessingOptions(
                    minSupport, minItemsetSize, maxItemsetSize, maxCandidateCount, hotTermThresholdExclusive,
                    ngramStart, ngramEnd, skipHashMinGram, skipHashMaxGram,
                    value, minSampleCount, samplingSupportScale,
                    pickerMinNetGain, pickerEstimatedBytesPerTerm, pickerCoverageRewardPerTerm
            );
        }

        public ProcessingOptions withMinSampleCount(int value) {
            return new ProcessingOptions(
                    minSupport, minItemsetSize, maxItemsetSize, maxCandidateCount, hotTermThresholdExclusive,
                    ngramStart, ngramEnd, skipHashMinGram, skipHashMaxGram,
                    sampleRatio, value, samplingSupportScale,
                    pickerMinNetGain, pickerEstimatedBytesPerTerm, pickerCoverageRewardPerTerm
            );
        }

        public ProcessingOptions withSamplingSupportScale(double value) {
            return new ProcessingOptions(
                    minSupport, minItemsetSize, maxItemsetSize, maxCandidateCount, hotTermThresholdExclusive,
                    ngramStart, ngramEnd, skipHashMinGram, skipHashMaxGram,
                    sampleRatio, minSampleCount, value,
                    pickerMinNetGain, pickerEstimatedBytesPerTerm, pickerCoverageRewardPerTerm
            );
        }

        public ProcessingOptions withPickerMinNetGain(int value) {
            return new ProcessingOptions(
                    minSupport, minItemsetSize, maxItemsetSize, maxCandidateCount, hotTermThresholdExclusive,
                    ngramStart, ngramEnd, skipHashMinGram, skipHashMaxGram,
                    sampleRatio, minSampleCount, samplingSupportScale,
                    value, pickerEstimatedBytesPerTerm, pickerCoverageRewardPerTerm
            );
        }

        public ProcessingOptions withPickerEstimatedBytesPerTerm(int value) {
            return new ProcessingOptions(
                    minSupport, minItemsetSize, maxItemsetSize, maxCandidateCount, hotTermThresholdExclusive,
                    ngramStart, ngramEnd, skipHashMinGram, skipHashMaxGram,
                    sampleRatio, minSampleCount, samplingSupportScale,
                    pickerMinNetGain, value, pickerCoverageRewardPerTerm
            );
        }

        public ProcessingOptions withPickerCoverageRewardPerTerm(int value) {
            return new ProcessingOptions(
                    minSupport, minItemsetSize, maxItemsetSize, maxCandidateCount, hotTermThresholdExclusive,
                    ngramStart, ngramEnd, skipHashMinGram, skipHashMaxGram,
                    sampleRatio, minSampleCount, samplingSupportScale,
                    pickerMinNetGain, pickerEstimatedBytesPerTerm, value
            );
        }

        public int getMinSupport() {
            return minSupport;
        }

        public int getMinItemsetSize() {
            return minItemsetSize;
        }

        public int getMaxItemsetSize() {
            return maxItemsetSize;
        }

        public int getMaxCandidateCount() {
            return maxCandidateCount;
        }

        public int getHotTermThresholdExclusive() {
            return hotTermThresholdExclusive;
        }

        public int getNgramStart() {
            return ngramStart;
        }

        public int getNgramEnd() {
            return ngramEnd;
        }

        public int getSkipHashMinGram() {
            return skipHashMinGram;
        }

        public int getSkipHashMaxGram() {
            return skipHashMaxGram;
        }

        public double getSampleRatio() {
            return sampleRatio;
        }

        public int getMinSampleCount() {
            return minSampleCount;
        }

        public double getSamplingSupportScale() {
            return samplingSupportScale;
        }

        public int getPickerMinNetGain() {
            return pickerMinNetGain;
        }

        public int getPickerEstimatedBytesPerTerm() {
            return pickerEstimatedBytesPerTerm;
        }

        public int getPickerCoverageRewardPerTerm() {
            return pickerCoverageRewardPerTerm;
        }
    }
}
