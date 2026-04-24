package cn.lxdb.plugins.muqingyu.fptoken.api;

import cn.lxdb.plugins.muqingyu.fptoken.ExclusiveFrequentItemsetSelector;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.config.EngineTuningConfig;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.ByteRef;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.DocTerms;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.ExclusiveSelectionResult;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.SelectedGroup;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.util.ByteArrayKey;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.util.ByteArrayUtils;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.util.OpenHashTable;
import cn.lxdb.plugins.muqingyu.fptoken.runner.ngram.ByteNgramTokenizer;
import cn.lxdb.plugins.muqingyu.fptoken.runner.result.LineFileProcessingResult;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.atomic.LongAdder;

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
    private static final double ONE_OFF_HINT_SUPPORT_MULTIPLIER = 1.5d;
    private static final int STABLE_HINT_REPEAT_THRESHOLD = 2;
    private static final int MAX_HINT_QUALITY_SCORE = 8;
    private static final String AUTO_HINTS_WHEN_MISSING_PROPERTY =
            "fptoken.premerge.autoHintsWhenMissing";
    private static final int AUTO_HINT_MAX_FREQUENT_TERMS = 12;
    private static final int AUTO_HINT_MAX_PAIR_HINTS = 16;
    private static final double SINGLE_HINT_SUPPORT_MULTIPLIER_WHEN_MUTEX_EXISTS = 1.5d;
    private static final int MIN_SINGLE_HINTS_WHEN_MUTEX_EXISTS = 16;
    private static final int SINGLE_HINTS_PER_MUTEX_HINT = 2;
    private static final int PREMERGE_HINT_AGGREGATE_CACHE_SLOTS = 64;
    private static final AtomicReferenceArray<HintAggregateCacheEntry> PREMERGE_HINT_AGGREGATE_CACHE =
            new AtomicReferenceArray<>(PREMERGE_HINT_AGGREGATE_CACHE_SLOTS);
    private static final LongAdder PREMERGE_HINT_CACHE_HITS = new LongAdder();
    private static final LongAdder PREMERGE_HINT_CACHE_MISSES = new LongAdder();
    private static final LongAdder PREMERGE_HINT_CACHE_CLEANUPS = new LongAdder();
    private static final AtomicLong PREMERGE_HINT_CACHE_OPS = new AtomicLong(0L);
    private static final int PREMERGE_HINT_CACHE_HYGIENE_INTERVAL = 256;
    private static final int PREMERGE_HINT_CACHE_MISS_TO_HIT_RATIO_FOR_CLEANUP = 4;
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
     *   <li>方法内部会把 {@link ProcessingOptions} 转为请求级调参并传给 selector，不修改全局静态参数。</li>
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
        // 处理层不要修改调用方传入的 rows，这里先做一份防御性拷贝。
        List<DocTerms> loadedRows = copyRows(rows);
        List<DocTerms> tokenizedRows = tokenizeRowsForMining(
                loadedRows,
                options.getNgramStart(),
                options.getNgramEnd()
        );
        List<ExclusiveFrequentItemsetSelector.PremergeHintCandidate> selectorPremergeHints =
                buildSelectorPremergeHints(options, tokenizedRows, options.getMinSupport());
        ExclusiveSelectionResult result = selectWithDefaultSelectorTuning(
                tokenizedRows,
                options,
                selectorPremergeHints,
                options.getHintBoostWeight()
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
        if (options.getHintBoostWeight() < 0) {
            throw new IllegalArgumentException("hintBoostWeight must be >= 0");
        }
        validatePremergeHints(
                options.getPremergeMutexGroupHints(),
                "premergeMutexGroupHints",
                options.getHintValidationMode());
        validatePremergeHints(
                options.getPremergeSingleTermHints(),
                "premergeSingleTermHints",
                options.getHintValidationMode());
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

    private static void validatePremergeHints(
            List<PremergeHint> hints,
            String listLabel,
            HintValidationMode mode
    ) {
        if (hints == null || hints.isEmpty()) {
            return;
        }
        for (int i = 0; i < hints.size(); i++) {
            PremergeHint hint = hints.get(i);
            if (hint == null) {
                if (mode == HintValidationMode.STRICT) {
                    throw new IllegalArgumentException(listLabel + "[" + i + "] must not be null");
                }
                continue;
            }
            if (hint.getTermRefs().isEmpty() && mode == HintValidationMode.STRICT) {
                throw new IllegalArgumentException(listLabel + "[" + i + "].termRefs must not be empty");
            }
        }
    }

    private static List<ExclusiveFrequentItemsetSelector.PremergeHintCandidate> buildSelectorPremergeHints(
            ProcessingOptions options,
            List<DocTerms> currentRows,
            int minSupport
    ) {
        if (options.getPremergeMutexGroupHints().isEmpty() && options.getPremergeSingleTermHints().isEmpty()) {
            return Collections.emptyList();
        }
        List<HintAggregate> aggregated = getOrBuildHintAggregates(options, currentRows, minSupport);
        if (aggregated.isEmpty()) {
            return Collections.emptyList();
        }
        return filterFreshHints(aggregated, currentRows, minSupport);
    }

    private static List<HintAggregate> getOrBuildHintAggregates(
            ProcessingOptions options,
            List<DocTerms> currentRows,
            int minSupport
    ) {
        List<PremergeHint> mutexHints = options.getPremergeMutexGroupHints();
        List<PremergeHint> singleHints = options.getPremergeSingleTermHints();
        if ((mutexHints == null || mutexHints.isEmpty())
                && (singleHints == null || singleHints.isEmpty())
                && isAutoHintsWhenMissingEnabled()) {
            return buildAutoGeneratedHintAggregates(currentRows, minSupport);
        }
        int keyHash = premergeHintCacheKeyHash(mutexHints, singleHints, options.getHintValidationMode());
        int slot = keyHash & (PREMERGE_HINT_AGGREGATE_CACHE_SLOTS - 1);
        HintAggregateCacheEntry entry = PREMERGE_HINT_AGGREGATE_CACHE.get(slot);
        if (entry != null && entry.matches(mutexHints, singleHints, options.getHintValidationMode(), keyHash)) {
            PREMERGE_HINT_CACHE_HITS.increment();
            maybeHygieneCleanupPremergeHintCache();
            return entry.aggregates;
        }
        PREMERGE_HINT_CACHE_MISSES.increment();
        List<HintAggregate> built = buildHintAggregates(options, mutexHints, singleHints);
        PREMERGE_HINT_AGGREGATE_CACHE.set(
                slot,
                new HintAggregateCacheEntry(keyHash, options.getHintValidationMode(), mutexHints, singleHints, built)
        );
        maybeHygieneCleanupPremergeHintCache();
        return built;
    }

    private static void maybeHygieneCleanupPremergeHintCache() {
        long ops = PREMERGE_HINT_CACHE_OPS.incrementAndGet();
        if ((ops & (PREMERGE_HINT_CACHE_HYGIENE_INTERVAL - 1)) != 0L) {
            return;
        }
        long hits = PREMERGE_HINT_CACHE_HITS.sum();
        long misses = PREMERGE_HINT_CACHE_MISSES.sum();
        if (misses < PREMERGE_HINT_CACHE_HYGIENE_INTERVAL) {
            return;
        }
        // Misses dominantly higher than hits indicates stale/low-reuse cache content.
        if (misses >= (hits + 1L) * PREMERGE_HINT_CACHE_MISS_TO_HIT_RATIO_FOR_CLEANUP) {
            clearPremergeHintAggregateCacheInternal();
        }
    }

    private static void clearPremergeHintAggregateCacheInternal() {
        for (int i = 0; i < PREMERGE_HINT_AGGREGATE_CACHE_SLOTS; i++) {
            PREMERGE_HINT_AGGREGATE_CACHE.set(i, null);
        }
        PREMERGE_HINT_CACHE_HITS.reset();
        PREMERGE_HINT_CACHE_MISSES.reset();
        PREMERGE_HINT_CACHE_OPS.set(0L);
        PREMERGE_HINT_CACHE_CLEANUPS.increment();
    }

    private static List<HintAggregate> buildHintAggregates(
            ProcessingOptions options,
            List<PremergeHint> mutexHints,
            List<PremergeHint> singleHints
    ) {
        LinkedHashMap<HintTermKey, HintAggregate> aggregated = new LinkedHashMap<>();
        appendPremergeHintCandidates(options, mutexHints, "premergeMutexGroupHints", aggregated);
        appendPremergeHintCandidates(options, singleHints, "premergeSingleTermHints", aggregated);
        return Collections.unmodifiableList(new ArrayList<>(aggregated.values()));
    }

    private static List<HintAggregate> buildAutoGeneratedHintAggregates(List<DocTerms> currentRows, int minSupport) {
        if (currentRows.isEmpty()) {
            return Collections.emptyList();
        }
        OpenHashTable termTable = new OpenHashTable();
        List<Integer> termSupports = new ArrayList<>();
        List<byte[]> termBytes = new ArrayList<>();
        for (int i = 0; i < currentRows.size(); i++) {
            List<ByteRef> terms = currentRows.get(i).getTermRefsUnsafe();
            OpenHashTable rowSeen = new OpenHashTable(Math.max(8, terms.size() * 2 + 1));
            for (int t = 0; t < terms.size(); t++) {
                ByteRef ref = terms.get(t);
                int hash = ByteArrayUtils.hash(ref.getSourceUnsafe(), ref.getOffset(), ref.getLength());
                if (rowSeen.get(ref, hash) >= 0) {
                    continue;
                }
                rowSeen.getOrPut(ref, hash, true);
                int termId = termTable.getOrPut(ref, hash, true);
                while (termSupports.size() <= termId) {
                    termSupports.add(Integer.valueOf(0));
                    termBytes.add(null);
                }
                termSupports.set(termId, Integer.valueOf(termSupports.get(termId).intValue() + 1));
                if (termBytes.get(termId) == null) {
                    termBytes.set(termId, ref.copyBytes());
                }
            }
        }
        List<TermSupport> frequent = new ArrayList<>();
        for (int termId = 0; termId < termSupports.size(); termId++) {
            int support = termSupports.get(termId).intValue();
            if (support >= minSupport && termBytes.get(termId) != null) {
                frequent.add(new TermSupport(termId, support));
            }
        }
        if (frequent.size() < 2) {
            return Collections.emptyList();
        }
        Collections.sort(frequent, (a, b) -> Integer.compare(b.support, a.support));
        int topN = Math.min(AUTO_HINT_MAX_FREQUENT_TERMS, frequent.size());
        int[] topTermIds = new int[topN];
        for (int i = 0; i < topN; i++) {
            topTermIds[i] = frequent.get(i).termId;
        }
        int[][] pairSupport = new int[topN][topN];
        for (int i = 0; i < currentRows.size(); i++) {
            boolean[] present = new boolean[topN];
            List<ByteRef> terms = currentRows.get(i).getTermRefsUnsafe();
            for (int t = 0; t < terms.size(); t++) {
                ByteRef ref = terms.get(t);
                int hash = ByteArrayUtils.hash(ref.getSourceUnsafe(), ref.getOffset(), ref.getLength());
                int termId = termTable.get(ref, hash);
                if (termId < 0) {
                    continue;
                }
                for (int k = 0; k < topN; k++) {
                    if (topTermIds[k] == termId) {
                        present[k] = true;
                        break;
                    }
                }
            }
            for (int a = 0; a < topN; a++) {
                if (!present[a]) {
                    continue;
                }
                for (int b = a + 1; b < topN; b++) {
                    if (present[b]) {
                        pairSupport[a][b]++;
                    }
                }
            }
        }
        List<PairSupport> pairs = new ArrayList<>();
        int pairMinSupport = Math.max(2, minSupport);
        for (int a = 0; a < topN; a++) {
            for (int b = a + 1; b < topN; b++) {
                if (pairSupport[a][b] >= pairMinSupport) {
                    pairs.add(new PairSupport(a, b, pairSupport[a][b]));
                }
            }
        }
        if (pairs.isEmpty()) {
            return Collections.emptyList();
        }
        Collections.sort(pairs, (x, y) -> Integer.compare(y.support, x.support));
        int limit = Math.min(AUTO_HINT_MAX_PAIR_HINTS, pairs.size());
        List<HintAggregate> out = new ArrayList<>(limit);
        for (int i = 0; i < limit; i++) {
            PairSupport p = pairs.get(i);
            List<ByteRef> refs = new ArrayList<>(2);
            byte[] left = termBytes.get(topTermIds[p.leftIdx]);
            byte[] right = termBytes.get(topTermIds[p.rightIdx]);
            refs.add(new ByteRef(left, 0, left.length));
            refs.add(new ByteRef(right, 0, right.length));
            out.add(new HintAggregate(ByteArrayUtils.normalizeTermRefs(refs), 2));
        }
        return Collections.unmodifiableList(out);
    }

    private static void appendPremergeHintCandidates(
            ProcessingOptions options,
            List<PremergeHint> hints,
            String listLabel,
            LinkedHashMap<HintTermKey, HintAggregate> aggregated
    ) {
        for (int i = 0; i < hints.size(); i++) {
            PremergeHint hint = hints.get(i);
            if (hint == null) {
                if (options.getHintValidationMode() == HintValidationMode.STRICT) {
                    throw new IllegalArgumentException(listLabel + "[" + i + "] must not be null");
                }
                continue;
            }
            List<ByteRef> refs = normalizeHintTermRefs(hint.getTermRefs());
            if (refs.isEmpty()) {
                if (options.getHintValidationMode() == HintValidationMode.STRICT) {
                    throw new IllegalArgumentException(listLabel + "[" + i + "] has no valid terms");
                }
                continue;
            }
            HintTermKey key = new HintTermKey(refs);
            HintAggregate old = aggregated.get(key);
            if (old == null) {
                aggregated.put(key, new HintAggregate(refs, 1));
            } else {
                aggregated.put(key, new HintAggregate(old.refs, old.seenCount + 1));
            }
        }
    }

    private static List<ExclusiveFrequentItemsetSelector.PremergeHintCandidate> filterFreshHints(
            List<HintAggregate> aggregated,
            List<DocTerms> currentRows,
            int minSupport
    ) {
        List<HintResolvedCandidate> multiTermCandidates = new ArrayList<>();
        List<SingleHintResolvedCandidate> singleTermCandidates = new ArrayList<>();
        for (int i = 0; i < aggregated.size(); i++) {
            HintAggregate aggregate = aggregated.get(i);
            int currentSupport = countRowsContainingAllTerms(currentRows, aggregate.refs);
            int requiredSupport = requiredHintSupport(minSupport, aggregate.seenCount);
            if (currentSupport >= requiredSupport) {
                int qualityScore = computeHintQualityScore(currentSupport, requiredSupport, aggregate.seenCount);
                if (aggregate.refs.size() <= 1) {
                    singleTermCandidates.add(new SingleHintResolvedCandidate(
                            aggregate.refs,
                            aggregate.seenCount,
                            currentSupport,
                            qualityScore));
                } else {
                    multiTermCandidates.add(new HintResolvedCandidate(
                            aggregate.refs,
                            aggregate.seenCount,
                            currentSupport,
                            qualityScore));
                }
            }
        }
        List<ExclusiveFrequentItemsetSelector.PremergeHintCandidate> resolvedMutexHints =
                resolveMutexConflicts(multiTermCandidates);
        List<ExclusiveFrequentItemsetSelector.PremergeHintCandidate> resolvedSingleHints =
                selectSingleHints(singleTermCandidates, resolvedMutexHints.size(), minSupport);
        List<ExclusiveFrequentItemsetSelector.PremergeHintCandidate> out = new ArrayList<>(
                resolvedSingleHints.size() + resolvedMutexHints.size());
        out.addAll(resolvedSingleHints);
        out.addAll(resolvedMutexHints);
        return out;
    }

    private static List<ExclusiveFrequentItemsetSelector.PremergeHintCandidate> selectSingleHints(
            List<SingleHintResolvedCandidate> singles,
            int resolvedMutexHintCount,
            int minSupport
    ) {
        if (singles.isEmpty()) {
            return Collections.emptyList();
        }
        List<SingleHintResolvedCandidate> eligible = new ArrayList<>(singles.size());
        int supportFloor = minSupport;
        if (resolvedMutexHintCount > 0) {
            supportFloor = Math.max(minSupport,
                    (int) Math.ceil(minSupport * SINGLE_HINT_SUPPORT_MULTIPLIER_WHEN_MUTEX_EXISTS));
        }
        for (int i = 0; i < singles.size(); i++) {
            SingleHintResolvedCandidate c = singles.get(i);
            if (c.currentSupport >= supportFloor) {
                eligible.add(c);
            }
        }
        if (eligible.isEmpty()) {
            return Collections.emptyList();
        }
        Collections.sort(eligible, (a, b) -> {
            int cmp = Integer.compare(b.qualityScore, a.qualityScore);
            if (cmp != 0) {
                return cmp;
            }
            cmp = Integer.compare(b.currentSupport, a.currentSupport);
            if (cmp != 0) {
                return cmp;
            }
            cmp = Integer.compare(b.seenCount, a.seenCount);
            if (cmp != 0) {
                return cmp;
            }
            return compareLexicographicRefList(a.refs, b.refs);
        });
        int limit = eligible.size();
        if (resolvedMutexHintCount > 0) {
            limit = Math.min(
                    eligible.size(),
                    Math.max(MIN_SINGLE_HINTS_WHEN_MUTEX_EXISTS, resolvedMutexHintCount * SINGLE_HINTS_PER_MUTEX_HINT)
            );
        }
        List<ExclusiveFrequentItemsetSelector.PremergeHintCandidate> out = new ArrayList<>(limit);
        for (int i = 0; i < limit; i++) {
            SingleHintResolvedCandidate c = eligible.get(i);
            out.add(new ExclusiveFrequentItemsetSelector.PremergeHintCandidate(c.refs, c.qualityScore));
        }
        return out;
    }

    private static List<ExclusiveFrequentItemsetSelector.PremergeHintCandidate> resolveMutexConflicts(
            List<HintResolvedCandidate> candidates
    ) {
        if (candidates.isEmpty()) {
            return Collections.emptyList();
        }
        Collections.sort(candidates, (a, b) -> {
            int cmp = Integer.compare(conflictPriorityScore(b), conflictPriorityScore(a));
            if (cmp != 0) {
                return cmp;
            }
            cmp = Integer.compare(b.currentSupport, a.currentSupport);
            if (cmp != 0) {
                return cmp;
            }
            cmp = Integer.compare(b.seenCount, a.seenCount);
            if (cmp != 0) {
                return cmp;
            }
            return compareLexicographicRefList(a.refs, b.refs);
        });
        List<HintResolvedCandidate> selected = new ArrayList<>(candidates.size());
        for (int i = 0; i < candidates.size(); i++) {
            HintResolvedCandidate candidate = candidates.get(i);
            if (!conflictsWithAnySelected(candidate, selected)) {
                selected.add(candidate);
            }
        }
        List<ExclusiveFrequentItemsetSelector.PremergeHintCandidate> out = new ArrayList<>(selected.size());
        for (int i = 0; i < selected.size(); i++) {
            HintResolvedCandidate candidate = selected.get(i);
            out.add(new ExclusiveFrequentItemsetSelector.PremergeHintCandidate(candidate.refs, candidate.qualityScore));
        }
        return out;
    }

    private static int conflictPriorityScore(HintResolvedCandidate candidate) {
        return candidate.qualityScore * Math.max(1, candidate.currentSupport) * Math.max(2, candidate.refs.size());
    }

    private static boolean conflictsWithAnySelected(HintResolvedCandidate candidate, List<HintResolvedCandidate> selected) {
        for (int i = 0; i < selected.size(); i++) {
            if (areMutexHintsConflicting(candidate.refs, selected.get(i).refs)) {
                return true;
            }
        }
        return false;
    }

    private static boolean areMutexHintsConflicting(List<ByteRef> left, List<ByteRef> right) {
        if (left.size() <= 1 || right.size() <= 1) {
            return false;
        }
        boolean hasOverlap = false;
        for (int i = 0; i < left.size(); i++) {
            ByteRef li = left.get(i);
            for (int j = 0; j < right.size(); j++) {
                if (sameRef(li, right.get(j))) {
                    hasOverlap = true;
                    break;
                }
            }
            if (hasOverlap) {
                break;
            }
        }
        if (!hasOverlap) {
            return false;
        }
        return !isSubsetRefs(left, right) && !isSubsetRefs(right, left);
    }

    private static boolean isSubsetRefs(List<ByteRef> maybeSubset, List<ByteRef> maybeSuperset) {
        for (int i = 0; i < maybeSubset.size(); i++) {
            boolean found = false;
            ByteRef needle = maybeSubset.get(i);
            for (int j = 0; j < maybeSuperset.size(); j++) {
                if (sameRef(needle, maybeSuperset.get(j))) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                return false;
            }
        }
        return true;
    }

    private static int compareLexicographicRefList(List<ByteRef> a, List<ByteRef> b) {
        int n = Math.min(a.size(), b.size());
        for (int i = 0; i < n; i++) {
            int cmp = ByteArrayUtils.compareUnsigned(a.get(i), b.get(i));
            if (cmp != 0) {
                return cmp;
            }
        }
        return Integer.compare(a.size(), b.size());
    }

    private static int premergeHintCacheKeyHash(
            List<PremergeHint> mutexHints,
            List<PremergeHint> singleHints,
            HintValidationMode mode
    ) {
        int h = 1;
        h = 31 * h + System.identityHashCode(mutexHints);
        h = 31 * h + System.identityHashCode(singleHints);
        h = 31 * h + mutexHints.size();
        h = 31 * h + singleHints.size();
        h = 31 * h + mode.ordinal();
        return h;
    }

    private static boolean isAutoHintsWhenMissingEnabled() {
        return !"false".equalsIgnoreCase(System.getProperty(AUTO_HINTS_WHEN_MISSING_PROPERTY, "true"));
    }

    private static int requiredHintSupport(int minSupport, int seenCount) {
        if (seenCount >= STABLE_HINT_REPEAT_THRESHOLD) {
            return Math.max(1, minSupport);
        }
        return Math.max(1, (int) Math.ceil(minSupport * ONE_OFF_HINT_SUPPORT_MULTIPLIER));
    }

    private static int computeHintQualityScore(int currentSupport, int requiredSupport, int seenCount) {
        double supportRatio = requiredSupport <= 0 ? 1.0d : (currentSupport / (double) requiredSupport);
        double stabilityFactor = 1.0d + 0.25d * Math.max(0, seenCount - 1);
        int score = (int) Math.floor(supportRatio * stabilityFactor);
        if (score < 1) {
            return 1;
        }
        return Math.min(MAX_HINT_QUALITY_SCORE, score);
    }

    private static int countRowsContainingAllTerms(List<DocTerms> rows, List<ByteRef> refs) {
        if (refs.isEmpty()) {
            return 0;
        }
        int support = 0;
        for (int i = 0; i < rows.size(); i++) {
            List<ByteRef> rowTerms = rows.get(i).getTermRefsUnsafe();
            int matched = 0;
            for (int r = 0; r < refs.size(); r++) {
                if (containsRef(rowTerms, refs.get(r))) {
                    matched++;
                } else {
                    break;
                }
            }
            if (matched == refs.size()) {
                support++;
            }
        }
        return support;
    }

    private static boolean containsRef(List<ByteRef> terms, ByteRef target) {
        for (int i = 0; i < terms.size(); i++) {
            if (sameRef(terms.get(i), target)) {
                return true;
            }
        }
        return false;
    }

    private static boolean sameRef(ByteRef a, ByteRef b) {
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

    private static List<ByteRef> normalizeHintTermRefs(List<ByteRef> termRefs) {
        List<ByteRef> out = new ArrayList<>(termRefs.size());
        for (int i = 0; i < termRefs.size(); i++) {
            ByteRef ref = termRefs.get(i);
            if (ref == null || ref.getLength() == 0) {
                continue;
            }
            out.add(new ByteRef(ref.getSourceUnsafe(), ref.getOffset(), ref.getLength()));
        }
        return ByteArrayUtils.normalizeTermRefs(out);
    }

    private static ExclusiveSelectionResult selectWithDefaultSelectorTuning(
            List<DocTerms> rows,
            ProcessingOptions options,
            List<ExclusiveFrequentItemsetSelector.PremergeHintCandidate> premergeHints,
            int hintBoostWeight
    ) {
        ExclusiveFrequentItemsetSelector.ExecutionTuning executionTuning =
                new ExclusiveFrequentItemsetSelector.ExecutionTuning(
                        options.getSampleRatio(),
                        options.getMinSampleCount(),
                        options.getSamplingSupportScale(),
                        options.getPickerMinNetGain(),
                        options.getPickerEstimatedBytesPerTerm(),
                        options.getPickerCoverageRewardPerTerm(),
                        EngineTuningConfig.DEFAULT_MAX_DOC_COVERAGE_RATIO,
                        ExclusiveFrequentItemsetSelector.getPickerScoringWeights()
                );
        ExclusiveFrequentItemsetSelector.SelectionRequest request =
                ExclusiveFrequentItemsetSelector.SelectionRequest.builder(rows)
                        .minSupport(options.getMinSupport())
                        .minItemsetSize(options.getMinItemsetSize())
                        .maxItemsetSize(options.getMaxItemsetSize())
                        .maxCandidateCount(options.getMaxCandidateCount())
                        .premergeHints(premergeHints)
                        .hintBoostWeight(hintBoostWeight)
                        .executionTuning(executionTuning)
                        .build();
        return ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(request);
    }

    /**
     * 基于挖掘输入与已选互斥结果，构建三层索引派生数据。
     *
     * <p><b>前置条件</b>：{@code miningRows != null}、{@code result != null}、
     * {@code hotTermThresholdExclusive >= 0}。</p>
     */
    public static LineFileProcessingResult.DerivedData buildDerivedData(
            List<DocTerms> miningRows,
            ExclusiveSelectionResult result,
            int hotTermThresholdExclusive
    ) {
        Objects.requireNonNull(miningRows, "miningRows");
        Objects.requireNonNull(result, "result");
        if (hotTermThresholdExclusive < 0) {
            throw new IllegalArgumentException("hotTermThresholdExclusive must be >= 0");
        }
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
        List<DocTerms> out = new ArrayList<>(rows.size());
        for (DocTerms row : rows) {
            out.add(new DocTerms(row.getDocId(), row.getTermRefsUnsafe()));
        }
        return out;
    }

    private static List<DocTerms> tokenizeRowsForMining(
            List<DocTerms> rows,
            int ngramStart,
            int ngramEnd
    ) {
        List<DocTerms> out = new ArrayList<>(rows.size());
        for (DocTerms row : rows) {
            List<ByteRef> termRefs = row.getTermRefsUnsafe();
            if (termRefs.size() == 1) {
                ByteRef source = termRefs.get(0);
                out.add(new DocTerms(
                        row.getDocId(),
                        ByteNgramTokenizer.tokenizeRefs(
                                source.getSourceUnsafe(),
                                source.getOffset(),
                                source.getLength(),
                                ngramStart,
                                ngramEnd)));
            } else {
                // 兼容历史：如果调用方已提供 term 列表，则不重复切词。
                out.add(new DocTerms(row.getDocId(), termRefs));
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
        // termId 由 OpenHashTable 分配，减少 byte[] 键对象创建。
        OpenHashTable termTable = new OpenHashTable();
        List<LinkedHashSet<Integer>> docsByTermId = new ArrayList<>();
        for (DocTerms row : rows) {
            int docId = row.getDocId();
            for (ByteRef termRef : row.getTermRefsUnsafe()) {
                int hash = ByteArrayUtils.hash(
                        termRef.getSourceUnsafe(),
                        termRef.getOffset(),
                        termRef.getLength());
                int termId = termTable.getOrPut(termRef, hash, true);
                while (docsByTermId.size() <= termId) {
                    docsByTermId.add(new LinkedHashSet<Integer>());
                }
                LinkedHashSet<Integer> docs = docsByTermId.get(termId);
                docs.add(docId);
            }
        }

        List<LineFileProcessingResult.HotTermDocList> out =
                new ArrayList<>(docsByTermId.size());
        for (int termId = 0; termId < docsByTermId.size(); termId++) {
            List<Integer> docIds = new ArrayList<>(docsByTermId.get(termId));
            // 阈值语义是严格大于（count > threshold）。
            if (docIds.size() > hotTermThresholdExclusive) {
                out.add(new LineFileProcessingResult.HotTermDocList(
                        ByteRef.wrap(termTable.getKeyBytes(termId)), docIds));
            }
        }
        // 展示层排序：先按出现文档数降序，再按字节序升序保证同 count 稳定顺序。
        Collections.sort(out, (a, b) -> {
            int c = b.getCount() - a.getCount();
            if (c != 0) {
                return c;
            }
            return ByteArrayUtils.compareUnsigned(a.getTermRef(), b.getTermRef());
        });
        return out;
    }

    private static Set<ByteArrayKey> collectSelectedTerms(ExclusiveSelectionResult result) {
        Set<ByteArrayKey> out = new LinkedHashSet<ByteArrayKey>();
        for (SelectedGroup g : result.getGroups()) {
            for (ByteRef term : g.getTermRefs()) {
                int hash = ByteArrayUtils.hash(
                        term.getSourceUnsafe(),
                        term.getOffset(),
                        term.getLength());
                byte[] termBytes = term.copyBytes();
                ByteArrayKey lookupKey = ByteArrayKey.forLookup(termBytes, hash);
                if (!out.contains(lookupKey)) {
                    out.add(new ByteArrayKey(termBytes));
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
        OpenHashTable selectedLookup = buildSelectedLookup(selectedTerms);
        List<DocTerms> out = new ArrayList<>(cutRes.size());
        for (DocTerms row : cutRes) {
            List<ByteRef> rowTerms = row.getTermRefsUnsafe();
            List<ByteRef> filtered = new ArrayList<>(rowTerms.size());
            for (ByteRef termRef : rowTerms) {
                int hash = ByteArrayUtils.hash(
                        termRef.getSourceUnsafe(),
                        termRef.getOffset(),
                        termRef.getLength());
                if (selectedLookup.get(termRef, hash) < 0) {
                    filtered.add(termRef);
                }
            }
            out.add(new DocTerms(row.getDocId(), filtered));
        }
        return out;
    }

    private static OpenHashTable buildSelectedLookup(Set<ByteArrayKey> selectedTerms) {
        OpenHashTable table = new OpenHashTable(selectedTerms.size() * 2 + 1);
        for (ByteArrayKey key : selectedTerms) {
            byte[] term = key.bytes();
            int hash = ByteArrayUtils.hash(term);
            table.getOrPut(term, hash, true);
        }
        return table;
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
            ByteRef term = item.getTermRef();
            int hash = ByteArrayUtils.hash(
                    term.getSourceUnsafe(),
                    term.getOffset(),
                    term.getLength());
            ByteArrayKey lookupKey = ByteArrayKey.forLookup(term.copyBytes(), hash);
            if (!selectedTerms.contains(lookupKey)) {
                out.add(item);
            }
        }
        return out;
    }

    public enum HintValidationMode {
        STRICT,
        FILTER_ONLY
    }

    /** merge 阶段前置高频提示：仅词项序列；支持度由当前批次全量回算。 */
    public static final class PremergeHint {
        private final List<ByteRef> termRefs;

        public PremergeHint(List<ByteRef> termRefs) {
            this.termRefs = Collections.unmodifiableList(new ArrayList<>(Objects.requireNonNull(termRefs, "termRefs")));
        }

        public List<ByteRef> getTermRefs() {
            return termRefs;
        }
    }

    /**
     * 中间步骤测试入口：用于对 copy/tokenize/derive 各步骤做独立断言。
     *
     * <p>仅做方法透传，不改变任何业务语义。</p>
     */
    public static final class IntermediateSteps {
        private IntermediateSteps() {
        }

        /**
         * 前置条件：{@code rows != null}，且每个 {@link DocTerms} 不能为 null。
         */
        public static List<DocTerms> copyRows(List<DocTerms> rows) {
            Objects.requireNonNull(rows, "rows");
            return ExclusiveFpRowsProcessingApi.copyRows(rows);
        }

        /**
         * 前置条件：{@code rows != null}、{@code ngramStart > 0}、{@code ngramEnd >= ngramStart}。
         */
        public static List<DocTerms> tokenizeRowsForMining(
                List<DocTerms> rows,
                int ngramStart,
                int ngramEnd
        ) {
            Objects.requireNonNull(rows, "rows");
            validateNgramRange(ngramStart, ngramEnd);
            return ExclusiveFpRowsProcessingApi.tokenizeRowsForMining(rows, ngramStart, ngramEnd);
        }

        /**
         * 前置条件：{@code rows != null}、{@code hotTermThresholdExclusive >= 0}。
         */
        public static List<LineFileProcessingResult.HotTermDocList> buildHotTerms(
                List<DocTerms> rows,
                int hotTermThresholdExclusive
        ) {
            Objects.requireNonNull(rows, "rows");
            if (hotTermThresholdExclusive < 0) {
                throw new IllegalArgumentException("hotTermThresholdExclusive must be >= 0");
            }
            return ExclusiveFpRowsProcessingApi.buildHotTerms(rows, hotTermThresholdExclusive);
        }

        /**
         * 前置条件：{@code result != null}。
         */
        public static Set<ByteArrayKey> collectSelectedTerms(ExclusiveSelectionResult result) {
            Objects.requireNonNull(result, "result");
            return ExclusiveFpRowsProcessingApi.collectSelectedTerms(result);
        }

        /**
         * 前置条件：{@code cutRes != null}、{@code selectedTerms != null}。
         */
        public static List<DocTerms> removeSelectedTermsFromCutRes(
                List<DocTerms> cutRes,
                Set<ByteArrayKey> selectedTerms
        ) {
            Objects.requireNonNull(cutRes, "cutRes");
            Objects.requireNonNull(selectedTerms, "selectedTerms");
            return ExclusiveFpRowsProcessingApi.removeSelectedTermsFromCutRes(cutRes, selectedTerms);
        }

        /**
         * 前置条件：{@code hotTerms != null}、{@code selectedTerms != null}。
         */
        public static List<LineFileProcessingResult.HotTermDocList> removeSelectedTermsFromHotTerms(
                List<LineFileProcessingResult.HotTermDocList> hotTerms,
                Set<ByteArrayKey> selectedTerms
        ) {
            Objects.requireNonNull(hotTerms, "hotTerms");
            Objects.requireNonNull(selectedTerms, "selectedTerms");
            return ExclusiveFpRowsProcessingApi.removeSelectedTermsFromHotTerms(hotTerms, selectedTerms);
        }

        /**
         * 前置条件：{@code options != null}。
         */
        public static void validateProcessingOptions(ProcessingOptions options) {
            Objects.requireNonNull(options, "options");
            ExclusiveFpRowsProcessingApi.validateProcessingOptions(options);
        }

        /** 仅用于并发测试/观测：清空 premerge hint 聚合缓存与命中计数。 */
        public static void clearPremergeHintAggregateCache() {
            clearPremergeHintAggregateCacheInternal();
            PREMERGE_HINT_CACHE_CLEANUPS.reset();
        }

        public static long premergeHintAggregateCacheHitCount() {
            return PREMERGE_HINT_CACHE_HITS.sum();
        }

        public static long premergeHintAggregateCacheMissCount() {
            return PREMERGE_HINT_CACHE_MISSES.sum();
        }

        public static long premergeHintAggregateCacheCleanupCount() {
            return PREMERGE_HINT_CACHE_CLEANUPS.sum();
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
        /**
         * 与 {@code highFreqMutexGroupPostings} 同源：每条 hint 对应一个互斥词组（多为多个 term）。
         */
        private final List<PremergeHint> premergeMutexGroupHints;
        /**
         * 与 {@code highFreqSingleTermPostings} 同源：每条 hint 通常只含一个 term。
         */
        private final List<PremergeHint> premergeSingleTermHints;
        private final int hintBoostWeight;
        private final HintValidationMode hintValidationMode;

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
            this(
                    minSupport,
                    minItemsetSize,
                    maxItemsetSize,
                    maxCandidateCount,
                    hotTermThresholdExclusive,
                    ngramStart,
                    ngramEnd,
                    skipHashMinGram,
                    skipHashMaxGram,
                    sampleRatio,
                    minSampleCount,
                    samplingSupportScale,
                    pickerMinNetGain,
                    pickerEstimatedBytesPerTerm,
                    pickerCoverageRewardPerTerm,
                    Collections.<PremergeHint>emptyList(),
                    Collections.<PremergeHint>emptyList(),
                    0,
                    HintValidationMode.FILTER_ONLY
            );
        }

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
                int pickerCoverageRewardPerTerm,
                List<PremergeHint> premergeMutexGroupHints,
                List<PremergeHint> premergeSingleTermHints,
                int hintBoostWeight,
                HintValidationMode hintValidationMode
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
            this.premergeMutexGroupHints = Collections.unmodifiableList(new ArrayList<>(
                    Objects.requireNonNull(premergeMutexGroupHints, "premergeMutexGroupHints")));
            this.premergeSingleTermHints = Collections.unmodifiableList(new ArrayList<>(
                    Objects.requireNonNull(premergeSingleTermHints, "premergeSingleTermHints")));
            this.hintBoostWeight = hintBoostWeight;
            this.hintValidationMode = Objects.requireNonNull(hintValidationMode, "hintValidationMode");
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
                    pickerMinNetGain, pickerEstimatedBytesPerTerm, pickerCoverageRewardPerTerm,
                    premergeMutexGroupHints, premergeSingleTermHints, hintBoostWeight, hintValidationMode
            );
        }

        public ProcessingOptions withMinItemsetSize(int value) {
            return new ProcessingOptions(
                    minSupport, value, maxItemsetSize, maxCandidateCount, hotTermThresholdExclusive,
                    ngramStart, ngramEnd, skipHashMinGram, skipHashMaxGram,
                    sampleRatio, minSampleCount, samplingSupportScale,
                    pickerMinNetGain, pickerEstimatedBytesPerTerm, pickerCoverageRewardPerTerm,
                    premergeMutexGroupHints, premergeSingleTermHints, hintBoostWeight, hintValidationMode
            );
        }

        public ProcessingOptions withMaxItemsetSize(int value) {
            return new ProcessingOptions(
                    minSupport, minItemsetSize, value, maxCandidateCount, hotTermThresholdExclusive,
                    ngramStart, ngramEnd, skipHashMinGram, skipHashMaxGram,
                    sampleRatio, minSampleCount, samplingSupportScale,
                    pickerMinNetGain, pickerEstimatedBytesPerTerm, pickerCoverageRewardPerTerm,
                    premergeMutexGroupHints, premergeSingleTermHints, hintBoostWeight, hintValidationMode
            );
        }

        public ProcessingOptions withMaxCandidateCount(int value) {
            return new ProcessingOptions(
                    minSupport, minItemsetSize, maxItemsetSize, value, hotTermThresholdExclusive,
                    ngramStart, ngramEnd, skipHashMinGram, skipHashMaxGram,
                    sampleRatio, minSampleCount, samplingSupportScale,
                    pickerMinNetGain, pickerEstimatedBytesPerTerm, pickerCoverageRewardPerTerm,
                    premergeMutexGroupHints, premergeSingleTermHints, hintBoostWeight, hintValidationMode
            );
        }

        public ProcessingOptions withHotTermThresholdExclusive(int value) {
            return new ProcessingOptions(
                    minSupport, minItemsetSize, maxItemsetSize, maxCandidateCount, value,
                    ngramStart, ngramEnd, skipHashMinGram, skipHashMaxGram,
                    sampleRatio, minSampleCount, samplingSupportScale,
                    pickerMinNetGain, pickerEstimatedBytesPerTerm, pickerCoverageRewardPerTerm,
                    premergeMutexGroupHints, premergeSingleTermHints, hintBoostWeight, hintValidationMode
            );
        }

        public ProcessingOptions withNgramRange(int start, int end) {
            return new ProcessingOptions(
                    minSupport, minItemsetSize, maxItemsetSize, maxCandidateCount, hotTermThresholdExclusive,
                    start, end, skipHashMinGram, skipHashMaxGram,
                    sampleRatio, minSampleCount, samplingSupportScale,
                    pickerMinNetGain, pickerEstimatedBytesPerTerm, pickerCoverageRewardPerTerm,
                    premergeMutexGroupHints, premergeSingleTermHints, hintBoostWeight, hintValidationMode
            );
        }

        public ProcessingOptions withSkipHashGramRange(int minGram, int maxGram) {
            return new ProcessingOptions(
                    minSupport, minItemsetSize, maxItemsetSize, maxCandidateCount, hotTermThresholdExclusive,
                    ngramStart, ngramEnd, minGram, maxGram,
                    sampleRatio, minSampleCount, samplingSupportScale,
                    pickerMinNetGain, pickerEstimatedBytesPerTerm, pickerCoverageRewardPerTerm,
                    premergeMutexGroupHints, premergeSingleTermHints, hintBoostWeight, hintValidationMode
            );
        }

        public ProcessingOptions withSampleRatio(double value) {
            return new ProcessingOptions(
                    minSupport, minItemsetSize, maxItemsetSize, maxCandidateCount, hotTermThresholdExclusive,
                    ngramStart, ngramEnd, skipHashMinGram, skipHashMaxGram,
                    value, minSampleCount, samplingSupportScale,
                    pickerMinNetGain, pickerEstimatedBytesPerTerm, pickerCoverageRewardPerTerm,
                    premergeMutexGroupHints, premergeSingleTermHints, hintBoostWeight, hintValidationMode
            );
        }

        public ProcessingOptions withMinSampleCount(int value) {
            return new ProcessingOptions(
                    minSupport, minItemsetSize, maxItemsetSize, maxCandidateCount, hotTermThresholdExclusive,
                    ngramStart, ngramEnd, skipHashMinGram, skipHashMaxGram,
                    sampleRatio, value, samplingSupportScale,
                    pickerMinNetGain, pickerEstimatedBytesPerTerm, pickerCoverageRewardPerTerm,
                    premergeMutexGroupHints, premergeSingleTermHints, hintBoostWeight, hintValidationMode
            );
        }

        public ProcessingOptions withSamplingSupportScale(double value) {
            return new ProcessingOptions(
                    minSupport, minItemsetSize, maxItemsetSize, maxCandidateCount, hotTermThresholdExclusive,
                    ngramStart, ngramEnd, skipHashMinGram, skipHashMaxGram,
                    sampleRatio, minSampleCount, value,
                    pickerMinNetGain, pickerEstimatedBytesPerTerm, pickerCoverageRewardPerTerm,
                    premergeMutexGroupHints, premergeSingleTermHints, hintBoostWeight, hintValidationMode
            );
        }

        public ProcessingOptions withPickerMinNetGain(int value) {
            return new ProcessingOptions(
                    minSupport, minItemsetSize, maxItemsetSize, maxCandidateCount, hotTermThresholdExclusive,
                    ngramStart, ngramEnd, skipHashMinGram, skipHashMaxGram,
                    sampleRatio, minSampleCount, samplingSupportScale,
                    value, pickerEstimatedBytesPerTerm, pickerCoverageRewardPerTerm,
                    premergeMutexGroupHints, premergeSingleTermHints, hintBoostWeight, hintValidationMode
            );
        }

        public ProcessingOptions withPickerEstimatedBytesPerTerm(int value) {
            return new ProcessingOptions(
                    minSupport, minItemsetSize, maxItemsetSize, maxCandidateCount, hotTermThresholdExclusive,
                    ngramStart, ngramEnd, skipHashMinGram, skipHashMaxGram,
                    sampleRatio, minSampleCount, samplingSupportScale,
                    pickerMinNetGain, value, pickerCoverageRewardPerTerm,
                    premergeMutexGroupHints, premergeSingleTermHints, hintBoostWeight, hintValidationMode
            );
        }

        public ProcessingOptions withPickerCoverageRewardPerTerm(int value) {
            return new ProcessingOptions(
                    minSupport, minItemsetSize, maxItemsetSize, maxCandidateCount, hotTermThresholdExclusive,
                    ngramStart, ngramEnd, skipHashMinGram, skipHashMaxGram,
                    sampleRatio, minSampleCount, samplingSupportScale,
                    pickerMinNetGain, pickerEstimatedBytesPerTerm, value,
                    premergeMutexGroupHints, premergeSingleTermHints, hintBoostWeight, hintValidationMode
            );
        }

        /**
         * 历史段 {@code highFreqMutexGroupPostings} 映射为 pre-merge 提示（每条 hint 对应一个互斥词组）。
         *
         * <p>契约建议：优先提供该类提示；每条提示通常包含 >=2 个 term，且来自最近 merge 窗口。</p>
         */
        public ProcessingOptions withPremergeMutexGroupHints(List<PremergeHint> value) {
            return new ProcessingOptions(
                    minSupport, minItemsetSize, maxItemsetSize, maxCandidateCount, hotTermThresholdExclusive,
                    ngramStart, ngramEnd, skipHashMinGram, skipHashMaxGram,
                    sampleRatio, minSampleCount, samplingSupportScale,
                    pickerMinNetGain, pickerEstimatedBytesPerTerm, pickerCoverageRewardPerTerm,
                    value, premergeSingleTermHints, hintBoostWeight, hintValidationMode
            );
        }

        /**
         * 历史段 {@code highFreqSingleTermPostings} 映射为 pre-merge 提示（每条 hint 通常只含 1 个 term）。
         *
         * <p>契约建议：single hints 仅作为补充，避免无筛选全量下发；存在 mutex hints 时会被更严格限流。</p>
         */
        public ProcessingOptions withPremergeSingleTermHints(List<PremergeHint> value) {
            return new ProcessingOptions(
                    minSupport, minItemsetSize, maxItemsetSize, maxCandidateCount, hotTermThresholdExclusive,
                    ngramStart, ngramEnd, skipHashMinGram, skipHashMaxGram,
                    sampleRatio, minSampleCount, samplingSupportScale,
                    pickerMinNetGain, pickerEstimatedBytesPerTerm, pickerCoverageRewardPerTerm,
                    premergeMutexGroupHints, value, hintBoostWeight, hintValidationMode
            );
        }

        /**
         * hint 优先级附加权重；取 0 等价于关闭 hint 对排序的影响。
         */
        public ProcessingOptions withHintBoostWeight(int value) {
            return new ProcessingOptions(
                    minSupport, minItemsetSize, maxItemsetSize, maxCandidateCount, hotTermThresholdExclusive,
                    ngramStart, ngramEnd, skipHashMinGram, skipHashMaxGram,
                    sampleRatio, minSampleCount, samplingSupportScale,
                    pickerMinNetGain, pickerEstimatedBytesPerTerm, pickerCoverageRewardPerTerm,
                    premergeMutexGroupHints, premergeSingleTermHints, value, hintValidationMode
            );
        }

        /**
         * hint 输入校验模式：STRICT 会对空/非法 hint 直接抛错，FILTER_ONLY 会尽量过滤后继续执行。
         */
        public ProcessingOptions withHintValidationMode(HintValidationMode value) {
            return new ProcessingOptions(
                    minSupport, minItemsetSize, maxItemsetSize, maxCandidateCount, hotTermThresholdExclusive,
                    ngramStart, ngramEnd, skipHashMinGram, skipHashMaxGram,
                    sampleRatio, minSampleCount, samplingSupportScale,
                    pickerMinNetGain, pickerEstimatedBytesPerTerm, pickerCoverageRewardPerTerm,
                    premergeMutexGroupHints, premergeSingleTermHints, hintBoostWeight, value
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

        public List<PremergeHint> getPremergeMutexGroupHints() {
            return premergeMutexGroupHints;
        }

        public List<PremergeHint> getPremergeSingleTermHints() {
            return premergeSingleTermHints;
        }

        public int getHintBoostWeight() {
            return hintBoostWeight;
        }

        public HintValidationMode getHintValidationMode() {
            return hintValidationMode;
        }
    }

    private static final class HintAggregate {
        private final List<ByteRef> refs;
        private final int seenCount;

        private HintAggregate(List<ByteRef> refs, int seenCount) {
            this.refs = refs;
            this.seenCount = seenCount;
        }
    }

    private static final class HintResolvedCandidate {
        private final List<ByteRef> refs;
        private final int seenCount;
        private final int currentSupport;
        private final int qualityScore;

        private HintResolvedCandidate(List<ByteRef> refs, int seenCount, int currentSupport, int qualityScore) {
            this.refs = refs;
            this.seenCount = seenCount;
            this.currentSupport = currentSupport;
            this.qualityScore = qualityScore;
        }
    }

    private static final class SingleHintResolvedCandidate {
        private final List<ByteRef> refs;
        private final int seenCount;
        private final int currentSupport;
        private final int qualityScore;

        private SingleHintResolvedCandidate(List<ByteRef> refs, int seenCount, int currentSupport, int qualityScore) {
            this.refs = refs;
            this.seenCount = seenCount;
            this.currentSupport = currentSupport;
            this.qualityScore = qualityScore;
        }
    }

    private static final class HintTermKey {
        private final List<ByteRef> refs;
        private final int hash;

        private HintTermKey(List<ByteRef> refs) {
            this.refs = refs;
            int h = 1;
            for (int i = 0; i < refs.size(); i++) {
                h = 31 * h + ByteArrayUtils.hash(
                        refs.get(i).getSourceUnsafe(),
                        refs.get(i).getOffset(),
                        refs.get(i).getLength());
            }
            this.hash = h;
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
            if (!(obj instanceof HintTermKey)) {
                return false;
            }
            HintTermKey other = (HintTermKey) obj;
            if (refs.size() != other.refs.size()) {
                return false;
            }
            for (int i = 0; i < refs.size(); i++) {
                if (!sameRef(refs.get(i), other.refs.get(i))) {
                    return false;
                }
            }
            return true;
        }
    }

    private static final class HintAggregateCacheEntry {
        private final int keyHash;
        private final HintValidationMode mode;
        private final List<PremergeHint> mutexHintsRef;
        private final List<PremergeHint> singleHintsRef;
        private final List<HintAggregate> aggregates;

        private HintAggregateCacheEntry(
                int keyHash,
                HintValidationMode mode,
                List<PremergeHint> mutexHintsRef,
                List<PremergeHint> singleHintsRef,
                List<HintAggregate> aggregates
        ) {
            this.keyHash = keyHash;
            this.mode = mode;
            this.mutexHintsRef = mutexHintsRef;
            this.singleHintsRef = singleHintsRef;
            this.aggregates = aggregates;
        }

        private boolean matches(
                List<PremergeHint> mutexHintsRef,
                List<PremergeHint> singleHintsRef,
                HintValidationMode mode,
                int keyHash
        ) {
            return this.keyHash == keyHash
                    && this.mode == mode
                    && this.mutexHintsRef == mutexHintsRef
                    && this.singleHintsRef == singleHintsRef;
        }
    }

    private static final class TermSupport {
        private final int termId;
        private final int support;

        private TermSupport(int termId, int support) {
            this.termId = termId;
            this.support = support;
        }
    }

    private static final class PairSupport {
        private final int leftIdx;
        private final int rightIdx;
        private final int support;

        private PairSupport(int leftIdx, int rightIdx, int support) {
            this.leftIdx = leftIdx;
            this.rightIdx = rightIdx;
            this.support = support;
        }
    }
}
