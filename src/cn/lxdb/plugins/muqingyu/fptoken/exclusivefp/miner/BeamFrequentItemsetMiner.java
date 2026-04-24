package cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.miner;

import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.config.EngineTuningConfig;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.config.SelectorConfig;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.CandidateItemset;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.FrequentItemsetMiningResult;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.protocol.ModuleDataContracts;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.PriorityQueue;
import java.util.List;
import java.util.Map;
import java.lang.ref.SoftReference;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * 这个类用 Beam Search 在大数据里找“经常一起出现”的词（或商品）组合。
 *
 * <p>可以把它理解成“加速版频繁项集挖掘”：
 * <ol>
 *   <li>先找出出现次数达标的单词（1-项集），并按支持度从高到低排好。</li>
 *   <li>从这些单词开始一层层往外扩展，拼成 2 项、3 项...更长的组合。</li>
 *   <li>每一层只保留最有希望的一小批前缀（beamWidth），避免组合数量爆炸。</li>
 *   <li>每个前缀还能限制最多扩多少个子分支（maxBranchingFactor），继续控时控内存。</li>
 *   <li>最终候选数量也受 maxCandidateCount 限制，达到上限就提前结束。</li>
 * </ol>
 *
 * <p>注意：这是“近似”算法，不保证把所有频繁组合都找全，但速度更快、资源更可控。
 *
 * <p><b>性能特征（近似）</b>：
 * <ul>
 *   <li>时间复杂度主要受频繁词数量、{@code beamWidth}、{@code maxBranchingFactor} 和最大项集长度影响。</li>
 *   <li>每次扩展的核心开销是位图交集与支持度统计，且按层做 Top-K 保留，避免全空间枚举。</li>
 *   <li>空间复杂度主要来自每层保留的前缀状态与候选输出，通常可近似看作与 {@code beamWidth} 线性相关。</li>
 * </ul>
 *
 * @author muqingyu
 */
public final class BeamFrequentItemsetMiner {
    private static final Logger LOG = Logger.getLogger(BeamFrequentItemsetMiner.class.getName());
    private static final String DEBUG_LOG_PROPERTY = "fptoken.miner.debugLog";
    private static final String REUSE_TIDSET_CACHE_ACROSS_CALLS_PROPERTY = "fptoken.miner.reuseTidsetCacheAcrossCalls";
    private static final int SHALLOW_EXPLORATION_MIN_BEAM = 4;
    private static final int SOFT_TIDSET_CACHE_MAX_ENTRIES = 2048;
    private static final int SCRATCH_SHRINK_TRIGGER_DIVISOR = 4;
    private static final int SCRATCH_SHRINK_HEADROOM_MULTIPLIER = 2;
    private static final Comparator<PrefixState> PREFIX_BEST_FIRST = BeamFrequentItemsetMiner::comparePrefixBest;
    private static final Comparator<PrefixState> PREFIX_WORST_FIRST = (a, b) -> comparePrefixBest(b, a);
    private static final Comparator<CandidateItemset> CANDIDATE_WORST_FIRST =
            BeamFrequentItemsetMiner::compareCandidateWorstFirst;
    private static final Comparator<CandidateItemset> CANDIDATE_BEST_FIRST =
            BeamFrequentItemsetMiner::compareCandidateBestFirst;

    @FunctionalInterface
    public interface ScoreFunction {
        int calculate(int length, int support);
    }

    private final ScoreFunction scoreFunction;
    private final boolean useDefaultScoreFormula;
    /** 线程本地复用临时位图，避免并发调用时共享可变状态。 */
    private final ThreadLocal<ScratchBitSet> threadLocalScratch = ThreadLocal.withInitial(
            () -> new ScratchBitSet(EngineTuningConfig.MIN_BITSET_CAPACITY)
    );
    /** 线程本地紧凑路径栈：数组模拟栈，复用用于路径物化，减少扩展时临时数组分配。 */
    private final ThreadLocal<CompactPathStack> threadLocalPathStack = ThreadLocal.withInitial(
            () -> new CompactPathStack(EngineTuningConfig.MIN_BITSET_CAPACITY)
    );
    /** 线程本地软引用缓存：在重复请求中复用中间 k 项集 tidset，内存紧张时可被 GC 回收。 */
    private final ThreadLocal<SoftTidsetCache> threadLocalTidsetCache = ThreadLocal.withInitial(
            () -> new SoftTidsetCache(SOFT_TIDSET_CACHE_MAX_ENTRIES)
    );

    public BeamFrequentItemsetMiner() {
        this(null);
    }

    public BeamFrequentItemsetMiner(ScoreFunction scoreFunction) {
        this.useDefaultScoreFormula = (scoreFunction == null);
        this.scoreFunction = scoreFunction != null ? scoreFunction : EngineTuningConfig::defaultScore;
    }

    /**
     * 执行挖掘主流程，返回候选项集和统计信息。
     *
     * <p><b>模块协议</b>：建议由 index 模块先组装
     * {@link ModuleDataContracts.TidsetMiningInput}，
     * 再通过本方法消费 {@code tidsetsByTermId}，保持“miner 只依赖 tidset 输入”的边界。</p>
     *
     * @param tidsetsByTermId termId -> BitSet，表示“这个词出现在哪些文档/交易里”
     * @param config 最小支持度、项集长度范围、最大候选数等配置
     * @param maxFrequentTermCount 只拿前多少个高频词参与扩展；{@code <= 0} 表示不限制
     * @param maxBranchingFactor 每个前缀最多扩多少个孩子；{@code <= 0} 表示不限制
     * @param beamWidth 每层最多保留多少个前缀状态；{@code <= 0} 使用默认值
     * @return 结果里包含候选列表，以及挖掘过程中的计数信息
     */
    public FrequentItemsetMiningResult mineWithStats(
            List<BitSet> tidsetsByTermId,
            SelectorConfig config,
            int maxFrequentTermCount,
            int maxBranchingFactor,
            int beamWidth
    ) {
        if (tidsetsByTermId == null || tidsetsByTermId.isEmpty()) {
            return result(Collections.<CandidateItemset>emptyList(), 0, 0, 0, false);
        }
        return mineWithStats(
                new ModuleDataContracts.TidsetMiningInput(tidsetsByTermId),
                config,
                maxFrequentTermCount,
                maxBranchingFactor,
                beamWidth,
                0,
                0L);
    }

    /**
     * 可读性更友好的别名入口；语义与 {@link #mineWithStats(List, SelectorConfig, int, int, int)} 完全一致。
     */
    public FrequentItemsetMiningResult mine(
            List<BitSet> tidsetsByTermId,
            SelectorConfig config,
            int maxFrequentTermCount,
            int maxBranchingFactor,
            int beamWidth
    ) {
        return mineWithStats(tidsetsByTermId, config, maxFrequentTermCount, maxBranchingFactor, beamWidth);
    }

    /**
     * 带早停策略的挖掘入口：
     * <ul>
     *   <li>maxIdleLevels：连续多少层没有新候选就停止（{@code <= 0} 表示关闭）。</li>
     *   <li>maxRuntimeMillis：运行时间上限，超时后停止（{@code <= 0} 表示关闭）。</li>
     * </ul>
     */
    public FrequentItemsetMiningResult mineWithStats(
            List<BitSet> tidsetsByTermId,
            SelectorConfig config,
            int maxFrequentTermCount,
            int maxBranchingFactor,
            int beamWidth,
            int maxIdleLevels,
            long maxRuntimeMillis
    ) {
        if (tidsetsByTermId == null || tidsetsByTermId.isEmpty()) {
            return result(Collections.<CandidateItemset>emptyList(), 0, 0, 0, false);
        }
        ModuleDataContracts.TidsetMiningInput protocolInput =
                new ModuleDataContracts.TidsetMiningInput(tidsetsByTermId);
        return mineWithStats(
                protocolInput,
                config,
                maxFrequentTermCount,
                maxBranchingFactor,
                beamWidth,
                maxIdleLevels,
                maxRuntimeMillis);
    }

    public FrequentItemsetMiningResult mineWithStats(
            ModuleDataContracts.TidsetMiningInput input,
            SelectorConfig config,
            int maxFrequentTermCount,
            int maxBranchingFactor,
            int beamWidth,
            int maxIdleLevels,
            long maxRuntimeMillis
    ) {
        try {
            boolean debugLog = isDebugLogEnabled();
            prepareTidsetCacheForRun();
            if (input == null || input.getVocabularySize() == 0) {
                return result(Collections.<CandidateItemset>emptyList(), 0, 0, 0, false);
            }
            List<BitSet> tidsetsByTermId = input.getTidsetsByTermId();
            validateMiningInputs(tidsetsByTermId, config);
            // 先把每个单词的支持度缓存下来，后面排序时不用重复算。
            int[] supportsByTermId = new int[tidsetsByTermId.size()];
            int[] tidsetWordsByTermId = collectTidsetWordSizes(tidsetsByTermId);
            int[] frequentTermIdsArray = resolveFrequentTermIds(
                    tidsetsByTermId,
                    config.getMinSupport(),
                    supportsByTermId,
                    maxFrequentTermCount
            );
            if (frequentTermIdsArray.length == 0) {
                return result(Collections.<CandidateItemset>emptyList(), 0, 0, 0, false);
            }
            if (debugLog) {
                LOG.log(Level.INFO,
                        "[fptoken-miner] start minSupport={0}, minItemsetSize={1}, maxItemsetSize={2}, maxCandidateCount={3}, frequentTerms={4}, beamWidth={5}, maxBranchingFactor={6}, maxIdleLevels={7}, maxRuntimeMillis={8}",
                        new Object[] {
                                config.getMinSupport(),
                                config.getMinItemsetSize(),
                                config.getMaxItemsetSize(),
                                config.getMaxCandidateCount(),
                                frequentTermIdsArray.length,
                                beamWidth,
                                maxBranchingFactor,
                                maxIdleLevels,
                                maxRuntimeMillis
                        });
            }

            int safeBeamWidth = beamWidth > 0 ? beamWidth : EngineTuningConfig.MINER_FALLBACK_BEAM_WIDTH;
            int[] suffixMaxSupportByPos = buildSuffixMaxSupportByPos(frequentTermIdsArray, supportsByTermId);
            MiningState miningState = new MiningState(new ArrayList<>(
                    Math.min(config.getMaxCandidateCount(), EngineTuningConfig.MAX_INITIAL_CANDIDATE_CAPACITY)
            ), config.getMaxCandidateCount());
            int scratchBitSize = estimateScratchBitSize(tidsetsByTermId);
            long deadlineNanos = maxRuntimeMillis > 0
                    ? System.nanoTime() + maxRuntimeMillis * 1_000_000L
                    : 0L;

            List<PrefixState> firstLevel = performFirstLevelExpansion(
                    frequentTermIdsArray,
                    tidsetsByTermId,
                    supportsByTermId,
                    tidsetWordsByTermId,
                    config,
                    miningState
            );
            if (miningState.truncatedByCandidateLimit) {
                if (debugLog) {
                    logMiningSummary(miningState, frequentTermIdsArray.length, true);
                }
                return toResult(miningState, frequentTermIdsArray.length);
            }
            // 首层做“保底探索宽度”，避免单个高频噪声词占满超窄 beam，导致高价值长项集路径过早丢失。
            List<PrefixState> currentLevel = topK(
                    firstLevel,
                    guardedBeamWidth(0, safeBeamWidth, config.getMaxItemsetSize())
            );
            int startDepth = 1;
            if (config.getMinItemsetSize() > 2 && config.getMaxItemsetSize() >= 2) {
                int pairLevelWidth = guardedBeamWidth(1, safeBeamWidth, config.getMaxItemsetSize());
                currentLevel = bootstrapPairLevel(
                        currentLevel,
                        frequentTermIdsArray,
                        tidsetsByTermId,
                        supportsByTermId,
                        tidsetWordsByTermId,
                        suffixMaxSupportByPos,
                        config,
                        maxBranchingFactor,
                        safeBeamWidth,
                        pairLevelWidth,
                        scratchBitSize,
                        deadlineNanos,
                        miningState
                );
                startDepth = 2;
            }

            // 从 startDepth+1 对应项集长度开始逐层扩展，直到配置的最大项集长度。
            for (int depth = startDepth; depth < config.getMaxItemsetSize(); depth++) {
                if (isTimedOut(deadlineNanos) || currentLevel.isEmpty()) {
                    break;
                }
                int adaptiveWidth = guardedBeamWidth(depth, safeBeamWidth, config.getMaxItemsetSize());
                LevelExpansionResult expansionResult = expandLevel(
                        currentLevel,
                        frequentTermIdsArray,
                        tidsetsByTermId,
                        supportsByTermId,
                        tidsetWordsByTermId,
                        suffixMaxSupportByPos,
                        config,
                        maxBranchingFactor,
                        safeBeamWidth,
                        adaptiveWidth,
                        scratchBitSize,
                        deadlineNanos,
                        miningState
                );
                currentLevel = expansionResult.nextLevel;
                if (shouldStopAfterLevel(expansionResult, maxIdleLevels, miningState)) {
                    if (expansionResult.timedOut) {
                        miningState.timedOutStop = true;
                    } else if (maxIdleLevels > 0 && expansionResult.discoveredCount == 0
                            && miningState.idleLevelCount >= maxIdleLevels) {
                        miningState.idleStop = true;
                    }
                    break;
                }
            }
            if (debugLog) {
                logMiningSummary(miningState, frequentTermIdsArray.length, false);
            }
            return toResult(miningState, frequentTermIdsArray.length);
        } finally {
            clearThreadLocalScratch();
        }
    }

    /**
     * 可读性更友好的别名入口；语义与
     * {@link #mineWithStats(List, SelectorConfig, int, int, int, int, long)} 完全一致。
     */
    public FrequentItemsetMiningResult mine(
            List<BitSet> tidsetsByTermId,
            SelectorConfig config,
            int maxFrequentTermCount,
            int maxBranchingFactor,
            int beamWidth,
            int maxIdleLevels,
            long maxRuntimeMillis
    ) {
        return mineWithStats(
                tidsetsByTermId,
                config,
                maxFrequentTermCount,
                maxBranchingFactor,
                beamWidth,
                maxIdleLevels,
                maxRuntimeMillis
        );
    }

    private void validateMiningInputs(List<BitSet> tidsetsByTermId, SelectorConfig config) {
        if (config == null) {
            throw new IllegalArgumentException("config must not be null");
        }
        for (int i = 0; i < tidsetsByTermId.size(); i++) {
            if (tidsetsByTermId.get(i) == null) {
                throw new IllegalArgumentException("tidsetsByTermId[" + i + "] must not be null");
            }
        }
    }

    private int[] resolveFrequentTermIds(
            List<BitSet> tidsetsByTermId,
            int minSupport,
            int[] supportsByTermId,
            int maxFrequentTermCount
    ) {
        return collectFrequentTermIds(
                tidsetsByTermId,
                minSupport,
                supportsByTermId,
                maxFrequentTermCount
        );
    }

    /** 第 1 层扩展：构造 1-项前缀；若允许 1-项集则直接写入候选。 */
    private List<PrefixState> performFirstLevelExpansion(
            int[] frequentTermIds,
            List<BitSet> tidsetsByTermId,
            int[] supportsByTermId,
            int[] tidsetWordsByTermId,
            SelectorConfig config,
            MiningState miningState
    ) {
        List<PrefixState> currentLevel = new ArrayList<>(frequentTermIds.length);
        for (int i = 0; i < frequentTermIds.length; i++) {
            PrefixState state = singleTermPrefix(
                    i,
                    frequentTermIds,
                    tidsetsByTermId,
                    supportsByTermId,
                    tidsetWordsByTermId
            );
            currentLevel.add(state);
            if (config.getMinItemsetSize() <= 1
                    && appendCandidate(materializeTermIds(state), state.tidset, state.support, miningState)) {
                break;
            }
        }
        return currentLevel;
    }

    /**
     * 当 minItemsetSize > 2 时，直接构造 2-项前缀层作为搜索起点，
     * 后续主循环从 3-项开始扩展，减少对短项集层的无效推进。
     */
    private List<PrefixState> bootstrapPairLevel(
            List<PrefixState> firstLevel,
            int[] frequentTermIds,
            List<BitSet> tidsetsByTermId,
            int[] supportsByTermId,
            int[] tidsetWordsByTermId,
            int[] suffixMaxSupportByPos,
            SelectorConfig config,
            int maxBranchingFactor,
            int beamWidth,
            int nextLevelWidth,
            int scratchBitSize,
            long deadlineNanos,
            MiningState miningState
    ) {
        if (firstLevel.isEmpty()) {
            return Collections.emptyList();
        }
        if (isTimedOut(deadlineNanos)) {
            return Collections.emptyList();
        }
        PriorityQueue<PrefixState> pairLevelHeap = createNextLevelHeap(nextLevelWidth);
        IntMinHeap topPairScores = beamWidth > 0 ? new IntMinHeap(beamWidth) : null;
        BitSet intersectionScratch = getScratchBitSet(scratchBitSize);
        SoftTidsetCache tidsetCache = getSoftTidsetCache();
        int timeoutCheckCounter = 0;

        for (PrefixState prefix : firstLevel) {
            int nextStartPos = prefix.lastPosition + 1;
            if (nextStartPos >= frequentTermIds.length) {
                continue;
            }
            int maxChildSupportUpperBound = Math.min(prefix.support, suffixMaxSupportByPos[nextStartPos]);
            if (maxChildSupportUpperBound < config.getMinSupport()) {
                miningState.prunedBySupersetSupportUpperBound++;
                continue;
            }
            int expandedChildren = 0;
            for (int nextPos = nextStartPos; nextPos < frequentTermIds.length; nextPos++) {
                if ((++timeoutCheckCounter % EngineTuningConfig.TIMEOUT_CHECK_INTERVAL) == 0 && isTimedOut(deadlineNanos)) {
                    return toSortedList(pairLevelHeap);
                }
                if (maxBranchingFactor > 0 && expandedChildren >= maxBranchingFactor) {
                    miningState.branchLimitHits++;
                    break;
                }
                int termId = frequentTermIds[nextPos];
                int termSupport = supportsByTermId[termId];
                int upperBound = prefix.upperBoundWith(termSupport);
                if (upperBound < config.getMinSupport()) {
                    miningState.prunedBySupportUpperBound++;
                    continue;
                }
                int optimisticSaving = score(2, upperBound);
                if (canPruneByCandidateFloor(miningState, optimisticSaving)) {
                    miningState.prunedByCandidateFloor++;
                    continue;
                }
                int[] childTermIds = appendTermId(prefix, termId);
                BitSet nextTidset = tidsetCache.get(childTermIds);
                int support;
                if (nextTidset != null) {
                    support = nextTidset.cardinality();
                } else {
                    intersectionScratch.clear();
                    BitSet termTidset = tidsetsByTermId.get(termId);
                    int termTidsetWords = tidsetWordsByTermId[termId];
                    if (prefix.tidsetWordsLength <= termTidsetWords) {
                        intersectionScratch.or(prefix.tidset);
                        intersectionScratch.and(termTidset);
                    } else {
                        intersectionScratch.or(termTidset);
                        intersectionScratch.and(prefix.tidset);
                    }
                    miningState.intersectionCount++;
                    support = intersectionScratch.cardinality();
                }
                if (support < config.getMinSupport()) {
                    miningState.prunedByMinSupportAfterIntersect++;
                    continue;
                }
                expandedChildren++;
                int pairScore = score(2, support);
                if (canPruneChildByScore(topPairScores, beamWidth, pairScore)) {
                    miningState.prunedByChildScore++;
                    continue;
                }
                BitSet pairTidset;
                if (nextTidset != null) {
                    pairTidset = nextTidset;
                } else {
                    pairTidset = (BitSet) intersectionScratch.clone();
                    tidsetCache.put(childTermIds, pairTidset);
                }
                PrefixState pairPrefix = new PrefixState(
                        prefix,
                        termId,
                        pairTidset,
                        nextPos,
                        2,
                        support,
                        pairScore,
                        bitSetWordSize(pairTidset)
                );
                addToNextLevelHeap(pairLevelHeap, nextLevelWidth, pairPrefix);
                updateTopChildScores(topPairScores, beamWidth, pairScore);
            }
        }
        return toSortedList(pairLevelHeap);
    }

    /** 扩展一层前缀并返回下一层。 */
    private LevelExpansionResult expandLevel(
            List<PrefixState> currentLevel,
            int[] frequentTermIds,
            List<BitSet> tidsetsByTermId,
            int[] supportsByTermId,
            int[] tidsetWordsByTermId,
            int[] suffixMaxSupportByPos,
            SelectorConfig config,
            int maxBranchingFactor,
            int beamWidth,
            int nextLevelWidth,
            int scratchBitSize,
            long deadlineNanos,
            MiningState miningState
    ) {
        if (currentLevel.isEmpty()) {
            return new LevelExpansionResult(Collections.<PrefixState>emptyList(), 0, false);
        }
        if (isTimedOut(deadlineNanos)) {
            return new LevelExpansionResult(Collections.<PrefixState>emptyList(), 0, true);
        }
        PriorityQueue<PrefixState> nextLevelHeap = createNextLevelHeap(nextLevelWidth);
        int discoveredThisLevel = 0;
        BitSet intersectionScratch = getScratchBitSet(scratchBitSize);
        SoftTidsetCache tidsetCache = getSoftTidsetCache();
        IntMinHeap topChildScores = beamWidth > 0
                ? new IntMinHeap(beamWidth)
                : null;
        boolean timedOut = false;
        int timeoutCheckCounter = 0;

        for (PrefixState prefix : currentLevel) {
            int remainingTerms = frequentTermIds.length - (prefix.lastPosition + 1);
            // 即使把后续词都加上，也达不到最小项集长度，提前跳过这个前缀。
            if (prefix.length + remainingTerms < config.getMinItemsetSize()) {
                miningState.prunedByLengthFeasibility++;
                continue;
            }
            int nextStartPos = prefix.lastPosition + 1;
            if (nextStartPos >= frequentTermIds.length) {
                continue;
            }
            int maxSupersetSupportUpperBound = Math.min(prefix.support, suffixMaxSupportByPos[nextStartPos]);
            if (maxSupersetSupportUpperBound < config.getMinSupport()) {
                miningState.prunedBySupersetSupportUpperBound++;
                continue;
            }
            int optimisticDescendantScore = optimisticDescendantScore(
                    prefix.length + 1,
                    remainingTerms,
                    config.getMaxItemsetSize(),
                    prefix.support
            );
            // 当前前缀在“最大可达长度”下的理论最高分都进不了本层 topK 时，整条前缀直接跳过。
            if (canPrunePrefixByScore(topChildScores, beamWidth, optimisticDescendantScore)) {
                miningState.prunedByPrefixScore++;
                continue;
            }
            int expandedChildren = 0;
            for (int nextPos = nextStartPos; nextPos < frequentTermIds.length; nextPos++) {
                if ((++timeoutCheckCounter % EngineTuningConfig.TIMEOUT_CHECK_INTERVAL) == 0 && isTimedOut(deadlineNanos)) {
                    timedOut = true;
                    break;
                }
                if (maxBranchingFactor > 0 && expandedChildren >= maxBranchingFactor) {
                    miningState.branchLimitHits++;
                    break;
                }
                int termId = frequentTermIds[nextPos];
                // 支持度上界剪枝：交集支持度不可能超过两侧支持度较小者。
                int termSupport = supportsByTermId[termId];
                int upperBound = prefix.upperBoundWith(termSupport);
                if (upperBound < config.getMinSupport()) {
                    miningState.prunedBySupportUpperBound++;
                    continue;
                }
                int optimisticSaving = score(prefix.length + 1, upperBound);
                if (canPruneByCandidateFloor(miningState, optimisticSaving)) {
                    miningState.prunedByCandidateFloor++;
                    continue;
                }
                int[] childTermIds = appendTermId(prefix, termId);
                BitSet nextTidset = tidsetCache.get(childTermIds);
                int support;
                if (nextTidset != null) {
                    support = nextTidset.cardinality();
                } else {
                    // 先在临时位图上算交集，只有达标时才 clone，减少无效对象创建。
                    intersectionScratch.clear();
                    BitSet termTidset = tidsetsByTermId.get(termId);
                    int termTidsetWords = tidsetWordsByTermId[termId];
                    // 先从更短的位图开始，通常可降低 OR/AND 的实际遍历成本。
                    if (prefix.tidsetWordsLength <= termTidsetWords) {
                        intersectionScratch.or(prefix.tidset);
                        intersectionScratch.and(termTidset);
                    } else {
                        intersectionScratch.or(termTidset);
                        intersectionScratch.and(prefix.tidset);
                    }
                    miningState.intersectionCount++;
                    support = intersectionScratch.cardinality();
                }
                if (support < config.getMinSupport()) {
                    miningState.prunedByMinSupportAfterIntersect++;
                    continue;
                }
                expandedChildren++;
                int nextLen = prefix.length + 1;
                int childScore = score(nextLen, support);
                int childRemainingTerms = frequentTermIds.length - (nextPos + 1);
                int childOptimisticDescendantScore = optimisticDescendantScore(
                        nextLen,
                        childRemainingTerms,
                        config.getMaxItemsetSize(),
                        support
                );
                // 当本层 topK 门槛已形成时，仅在 child 的“可达最大长度乐观分”也低于门槛时才剪枝，
                // 避免过早丢掉潜在高价值长项集路径。
                if (canPruneChildByScore(topChildScores, beamWidth, childOptimisticDescendantScore)) {
                    miningState.prunedByChildScore++;
                    continue;
                }
                if (nextTidset == null) {
                    nextTidset = (BitSet) intersectionScratch.clone();
                    tidsetCache.put(childTermIds, nextTidset);
                }
                PrefixState child = new PrefixState(
                        prefix,
                        termId,
                        nextTidset,
                        nextPos,
                        nextLen,
                        support,
                        childScore,
                        bitSetWordSize(nextTidset)
                );
                addToNextLevelHeap(nextLevelHeap, nextLevelWidth, child);
                discoveredThisLevel++;
                updateTopChildScores(topChildScores, beamWidth, childOptimisticDescendantScore);

                if (nextLen >= config.getMinItemsetSize()
                        && appendCandidate(materializeTermIds(child), nextTidset, support, miningState)) {
                    return new LevelExpansionResult(toSortedList(nextLevelHeap), discoveredThisLevel, timedOut);
                }
            }
            if (timedOut) {
                break;
            }
        }
        return new LevelExpansionResult(toSortedList(nextLevelHeap), discoveredThisLevel, timedOut);
    }

    /** 估算 scratch BitSet 初始容量，减少交集过程中 BitSet 内部扩容。 */
    private int estimateScratchBitSize(List<BitSet> tidsetsByTermId) {
        int maxLength = 0;
        for (int i = 0; i < tidsetsByTermId.size(); i++) {
            int len = tidsetsByTermId.get(i).length();
            if (len > maxLength) {
                maxLength = len;
            }
        }
        return Math.max(EngineTuningConfig.MIN_BITSET_CAPACITY, maxLength);
    }

    /** 统计每个 term 位图实际占用的 word 数。 */
    private int[] collectTidsetWordSizes(List<BitSet> tidsetsByTermId) {
        int[] out = new int[tidsetsByTermId.size()];
        for (int i = 0; i < tidsetsByTermId.size(); i++) {
            out[i] = bitSetWordSize(tidsetsByTermId.get(i));
        }
        return out;
    }

    /** 估算 BitSet 当前有效 word 数（每个 word = 64 bits）。 */
    private int bitSetWordSize(BitSet bitSet) {
        int length = bitSet.length();
        return length <= 0 ? 0 : ((length - 1) / EngineTuningConfig.BITS_PER_WORD) + 1;
    }

    /** 若前缀可达到的最高 child 分数都低于当前 topK 门槛，则可安全剪枝。 */
    private boolean canPrunePrefixByScore(
            IntMinHeap topChildScores,
            int beamWidth,
            int optimisticChildScore
    ) {
        if (beamWidth <= 0 || topChildScores == null || topChildScores.size() < beamWidth) {
            return false;
        }
        return optimisticChildScore < topChildScores.peek();
    }

    /** 若 child 分数已低于当前 topK 门槛，则不再创建 child 对象。 */
    private boolean canPruneChildByScore(
            IntMinHeap topChildScores,
            int beamWidth,
            int childScore
    ) {
        if (beamWidth <= 0 || topChildScores == null || topChildScores.size() < beamWidth) {
            return false;
        }
        return childScore < topChildScores.peek();
    }

    /** 维护“当前已发现 child 分数”的 topK 最小堆，堆顶即当前门槛分。 */
    private void updateTopChildScores(IntMinHeap topChildScores, int beamWidth, int childScore) {
        if (beamWidth <= 0 || topChildScores == null) {
            return;
        }
        if (topChildScores.size() < beamWidth) {
            topChildScores.add(childScore);
            return;
        }
        int threshold = topChildScores.peek();
        if (childScore > threshold) {
            topChildScores.poll();
            topChildScores.add(childScore);
        }
    }

    /** 每层扩展后更新早停条件。 */
    private boolean shouldStopAfterLevel(
            LevelExpansionResult expansionResult,
            int maxIdleLevels,
            MiningState miningState
    ) {
        if (expansionResult.timedOut) {
            return true;
        }
        if (expansionResult.discoveredCount == 0) {
            miningState.idleLevelCount++;
            return maxIdleLevels > 0 && miningState.idleLevelCount >= maxIdleLevels;
        }
        miningState.idleLevelCount = 0;
        return false;
    }

    /** 判断是否达到运行时间上限。 */
    private boolean isTimedOut(long deadlineNanos) {
        return deadlineNanos > 0L && System.nanoTime() >= deadlineNanos;
    }

    /** 线程本地复用 scratch BitSet，按需扩容后清空再返回。 */
    private BitSet getScratchBitSet(int expectedBits) {
        ScratchBitSet holder = threadLocalScratch.get();
        int safeExpectedBits = Math.max(EngineTuningConfig.MIN_BITSET_CAPACITY, expectedBits);
        if (safeExpectedBits > holder.capacityBits) {
            holder.bitSet = new BitSet(safeExpectedBits);
            holder.capacityBits = safeExpectedBits;
        } else if (holder.capacityBits > EngineTuningConfig.MIN_BITSET_CAPACITY
                && safeExpectedBits * SCRATCH_SHRINK_TRIGGER_DIVISOR < holder.capacityBits) {
            int shrinkTarget = Math.max(
                    EngineTuningConfig.MIN_BITSET_CAPACITY,
                    safeExpectedBits * SCRATCH_SHRINK_HEADROOM_MULTIPLIER
            );
            holder.bitSet = new BitSet(shrinkTarget);
            holder.capacityBits = shrinkTarget;
        }
        holder.bitSet.clear();
        return holder.bitSet;
    }

    private SoftTidsetCache getSoftTidsetCache() {
        return threadLocalTidsetCache.get();
    }

    private void prepareTidsetCacheForRun() {
        if (!Boolean.parseBoolean(System.getProperty(REUSE_TIDSET_CACHE_ACROSS_CALLS_PROPERTY, "false"))) {
            getSoftTidsetCache().clear();
        }
    }

    private int[] appendTermId(PrefixState prefix, int termId) {
        CompactPathStack stack = threadLocalPathStack.get();
        return stack.materializeWithAppend(prefix, termId);
    }

    private void clearThreadLocalScratch() {
        threadLocalScratch.remove();
        threadLocalPathStack.remove();
    }

    /**
     * 追加一条候选并更新计数。
     *
     * <p>候选容器采用 bounded top-k：当达到上限后，仅在新候选更优时替换当前最差候选。
     */
    private boolean appendCandidate(
            int[] termIds,
            BitSet tidset,
            int support,
            MiningState miningState
    ) {
        CandidateItemset candidate = CandidateItemset.trusted(termIds, tidset, support);
        if (miningState.topCandidatesHeap.size() < miningState.maxCandidateCount) {
            miningState.topCandidatesHeap.offer(candidate);
            miningState.generatedCandidateCount++;
            updateCandidateFloor(miningState);
            return false;
        }
        miningState.truncatedByCandidateLimit = true;
        CandidateItemset currentWorst = miningState.topCandidatesHeap.peek();
        if (currentWorst != null && compareCandidateWorstFirst(candidate, currentWorst) > 0) {
            miningState.topCandidatesHeap.poll();
            miningState.topCandidatesHeap.offer(candidate);
            updateCandidateFloor(miningState);
            return false;
        }
        return false;
    }

    /** 将内部可变状态打包为最终返回对象。 */
    private FrequentItemsetMiningResult toResult(MiningState miningState, int frequentTermCount) {
        return result(
                miningState.materializeTopCandidates(),
                frequentTermCount,
                miningState.generatedCandidateCount,
                miningState.intersectionCount,
                miningState.truncatedByCandidateLimit
        );
    }

    private static boolean isDebugLogEnabled() {
        return Boolean.parseBoolean(System.getProperty(DEBUG_LOG_PROPERTY, "false"));
    }

    private static void logMiningSummary(MiningState s, int frequentTermCount, boolean earlyStopAtFirstLevel) {
        LOG.log(Level.INFO,
                "[fptoken-miner] summary frequentTerms={0}, generatedCandidates={1}, intersections={2}, truncatedByCandidateLimit={3}, earlyStopAtFirstLevel={4}, timedOutStop={5}, idleStop={6}, prunedByLengthFeasibility={7}, prunedByPrefixScore={8}, prunedByChildScore={9}, prunedBySupportUpperBound={10}, prunedBySupersetSupportUpperBound={11}, prunedByCandidateFloor={12}, prunedByMinSupportAfterIntersect={13}, branchLimitHits={14}",
                new Object[] {
                        frequentTermCount,
                        s.generatedCandidateCount,
                        s.intersectionCount,
                        s.truncatedByCandidateLimit,
                        earlyStopAtFirstLevel,
                        s.timedOutStop,
                        s.idleStop,
                        s.prunedByLengthFeasibility,
                        s.prunedByPrefixScore,
                        s.prunedByChildScore,
                        s.prunedBySupportUpperBound,
                        s.prunedBySupersetSupportUpperBound,
                        s.prunedByCandidateFloor,
                        s.prunedByMinSupportAfterIntersect,
                        s.branchLimitHits
                });
    }

    /** 统一在这里组装返回对象，避免多处手写参数顺序。 */
    private FrequentItemsetMiningResult result(
            List<CandidateItemset> out,
            int frequentTermCount,
            int generatedCandidateCount,
            int intersectionCount,
            boolean truncatedByCandidateLimit
    ) {
        return new FrequentItemsetMiningResult(
                out,
                frequentTermCount,
                generatedCandidateCount,
                intersectionCount,
                truncatedByCandidateLimit
        );
    }

    /**
     * 用频繁词列表里的一个词，构造 1-项前缀状态。
     * <p>这里会 clone 一份位图，避免后续 and 运算改到原始索引数据。
     */
    private PrefixState singleTermPrefix(
            int frequentTermIndex,
            int[] frequentTermIds,
            List<BitSet> tidsetsByTermId,
            int[] supportsByTermId,
            int[] tidsetWordsByTermId
    ) {
        int termId = frequentTermIds[frequentTermIndex];
        BitSet tidset = (BitSet) tidsetsByTermId.get(termId).clone();
        int support = supportsByTermId[termId];
        return new PrefixState(
                null,
                termId,
                tidset,
                frequentTermIndex,
                1,
                support,
                score(1, support),
                tidsetWordsByTermId[termId]
        );
    }

    /**
     * 收集所有达标的 1-项（单词），按支持度从高到低排序。
     * 如有需要，只截取前 maxFrequentTermCount 个高频词。
     *
     * @param supportsByTermId 输出参数：记录每个 termId 的支持度，供排序时复用
     */
    private int[] collectFrequentTermIds(
            List<BitSet> tidsetsByTermId,
            int minSupport,
            int[] supportsByTermId,
            int maxFrequentTermCount
    ) {
        if (maxFrequentTermCount <= 0) {
            int[] frequentTermIds = new int[Math.max(8, tidsetsByTermId.size())];
            int count = 0;
            for (int termId = 0; termId < tidsetsByTermId.size(); termId++) {
                int support = tidsetsByTermId.get(termId).cardinality();
                supportsByTermId[termId] = support;
                if (support >= minSupport) {
                    if (count >= frequentTermIds.length) {
                        frequentTermIds = Arrays.copyOf(frequentTermIds, frequentTermIds.length * 2);
                    }
                    frequentTermIds[count++] = termId;
                }
            }
            int[] out = Arrays.copyOf(frequentTermIds, count);
            sortTermIdsBestFirst(out, supportsByTermId);
            return out;
        }

        IntTermMinHeap topMHeap = new IntTermMinHeap(maxFrequentTermCount, supportsByTermId);

        for (int termId = 0; termId < tidsetsByTermId.size(); termId++) {
            int support = tidsetsByTermId.get(termId).cardinality();
            supportsByTermId[termId] = support;
            if (support >= minSupport) {
                if (topMHeap.size() < maxFrequentTermCount) {
                    topMHeap.offer(termId);
                    continue;
                }
                int currentWorst = topMHeap.peek();
                if (compareTermBest(termId, currentWorst, supportsByTermId) < 0) {
                    topMHeap.poll();
                    topMHeap.offer(termId);
                }
            }
        }

        int[] out = topMHeap.toArray();
        sortTermIdsBestFirst(out, supportsByTermId);
        return out;
    }

    private void sortTermIdsBestFirst(int[] termIds, int[] supportsByTermId) {
        for (int i = 1; i < termIds.length; i++) {
            int v = termIds[i];
            int j = i - 1;
            while (j >= 0 && compareTermBest(termIds[j], v, supportsByTermId) > 0) {
                termIds[j + 1] = termIds[j];
                j--;
            }
            termIds[j + 1] = v;
        }
    }

    private int[] buildSuffixMaxSupportByPos(int[] frequentTermIds, int[] supportsByTermId) {
        int[] suffixMax = new int[frequentTermIds.length + 1];
        int runningMax = 0;
        for (int i = frequentTermIds.length - 1; i >= 0; i--) {
            int support = supportsByTermId[frequentTermIds[i]];
            if (support > runningMax) {
                runningMax = support;
            }
            suffixMax[i] = runningMax;
        }
        suffixMax[frequentTermIds.length] = 0;
        return suffixMax;
    }

    /**
     * 在一层候选前缀里做 Top-K。
     * 这里用固定大小的最小堆，不再全量排序，复杂度从 O(NlogN) 降到 O(NlogK)。
     */
    private List<PrefixState> topK(List<PrefixState> items, int k) {
        if (k <= 0) {
            return Collections.emptyList();
        }
        if (items.size() <= k) {
            return items;
        }

        PriorityQueue<PrefixState> heap = new PriorityQueue<>(k, PREFIX_WORST_FIRST);
        for (PrefixState item : items) {
            if (heap.size() < k) {
                heap.offer(item);
                continue;
            }
            if (PREFIX_WORST_FIRST.compare(item, heap.peek()) > 0) {
                heap.poll();
                heap.offer(item);
            }
        }

        // 返回前按“最好优先”的顺序排好，保持原来输出语义。
        List<PrefixState> sortedTopK = new ArrayList<>(heap);
        Collections.sort(sortedTopK, PREFIX_BEST_FIRST);
        return sortedTopK;
    }

    /** 统一比较规则：score 高优先；同分优先更长项集，再按位置稳定打破平局。 */
    private static int comparePrefixBest(PrefixState a, PrefixState b) {
        int scoreCompare = Integer.compare(b.score, a.score);
        if (scoreCompare != 0) {
            return scoreCompare;
        }
        int lengthCompare = Integer.compare(b.length, a.length);
        if (lengthCompare != 0) {
            return lengthCompare;
        }
        return Integer.compare(a.lastPosition, b.lastPosition);
    }

    /** 创建下一层 bounded topK 堆（堆顶是当前最差前缀）。 */
    private PriorityQueue<PrefixState> createNextLevelHeap(int width) {
        if (width <= 0) {
            return new PriorityQueue<>(1, PREFIX_WORST_FIRST);
        }
        return new PriorityQueue<>(width, PREFIX_WORST_FIRST);
    }

    /** 将 child 放入 bounded topK 堆，超限时淘汰最差元素。 */
    private void addToNextLevelHeap(PriorityQueue<PrefixState> heap, int width, PrefixState child) {
        if (width <= 0) {
            return;
        }
        if (heap.size() < width) {
            heap.offer(child);
            return;
        }
        PrefixState worst = heap.peek();
        if (worst != null && comparePrefixBest(child, worst) >= 0) {
            return;
        }
        heap.poll();
        heap.offer(child);
    }

    /** 将 bounded heap 结果转成“最好优先”的有序列表。 */
    private List<PrefixState> toSortedList(PriorityQueue<PrefixState> heap) {
        if (heap == null || heap.isEmpty()) {
            return Collections.emptyList();
        }
        List<PrefixState> out = new ArrayList<>(heap);
        Collections.sort(out, PREFIX_BEST_FIRST);
        return out;
    }

    /** 统一比较规则：支持度高的更好；同支持度时 termId 小的更好。 */
    private int compareTermBest(int termA, int termB, int[] supportsByTermId) {
        int supportCompare = Integer.compare(supportsByTermId[termB], supportsByTermId[termA]);
        if (supportCompare != 0) {
            return supportCompare;
        }
        return Integer.compare(termA, termB);
    }

    /** 将链式前缀状态还原成对外使用的 termId 数组。 */
    private int[] materializeTermIds(PrefixState state) {
        return state.getTermIdsUnsafe();
    }

    /**
     * 前缀打分函数：长度越长、支持度越高，越容易留在 beam 里。
     * <p>公式：{@code (len - 1) * support}。
     */
    private int score(int len, int support) {
        // 热路径优化：默认评分直接走内联公式，避免每次接口动态派发。
        if (useDefaultScoreFormula) {
            return (len - 1) * support;
        }
        return scoreFunction.calculate(len, support);
    }

    /** 自适应 beam 宽度：层越深保留越少，降低深层搜索开销。 */
    private int adaptiveBeamWidth(int depth, int baseBeamWidth) {
        if (baseBeamWidth <= 0) {
            return EngineTuningConfig.MINER_FALLBACK_BEAM_WIDTH;
        }
        if (baseBeamWidth == 1) {
            return 1;
        }
        if (depth <= 2) {
            return baseBeamWidth;
        }
        if (depth <= 4) {
            return Math.max(1, baseBeamWidth / EngineTuningConfig.BEAM_WIDTH_DIVISOR_1);
        }
        return Math.max(1, baseBeamWidth / EngineTuningConfig.BEAM_WIDTH_DIVISOR_2);
    }

    private int guardedBeamWidth(int depth, int baseBeamWidth, int maxItemsetSize) {
        int adaptive = adaptiveBeamWidth(depth, baseBeamWidth);
        if (maxItemsetSize >= 3 && depth <= 2) {
            return Math.max(adaptive, SHALLOW_EXPLORATION_MIN_BEAM);
        }
        return adaptive;
    }

    private boolean canPruneByCandidateFloor(MiningState miningState, int optimisticSaving) {
        if (!miningState.truncatedByCandidateLimit || !miningState.candidateFloorEnabled) {
            return false;
        }
        double optimisticAfterDecay = optimisticSaving * EngineTuningConfig.CANDIDATE_OPTIMISTIC_DECAY;
        return optimisticAfterDecay <= miningState.candidateSavingFloor;
    }

    private int optimisticDescendantScore(
            int currentLength,
            int remainingTerms,
            int maxItemsetSize,
            int supportUpperBound
    ) {
        int maxReachableLength = Math.min(maxItemsetSize, currentLength + Math.max(0, remainingTerms));
        return score(maxReachableLength, supportUpperBound);
    }

    private void updateCandidateFloor(MiningState miningState) {
        if (miningState.topCandidatesHeap.isEmpty() || miningState.maxCandidateCount <= 0) {
            return;
        }
        int activationSize = Math.max(
                1,
                (int) Math.floor(miningState.maxCandidateCount * EngineTuningConfig.CANDIDATE_FLOOR_ACTIVATION_RATIO)
        );
        miningState.candidateFloorEnabled = miningState.topCandidatesHeap.size() >= activationSize;
        if (!miningState.candidateFloorEnabled) {
            return;
        }
        CandidateItemset worst = miningState.topCandidatesHeap.peek();
        if (worst != null) {
            miningState.candidateSavingFloor = worst.getEstimatedSaving();
        }
    }

    private static int compareCandidateBestFirst(CandidateItemset a, CandidateItemset b) {
        int savingCompare = Integer.compare(b.getEstimatedSaving(), a.getEstimatedSaving());
        if (savingCompare != 0) {
            return savingCompare;
        }
        int supportCompare = Integer.compare(b.getSupport(), a.getSupport());
        if (supportCompare != 0) {
            return supportCompare;
        }
        return Integer.compare(b.length(), a.length());
    }

    private static int compareCandidateWorstFirst(CandidateItemset a, CandidateItemset b) {
        int savingCompare = Integer.compare(a.getEstimatedSaving(), b.getEstimatedSaving());
        if (savingCompare != 0) {
            return savingCompare;
        }
        int supportCompare = Integer.compare(a.getSupport(), b.getSupport());
        if (supportCompare != 0) {
            return supportCompare;
        }
        return Integer.compare(a.length(), b.length());
    }

    /**
     * Beam 搜索里的一条“前缀状态”（搜索树节点）。
     *
     * <p>使用链式结构存储路径，只记录“父节点 + 当前 termId”，扩展时无需复制整条 termId 数组。
     * 需要输出完整 termIds 时，再通过 {@link #materializeTermIds(PrefixState)} 一次性还原。
     */
    private static final class PrefixState {
        /** 父前缀；root 为 null。 */
        private final PrefixState parent;
        /** 当前层新追加的 termId。 */
        private final int termId;
        /** 这个项集对应的共现文档位图。 */
        private final BitSet tidset;
        /** 最后一个词在频繁列表中的位置；后续只从它后面继续扩。 */
        private final int lastPosition;
        /** 当前前缀长度（term 个数）。 */
        private final int length;
        /** 当前项集支持度（命中文档数）。 */
        private final int support;
        /** 这个前缀的得分，用于每层做 topK。 */
        private final int score;
        /** 当前 tidset 实际占用的 64-bit word 数。 */
        private final int tidsetWordsLength;
        /** 延迟缓存：完整 termIds 数组。仅在需要输出候选时构建。 */
        private int[] cachedTermIds;

        private PrefixState(
                PrefixState parent,
                int termId,
                BitSet tidset,
                int lastPosition,
                int length,
                int support,
                int score,
                int tidsetWordsLength
        ) {
            this.parent = parent;
            this.termId = termId;
            this.tidset = tidset;
            this.lastPosition = lastPosition;
            this.length = length;
            this.support = support;
            this.score = score;
            this.tidsetWordsLength = tidsetWordsLength;
        }

        /** 计算与另一个 term 的交集支持度理论上界。 */
        private int upperBoundWith(int termSupport) {
            return Math.min(support, termSupport);
        }

        /** 获取完整 termIds；首次按链表物化并缓存，后续复用同一只读数组。 */
        private int[] getTermIdsUnsafe() {
            if (cachedTermIds == null) {
                cachedTermIds = materializeFromChain();
            }
            return cachedTermIds;
        }

        /** 将链式路径还原成完整 termIds。 */
        private int[] materializeFromChain() {
            int[] out = new int[length];
            PrefixState cursor = this;
            for (int i = length - 1; i >= 0; i--) {
                out[i] = cursor.termId;
                cursor = cursor.parent;
            }
            return out;
        }
    }

    /** 单层扩展的返回值：下一层前缀、发现数、是否超时。 */
    private static final class LevelExpansionResult {
        private final List<PrefixState> nextLevel;
        private final int discoveredCount;
        private final boolean timedOut;

        private LevelExpansionResult(List<PrefixState> nextLevel, int discoveredCount, boolean timedOut) {
            this.nextLevel = nextLevel;
            this.discoveredCount = discoveredCount;
            this.timedOut = timedOut;
        }
    }

    /** 扩展层局部 topK 分数的轻量最小堆，避免 Integer 装箱。 */
    private static final class IntMinHeap {
        private final int[] heap;
        private int size;

        private IntMinHeap(int capacity) {
            this.heap = new int[Math.max(1, capacity)];
        }

        private int size() {
            return size;
        }

        private int peek() {
            return heap[0];
        }

        private void add(int value) {
            heap[size] = value;
            siftUp(size);
            size++;
        }

        private int poll() {
            int out = heap[0];
            size--;
            heap[0] = heap[size];
            siftDown(0);
            return out;
        }

        private void siftUp(int idx) {
            while (idx > 0) {
                int p = (idx - 1) >>> 1;
                if (heap[p] <= heap[idx]) {
                    break;
                }
                int t = heap[p];
                heap[p] = heap[idx];
                heap[idx] = t;
                idx = p;
            }
        }

        private void siftDown(int idx) {
            while (true) {
                int l = (idx << 1) + 1;
                if (l >= size) {
                    return;
                }
                int r = l + 1;
                int s = (r < size && heap[r] < heap[l]) ? r : l;
                if (heap[idx] <= heap[s]) {
                    return;
                }
                int t = heap[idx];
                heap[idx] = heap[s];
                heap[s] = t;
                idx = s;
            }
        }
    }

    /** termId 的有界最小堆（堆顶=当前最差候选），避免 Integer 装箱。 */
    private static final class IntTermMinHeap {
        private final int[] heap;
        private final int[] supportsByTermId;
        private int size;

        private IntTermMinHeap(int capacity, int[] supportsByTermId) {
            this.heap = new int[Math.max(1, capacity)];
            this.supportsByTermId = supportsByTermId;
        }

        private int size() {
            return size;
        }

        private int peek() {
            return heap[0];
        }

        private void offer(int termId) {
            heap[size] = termId;
            siftUp(size);
            size++;
        }

        private int poll() {
            int out = heap[0];
            size--;
            heap[0] = heap[size];
            siftDown(0);
            return out;
        }

        private int[] toArray() {
            return Arrays.copyOf(heap, size);
        }

        private void siftUp(int idx) {
            while (idx > 0) {
                int p = (idx - 1) >>> 1;
                if (compareTermWorstFirst(heap[p], heap[idx]) <= 0) {
                    break;
                }
                int t = heap[p];
                heap[p] = heap[idx];
                heap[idx] = t;
                idx = p;
            }
        }

        private void siftDown(int idx) {
            while (true) {
                int l = (idx << 1) + 1;
                if (l >= size) {
                    return;
                }
                int r = l + 1;
                int s = l;
                if (r < size && compareTermWorstFirst(heap[r], heap[l]) < 0) {
                    s = r;
                }
                if (compareTermWorstFirst(heap[idx], heap[s]) <= 0) {
                    return;
                }
                int t = heap[idx];
                heap[idx] = heap[s];
                heap[s] = t;
                idx = s;
            }
        }

        /** 最差优先：支持度低更差；同支持度 termId 大更差。 */
        private int compareTermWorstFirst(int a, int b) {
            int supportCompare = Integer.compare(supportsByTermId[a], supportsByTermId[b]);
            if (supportCompare != 0) {
                return supportCompare;
            }
            return Integer.compare(b, a);
        }
    }

    /** 线程本地 scratch 位图及其容量记录。 */
    private static final class ScratchBitSet {
        private BitSet bitSet;
        private int capacityBits;

        private ScratchBitSet(int capacityBits) {
            this.bitSet = new BitSet(capacityBits);
            this.capacityBits = capacityBits;
        }
    }

    /**
     * 紧凑路径栈：以可复用 int[] 作为 DFS 路径缓冲，避免每次扩展都先拷贝父路径再 append。
     * 最终仍返回不可共享的 termIds 数组，保证缓存键与候选对象语义稳定。
     */
    private static final class CompactPathStack {
        private int[] buffer;

        private CompactPathStack(int initialCapacity) {
            this.buffer = new int[Math.max(8, initialCapacity)];
        }

        private int[] materializeWithAppend(PrefixState prefix, int appendTermId) {
            int size = prefix.length + 1;
            ensureCapacity(size);
            int idx = prefix.length - 1;
            PrefixState cursor = prefix;
            while (cursor != null && idx >= 0) {
                buffer[idx--] = cursor.termId;
                cursor = cursor.parent;
            }
            buffer[size - 1] = appendTermId;
            return Arrays.copyOf(buffer, size);
        }

        private void ensureCapacity(int required) {
            if (required <= buffer.length) {
                return;
            }
            int newCap = buffer.length;
            while (newCap < required) {
                newCap = Math.max(newCap * 2, required);
            }
            buffer = Arrays.copyOf(buffer, newCap);
        }
    }

    private static final class SoftTidsetCache {
        private final int maxEntries;
        private final LinkedHashMap<IntArrayKey, SoftReference<BitSet>> map;

        private SoftTidsetCache(final int maxEntries) {
            this.maxEntries = Math.max(64, maxEntries);
            this.map = new LinkedHashMap<IntArrayKey, SoftReference<BitSet>>(this.maxEntries, 0.75f, true) {
                private static final long serialVersionUID = 1L;

                @Override
                protected boolean removeEldestEntry(Map.Entry<IntArrayKey, SoftReference<BitSet>> eldest) {
                    return size() > SoftTidsetCache.this.maxEntries;
                }
            };
        }

        private BitSet get(int[] termIds) {
            SoftReference<BitSet> ref = map.get(new IntArrayKey(termIds));
            if (ref == null) {
                return null;
            }
            BitSet cached = ref.get();
            if (cached == null) {
                map.remove(new IntArrayKey(termIds));
            }
            return cached;
        }

        private void put(int[] termIds, BitSet tidset) {
            map.put(new IntArrayKey(termIds), new SoftReference<BitSet>(tidset));
        }

        private void clear() {
            map.clear();
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

    /** 挖掘过程中的共享可变状态，集中管理计数与候选输出。 */
    private static final class MiningState {
        private final List<CandidateItemset> out;
        private final PriorityQueue<CandidateItemset> topCandidatesHeap;
        private final int maxCandidateCount;
        private int generatedCandidateCount;
        private int intersectionCount;
        private boolean truncatedByCandidateLimit;
        private int idleLevelCount;
        private int candidateSavingFloor;
        private boolean candidateFloorEnabled;
        private long prunedByLengthFeasibility;
        private long prunedByPrefixScore;
        private long prunedByChildScore;
        private long prunedBySupportUpperBound;
        private long prunedBySupersetSupportUpperBound;
        private long prunedByCandidateFloor;
        private long prunedByMinSupportAfterIntersect;
        private long branchLimitHits;
        private boolean timedOutStop;
        private boolean idleStop;

        private MiningState(List<CandidateItemset> out, int maxCandidateCount) {
            this.out = out;
            this.maxCandidateCount = Math.max(1, maxCandidateCount);
            this.topCandidatesHeap = new PriorityQueue<>(this.maxCandidateCount, CANDIDATE_WORST_FIRST);
        }

        private List<CandidateItemset> materializeTopCandidates() {
            if (topCandidatesHeap.isEmpty()) {
                return Collections.emptyList();
            }
            out.clear();
            out.addAll(topCandidatesHeap);
            Collections.sort(out, CANDIDATE_BEST_FIRST);
            return out;
        }
    }
}
