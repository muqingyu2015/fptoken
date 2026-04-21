package cn.lxdb.plugins.muqingyu.fptoken.miner;

import cn.lxdb.plugins.muqingyu.fptoken.config.EngineTuningConfig;
import cn.lxdb.plugins.muqingyu.fptoken.config.SelectorConfig;
import cn.lxdb.plugins.muqingyu.fptoken.model.CandidateItemset;
import cn.lxdb.plugins.muqingyu.fptoken.model.FrequentItemsetMiningResult;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

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

    @FunctionalInterface
    public interface ScoreFunction {
        int calculate(int length, int support);
    }

    private final ScoreFunction scoreFunction;
    /** 单线程场景下复用的临时位图，避免频繁分配。 */
    private BitSet pooledScratch = new BitSet(EngineTuningConfig.MIN_BITSET_CAPACITY);
    /** 当前 pooledScratch 的目标容量记录。 */
    private int pooledScratchCapacityBits = EngineTuningConfig.MIN_BITSET_CAPACITY;

    public BeamFrequentItemsetMiner() {
        this(null);
    }

    public BeamFrequentItemsetMiner(ScoreFunction scoreFunction) {
        this.scoreFunction = scoreFunction != null ? scoreFunction : EngineTuningConfig::defaultScore;
    }

    /**
     * 执行挖掘主流程，返回候选项集和统计信息。
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
        return mineWithStats(
                tidsetsByTermId,
                config,
                maxFrequentTermCount,
                maxBranchingFactor,
                beamWidth,
                0,
                0L
        );
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

        int safeBeamWidth = beamWidth > 0 ? beamWidth : EngineTuningConfig.MINER_FALLBACK_BEAM_WIDTH;
        MiningState miningState = new MiningState(new ArrayList<>(
                Math.min(config.getMaxCandidateCount(), EngineTuningConfig.MAX_INITIAL_CANDIDATE_CAPACITY)
        ));
        int scratchBitSize = estimateScratchBitSize(tidsetsByTermId);
        long deadlineNanos = maxRuntimeMillis > 0
                ? System.nanoTime() + maxRuntimeMillis * 1_000_000L
                : 0L;

        List<PrefixState> currentLevel = performFirstLevelExpansion(
                frequentTermIdsArray,
                tidsetsByTermId,
                supportsByTermId,
                tidsetWordsByTermId,
                config,
                miningState
        );
        if (miningState.truncatedByCandidateLimit) {
            return toResult(miningState, frequentTermIdsArray.length);
        }
        // 先做一次 beam 截断，只保留头部前缀，避免后续扩展爆炸。
        currentLevel = topK(currentLevel, safeBeamWidth);

        // 从 2-项集开始逐层扩展，直到配置的最大项集长度。
        for (int depth = 1; depth < config.getMaxItemsetSize(); depth++) {
            if (isTimedOut(deadlineNanos) || currentLevel.isEmpty()) {
                break;
            }
            int adaptiveWidth = adaptiveBeamWidth(depth, safeBeamWidth);
            LevelExpansionResult expansionResult = expandLevel(
                    currentLevel,
                    frequentTermIdsArray,
                    tidsetsByTermId,
                    supportsByTermId,
                    tidsetWordsByTermId,
                    config,
                    maxBranchingFactor,
                    safeBeamWidth,
                    adaptiveWidth,
                    scratchBitSize,
                    deadlineNanos,
                    miningState
            );
            if (miningState.truncatedByCandidateLimit) {
                return toResult(miningState, frequentTermIdsArray.length);
            }
            currentLevel = expansionResult.nextLevel;
            if (shouldStopAfterLevel(expansionResult, maxIdleLevels, miningState)) {
                break;
            }
        }

        return toResult(miningState, frequentTermIdsArray.length);
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
        List<Integer> frequentTermIds = collectFrequentTermIds(
                tidsetsByTermId,
                minSupport,
                supportsByTermId,
                maxFrequentTermCount
        );
        return toIntArray(frequentTermIds);
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
                    && appendCandidate(materializeTermIds(state), state.tidset, state.support, config, miningState)) {
                break;
            }
        }
        return currentLevel;
    }

    /** 扩展一层前缀并返回下一层。 */
    private LevelExpansionResult expandLevel(
            List<PrefixState> currentLevel,
            int[] frequentTermIds,
            List<BitSet> tidsetsByTermId,
            int[] supportsByTermId,
            int[] tidsetWordsByTermId,
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
        IntMinHeap topChildScores = beamWidth > 0
                ? new IntMinHeap(beamWidth)
                : null;
        boolean timedOut = false;
        int timeoutCheckCounter = 0;

        for (PrefixState prefix : currentLevel) {
            int remainingTerms = frequentTermIds.length - (prefix.lastPosition + 1);
            // 即使把后续词都加上，也达不到最小项集长度，提前跳过这个前缀。
            if (prefix.length + remainingTerms < config.getMinItemsetSize()) {
                continue;
            }
            int optimisticChildScore = score(prefix.length + 1, prefix.support);
            // 当前前缀理论最高分都进不了本层 topK 时，整条前缀直接跳过。
            if (canPrunePrefixByScore(topChildScores, beamWidth, optimisticChildScore)) {
                continue;
            }
            int expandedChildren = 0;
            for (int nextPos = prefix.lastPosition + 1; nextPos < frequentTermIds.length; nextPos++) {
                if ((++timeoutCheckCounter % EngineTuningConfig.TIMEOUT_CHECK_INTERVAL) == 0 && isTimedOut(deadlineNanos)) {
                    timedOut = true;
                    break;
                }
                if (maxBranchingFactor > 0 && expandedChildren >= maxBranchingFactor) {
                    break;
                }
                int termId = frequentTermIds[nextPos];
                // 支持度上界剪枝：交集支持度不可能超过两侧支持度较小者。
                int termSupport = supportsByTermId[termId];
                int upperBound = prefix.upperBoundWith(termSupport);
                if (upperBound < config.getMinSupport()) {
                    continue;
                }
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

                int support = intersectionScratch.cardinality();
                if (support < config.getMinSupport()) {
                    continue;
                }
                expandedChildren++;
                int nextLen = prefix.length + 1;
                int childScore = score(nextLen, support);
                // 当本层 topK 门槛已形成时，低于门槛的 child 直接跳过，避免无效 append/clone。
                if (canPruneChildByScore(topChildScores, beamWidth, childScore)) {
                    continue;
                }
                BitSet nextTidset = (BitSet) intersectionScratch.clone();
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
                updateTopChildScores(topChildScores, beamWidth, childScore);

                if (nextLen >= config.getMinItemsetSize()
                        && appendCandidate(materializeTermIds(child), nextTidset, support, config, miningState)) {
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

    /** 单线程复用 scratch BitSet，按需扩容后清空再返回。 */
    private BitSet getScratchBitSet(int expectedBits) {
        if (expectedBits > pooledScratchCapacityBits) {
            pooledScratch = new BitSet(expectedBits);
            pooledScratchCapacityBits = expectedBits;
        }
        pooledScratch.clear();
        return pooledScratch;
    }

    /**
     * 追加一条候选并更新计数；若命中候选上限，返回 true 表示需要提前结束。
     */
    private boolean appendCandidate(
            int[] termIds,
            BitSet tidset,
            int support,
            SelectorConfig config,
            MiningState miningState
    ) {
        if (miningState.out.size() >= config.getMaxCandidateCount()) {
            miningState.truncatedByCandidateLimit = true;
            return true;
        }
        miningState.out.add(CandidateItemset.trusted(termIds, tidset, support));
        miningState.generatedCandidateCount++;
        if (miningState.out.size() >= config.getMaxCandidateCount()) {
            miningState.truncatedByCandidateLimit = true;
            return true;
        }
        return false;
    }

    /** 将内部可变状态打包为最终返回对象。 */
    private FrequentItemsetMiningResult toResult(MiningState miningState, int frequentTermCount) {
        return result(
                miningState.out,
                frequentTermCount,
                miningState.generatedCandidateCount,
                miningState.intersectionCount,
                miningState.truncatedByCandidateLimit
        );
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
    private List<Integer> collectFrequentTermIds(
            List<BitSet> tidsetsByTermId,
            int minSupport,
            int[] supportsByTermId,
            int maxFrequentTermCount
    ) {
        // 用于最终输出的排序规则：支持度高的在前；支持度一样时 termId 小的在前。
        Comparator<Integer> bestFirst = (a, b) ->
                compareTermBest(a.intValue(), b.intValue(), supportsByTermId);

        if (maxFrequentTermCount <= 0) {
            List<Integer> frequentTermIds = new ArrayList<>();
            for (int termId = 0; termId < tidsetsByTermId.size(); termId++) {
                int support = tidsetsByTermId.get(termId).cardinality();
                supportsByTermId[termId] = support;
                if (support >= minSupport) {
                    frequentTermIds.add(termId);
                }
            }
            Collections.sort(frequentTermIds, bestFirst);
            return frequentTermIds;
        }

        // 若需要截前 M 个，这个比较器把“最差的”放堆顶，方便替换。
        Comparator<Integer> worstFirst = (a, b) ->
                compareTermBest(b.intValue(), a.intValue(), supportsByTermId);
        PriorityQueue<Integer> topMHeap = new PriorityQueue<>(maxFrequentTermCount, worstFirst);

        for (int termId = 0; termId < tidsetsByTermId.size(); termId++) {
            int support = tidsetsByTermId.get(termId).cardinality();
            supportsByTermId[termId] = support;
            if (support >= minSupport) {
                if (topMHeap.size() < maxFrequentTermCount) {
                    topMHeap.offer(termId);
                    continue;
                }
                Integer currentWorst = topMHeap.peek();
                if (currentWorst != null && worstFirst.compare(termId, currentWorst) > 0) {
                    topMHeap.poll();
                    topMHeap.offer(termId);
                }
            }
        }

        List<Integer> out = new ArrayList<>(topMHeap);
        Collections.sort(out, bestFirst);
        return out;
    }

    private int[] toIntArray(List<Integer> ints) {
        int[] out = new int[ints.size()];
        for (int i = 0; i < ints.size(); i++) {
            out[i] = ints.get(i).intValue();
        }
        return out;
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

        Comparator<PrefixState> bestFirst = (a, b) -> comparePrefixBest(a, b);
        // 堆顶放“当前 K 个里最差的一个”，方便和新元素比较替换。
        Comparator<PrefixState> worstFirst = (a, b) -> comparePrefixBest(b, a);

        PriorityQueue<PrefixState> heap = new PriorityQueue<>(k, worstFirst);
        for (PrefixState item : items) {
            if (heap.size() < k) {
                heap.offer(item);
                continue;
            }
            if (worstFirst.compare(item, heap.peek()) > 0) {
                heap.poll();
                heap.offer(item);
            }
        }

        // 返回前按“最好优先”的顺序排好，保持原来输出语义。
        List<PrefixState> sortedTopK = new ArrayList<>(heap);
        Collections.sort(sortedTopK, bestFirst);
        return sortedTopK;
    }

    /** 统一比较规则：score 高优先；同分优先更长项集，再按位置稳定打破平局。 */
    private int comparePrefixBest(PrefixState a, PrefixState b) {
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
            return new PriorityQueue<>(1, (a, b) -> comparePrefixBest(b, a));
        }
        return new PriorityQueue<>(width, (a, b) -> comparePrefixBest(b, a));
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
        Collections.sort(out, (a, b) -> comparePrefixBest(a, b));
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

    /** 挖掘过程中的共享可变状态，集中管理计数与候选输出。 */
    private static final class MiningState {
        private final List<CandidateItemset> out;
        private int generatedCandidateCount;
        private int intersectionCount;
        private boolean truncatedByCandidateLimit;
        private int idleLevelCount;

        private MiningState(List<CandidateItemset> out) {
            this.out = out;
        }
    }
}
