package cn.lxdb.plugins.muqingyu.fptoken.miner;

import cn.lxdb.plugins.muqingyu.fptoken.config.SelectorConfig;
import cn.lxdb.plugins.muqingyu.fptoken.model.CandidateItemset;
import cn.lxdb.plugins.muqingyu.fptoken.model.FrequentItemsetMiningResult;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * 在垂直 tidset（{@code List<BitSet>}）上做束搜索（Beam Search）的近似频繁项集挖掘。
 *
 * <p><b>算法要点</b>：
 * <ol>
 *   <li>先收集满足 {@link SelectorConfig#getMinSupport()} 的 1-项，按支持度降序（同支持度按 termId 升序打破平局）。</li>
 *   <li>可选按 {@code maxFrequentTermCount} 截断，只保留头部高频词参与扩展。</li>
 *   <li>从 1-项前缀逐层扩展：仅向「频繁列表中更靠后的位置」加词，避免重复组合；新 tidset = 父 tidset ∧ 单项 tidset。</li>
 *   <li>每层结束后用 {@code beamWidth} 对「前缀状态」做 top-K（按 {@link #score(int, int)}），控制状态规模。</li>
 *   <li>每个前缀扩展时可用 {@code maxBranchingFactor} 限制子分支数；输出候选数受 {@link SelectorConfig#getMaxCandidateCount()} 限制。</li>
 * </ol>
 *
 * <p><b>近似性说明</b>：Beam + 分支上限 + 高频词截断均为近似手段，不保证完备；与 {@code maxCandidateCount} 配合可在劣化数据上保持可预测时延。
 *
 * @author muqingyu
 */
public final class BeamFrequentItemsetMiner {

    /** 当调用方传入的 {@code beamWidth <= 0} 时使用的内置束宽。 */
    private static final int DEFAULT_BEAM_WIDTH = 64;

    /**
     * 执行挖掘并返回候选项与统计信息。
     *
     * @param tidsetsByTermId 下标为 termId，元素为该词出现的文档位图（与门面索引一致）
     * @param config 支持度、项集长度区间、候选上限
     * @param maxFrequentTermCount 参与扩展的频繁 1-项最大个数；{@code <= 0} 表示不截断
     * @param maxBranchingFactor 每个前缀每层最多保留的扩展子分支数；{@code <= 0} 表示不限制
     * @param beamWidth 每层保留的前缀状态数；{@code <= 0} 时使用 {@link #DEFAULT_BEAM_WIDTH}
     * @return 候选项列表及 {@link FrequentItemsetMiningResult} 中各计数字段
     */
    public FrequentItemsetMiningResult mineWithStats(
            List<BitSet> tidsetsByTermId,
            SelectorConfig config,
            int maxFrequentTermCount,
            int maxBranchingFactor,
            int beamWidth
    ) {
        // 单项支持度缓存：排序比较器内避免反复 cardinality。
        int[] supportsByTermId = new int[tidsetsByTermId.size()];
        List<Integer> frequentTermIds = collectFrequentTermIds(
                tidsetsByTermId,
                config.getMinSupport(),
                supportsByTermId,
                maxFrequentTermCount
        );
        if (frequentTermIds.isEmpty()) {
            return result(Collections.<CandidateItemset>emptyList(), 0, 0, 0, false);
        }

        int safeBeamWidth = beamWidth > 0 ? beamWidth : DEFAULT_BEAM_WIDTH;
        List<CandidateItemset> out = new ArrayList<>(
                Math.min(config.getMaxCandidateCount(), 10000)
        );
        int generatedCandidateCount = 0;
        int intersectionCount = 0;
        boolean truncatedByCandidateLimit = false;

        // --- 第 1 层：1-项前缀；若允许长度 1 则直接输出单项候选 ---
        List<PrefixState> currentLevel = new ArrayList<>();
        for (int i = 0; i < frequentTermIds.size(); i++) {
            PrefixState state = singleTermPrefix(i, frequentTermIds, tidsetsByTermId);
            currentLevel.add(state);
            if (config.getMinItemsetSize() <= 1) {
                out.add(new CandidateItemset(state.termIds, state.tidset));
                generatedCandidateCount++;
                if (out.size() >= config.getMaxCandidateCount()) {
                    truncatedByCandidateLimit = true;
                    return result(
                            out,
                            frequentTermIds.size(),
                            generatedCandidateCount,
                            intersectionCount,
                            truncatedByCandidateLimit
                    );
                }
            }
        }
        // 首层 beam：在扩展 deeper 项集前先砍掉尾部前缀，控制组合爆炸。
        currentLevel = topK(currentLevel, safeBeamWidth);

        // --- 深度 2..maxItemsetSize：逐层扩展；depth 与「当前项集长度 - 1」对齐 ---
        for (int depth = 1; depth < config.getMaxItemsetSize(); depth++) {
            if (currentLevel.isEmpty()) {
                break;
            }
            List<PrefixState> nextLevel = new ArrayList<>();
            for (PrefixState prefix : currentLevel) {
                int expandedChildren = 0;
                // 仅向 lastPosition 之后扩展，保证 termId 组合唯一且与排序列表一致。
                for (int nextPos = prefix.lastPosition + 1; nextPos < frequentTermIds.size(); nextPos++) {
                    if (maxBranchingFactor > 0 && expandedChildren >= maxBranchingFactor) {
                        break;
                    }
                    int termId = frequentTermIds.get(nextPos).intValue();
                    BitSet nextTidset = (BitSet) prefix.tidset.clone();
                    nextTidset.and(tidsetsByTermId.get(termId));
                    intersectionCount++;

                    int support = nextTidset.cardinality();
                    if (support < config.getMinSupport()) {
                        continue;
                    }
                    expandedChildren++;
                    int[] nextTermIds = append(prefix.termIds, termId);
                    PrefixState child = new PrefixState(
                            nextTermIds,
                            nextTidset,
                            nextPos,
                            score(nextTermIds.length, support)
                    );
                    nextLevel.add(child);

                    if (nextTermIds.length >= config.getMinItemsetSize()) {
                        out.add(new CandidateItemset(nextTermIds, nextTidset));
                        generatedCandidateCount++;
                        if (out.size() >= config.getMaxCandidateCount()) {
                            truncatedByCandidateLimit = true;
                            return result(
                                    out,
                                    frequentTermIds.size(),
                                    generatedCandidateCount,
                                    intersectionCount,
                                    truncatedByCandidateLimit
                            );
                        }
                    }
                }
            }
            currentLevel = topK(nextLevel, safeBeamWidth);
        }

        return result(
                out,
                frequentTermIds.size(),
                generatedCandidateCount,
                intersectionCount,
                truncatedByCandidateLimit
        );
    }

    /** 统一构造 {@link FrequentItemsetMiningResult}，避免多处重复字段顺序。 */
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
     * 由频繁列表第 {@code frequentTermIndex} 个词构造 1-项前缀状态。
     * <p>tidset 使用 clone，避免后续与运算污染索引中的原始位图。
     */
    private PrefixState singleTermPrefix(
            int frequentTermIndex,
            List<Integer> frequentTermIds,
            List<BitSet> tidsetsByTermId
    ) {
        int termId = frequentTermIds.get(frequentTermIndex).intValue();
        BitSet tidset = (BitSet) tidsetsByTermId.get(termId).clone();
        return new PrefixState(
                new int[] {termId},
                tidset,
                frequentTermIndex,
                score(1, tidset.cardinality())
        );
    }

    /**
     * 收集频繁 1-项 id，按支持度降序排序；可选截断为前 {@code maxFrequentTermCount} 个。
     *
     * @param supportsByTermId 输出：每个 termId 的支持度，供排序比较器使用
     */
    private List<Integer> collectFrequentTermIds(
            List<BitSet> tidsetsByTermId,
            int minSupport,
            int[] supportsByTermId,
            int maxFrequentTermCount
    ) {
        List<Integer> frequentTermIds = new ArrayList<>();
        for (int termId = 0; termId < tidsetsByTermId.size(); termId++) {
            int support = tidsetsByTermId.get(termId).cardinality();
            supportsByTermId[termId] = support;
            if (support >= minSupport) {
                frequentTermIds.add(Integer.valueOf(termId));
            }
        }
        Collections.sort(frequentTermIds, new Comparator<Integer>() {
            @Override
            public int compare(Integer a, Integer b) {
                int supportCompare = Integer.compare(
                        supportsByTermId[b.intValue()],
                        supportsByTermId[a.intValue()]
                );
                if (supportCompare != 0) {
                    return supportCompare;
                }
                return Integer.compare(a.intValue(), b.intValue());
            }
        });
        if (maxFrequentTermCount > 0 && frequentTermIds.size() > maxFrequentTermCount) {
            return new ArrayList<>(frequentTermIds.subList(0, maxFrequentTermCount));
        }
        return frequentTermIds;
    }

    /**
     * 按 score 降序、{@code lastPosition} 升序打破平局，取前 {@code k} 个前缀状态。
     */
    private List<PrefixState> topK(List<PrefixState> items, int k) {
        if (items.size() <= k) {
            return items;
        }
        Collections.sort(items, new Comparator<PrefixState>() {
            @Override
            public int compare(PrefixState a, PrefixState b) {
                int v = Integer.compare(b.score, a.score);
                if (v != 0) {
                    return v;
                }
                return Integer.compare(a.lastPosition, b.lastPosition);
            }
        });
        return new ArrayList<>(items.subList(0, k));
    }

    /** 在 {@code src} 末尾追加 {@code termId}，返回新数组（不修改 {@code src}）。 */
    private int[] append(int[] src, int termId) {
        int[] out = new int[src.length + 1];
        System.arraycopy(src, 0, out, 0, src.length);
        out[src.length] = termId;
        return out;
    }

    /**
     * 前缀排序得分：鼓励「更长且支持度更高」的前缀在 beam 中存活。
     * <p>公式：{@code max(0, (len - 1) * support)}，与 {@link cn.lxdb.plugins.muqingyu.fptoken.model.CandidateItemset} 中启发式同一量级思想。
     */
    private int score(int len, int support) {
        return Math.max(0, (len - 1) * support);
    }

    /**
     * Beam 搜索中的前缀状态（一层内的节点）。
     * <p>仅由外层 {@link BeamFrequentItemsetMiner} 使用；构造后不在原对象上原地修改 tidset（扩展时 clone 或新建状态）。
     */
    private static final class PrefixState {
        /** 当前前缀对应的项集（升序 termId）。 */
        private final int[] termIds;
        /** 该项集对应的文档交集位图。 */
        private final BitSet tidset;
        /** 最后一词在「频繁 term 有序列表」中的下标；扩展时只向后扫。 */
        private final int lastPosition;
        /** {@link BeamFrequentItemsetMiner#score(int, int)} 结果，用于 topK。 */
        private final int score;

        private PrefixState(int[] termIds, BitSet tidset, int lastPosition, int score) {
            this.termIds = termIds;
            this.tidset = tidset;
            this.lastPosition = lastPosition;
            this.score = score;
        }
    }
}
