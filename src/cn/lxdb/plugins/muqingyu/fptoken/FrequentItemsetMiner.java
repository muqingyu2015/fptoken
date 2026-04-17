package cn.lxdb.plugins.muqingyu.fptoken;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * 频繁项集挖掘器（基于 tidset 交集）。
 *
 * 原理说明：
 * - 采用垂直数据表示：每个 termId 对应一个 tidset(BitSet)。
 * - 若要计算项集 X∪Y 的支持度，只需做 tidset 交集：
 *   support(X∪Y) = |T(X) ∩ T(Y)|
 * - 通过 DFS 逐层扩展项集，并用 minSupport 做早停剪枝。
 *
 * 复杂度说明（近似）：
 * - 最坏候选规模仍可能指数增长（频繁项集问题本质如此）。
 * - 但在工程上通过 maxCandidateCount + maxItemsetSize 双上限控成本。
 */
public final class FrequentItemsetMiner {

    public List<CandidateItemset> mine(List<BitSet> tidsetsByTermId, SelectorConfig config) {
        // 第一步：先找出频繁 1-项集，非频繁单词直接剪掉。
        List<Integer> frequentTermIds = collectFrequentTermIds(tidsetsByTermId, config.getMinSupport());
        List<CandidateItemset> out = new ArrayList<CandidateItemset>(
                Math.min(config.getMaxCandidateCount(), 10_000)
        );
        // prefix 复用数组，避免递归过程中频繁创建新对象。
        int[] prefix = new int[config.getMaxItemsetSize()];
        dfs(
                frequentTermIds,
                tidsetsByTermId,
                0,
                prefix,
                0,
                null,
                config,
                out
        );
        return out;
    }

    private List<Integer> collectFrequentTermIds(List<BitSet> tidsetsByTermId, int minSupport) {
        List<Integer> frequentTermIds = new ArrayList<Integer>();
        for (int termId = 0; termId < tidsetsByTermId.size(); termId++) {
            BitSet tidset = tidsetsByTermId.get(termId);
            if (tidset.cardinality() >= minSupport) {
                frequentTermIds.add(Integer.valueOf(termId));
            }
        }
        Collections.sort(frequentTermIds, new Comparator<Integer>() {
            @Override
            public int compare(Integer a, Integer b) {
                return Integer.compare(a.intValue(), b.intValue());
            }
        });
        return frequentTermIds;
    }

    private void dfs(
            List<Integer> frequentTermIds,
            List<BitSet> tidsetsByTermId,
            int start,
            int[] prefix,
            int prefixLen,
            BitSet prefixTidset,
            SelectorConfig config,
            List<CandidateItemset> out
    ) {
        // 候选达到硬上限后立即返回，保障最坏时延。
        if (out.size() >= config.getMaxCandidateCount()) {
            return;
        }
        for (int i = start; i < frequentTermIds.size(); i++) {
            int nextTermId = frequentTermIds.get(i).intValue();
            BitSet singleTidset = tidsetsByTermId.get(nextTermId);

            BitSet nextTidset;
            if (prefixTidset == null) {
                nextTidset = (BitSet) singleTidset.clone();
            } else {
                // 核心：通过交集得到扩展后项集的 doclist。
                nextTidset = (BitSet) prefixTidset.clone();
                nextTidset.and(singleTidset);
            }

            int support = nextTidset.cardinality();
            // 支持度不足直接剪枝，不再向下扩展。
            if (support < config.getMinSupport()) {
                continue;
            }

            prefix[prefixLen] = nextTermId;
            int nextLen = prefixLen + 1;
            if (nextLen >= config.getMinItemsetSize()) {
                out.add(new CandidateItemset(copyPrefix(prefix, nextLen), nextTidset));
                if (out.size() >= config.getMaxCandidateCount()) {
                    return;
                }
            }

            // 长度未到上限时继续扩展更长项集。
            if (nextLen < config.getMaxItemsetSize()) {
                dfs(
                        frequentTermIds,
                        tidsetsByTermId,
                        i + 1,
                        prefix,
                        nextLen,
                        nextTidset,
                        config,
                        out
                );
                if (out.size() >= config.getMaxCandidateCount()) {
                    return;
                }
            }
        }
    }

    private int[] copyPrefix(int[] source, int len) {
        int[] out = new int[len];
        System.arraycopy(source, 0, out, 0, len);
        return out;
    }
}
