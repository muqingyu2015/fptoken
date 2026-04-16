package cn.lxdb.plugins.muqingyu.fptoken;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * 高性能贪心选择器：
 * 1) 先排序（长度、价值、支持度）
 * 2) 单次扫描做词级互斥选择
 *
 * 设计理由：
 * - 精确最优（如 ILP）在候选很多时成本高，线上不稳定。
 * - 贪心可在 O(C log C)（排序）+ O(C * L)（扫描）内完成，
 *   其中 C 为候选数，L 为候选平均长度。
 * - 通过排序规则把“最大长度、高价值”偏好编码进选择顺序。
 */
final class GreedyExclusiveItemsetPicker {

    List<CandidateItemset> pick(List<CandidateItemset> candidates, int dictionarySize) {
        // 复制一份再排序，避免影响调用方传入列表顺序。
        List<CandidateItemset> sorted = new ArrayList<CandidateItemset>(candidates);
        sortCandidates(sorted);
        BitSet usedTermIds = new BitSet(dictionarySize);
        List<CandidateItemset> out = new ArrayList<CandidateItemset>();
        for (CandidateItemset candidate : sorted) {
            // 若候选与已选结果在词上有交集，则跳过（互斥约束）。
            if (!hasConflict(usedTermIds, candidate.getTermIds())) {
                markUsed(usedTermIds, candidate.getTermIds());
                out.add(candidate);
            }
        }
        return out;
    }

    private void sortCandidates(List<CandidateItemset> candidates) {
        Collections.sort(candidates, new Comparator<CandidateItemset>() {
            @Override
            public int compare(CandidateItemset a, CandidateItemset b) {
                // 第1优先级：长度越长越靠前（满足“最大长度优先”诉求）。
                int v = Integer.compare(b.length(), a.length());
                if (v != 0) {
                    return v;
                }
                // 第2优先级：估算节省值越大越靠前。
                v = Integer.compare(b.getEstimatedSaving(), a.getEstimatedSaving());
                if (v != 0) {
                    return v;
                }
                // 第3优先级：支持度越高越靠前。
                v = Integer.compare(b.getSupport(), a.getSupport());
                if (v != 0) {
                    return v;
                }
                return Integer.compare(a.getTermIds()[0], b.getTermIds()[0]);
            }
        });
    }

    private boolean hasConflict(BitSet usedTermIds, int[] termIds) {
        for (int termId : termIds) {
            if (usedTermIds.get(termId)) {
                return true;
            }
        }
        return false;
    }

    private void markUsed(BitSet usedTermIds, int[] termIds) {
        for (int termId : termIds) {
            usedTermIds.set(termId);
        }
    }
}
