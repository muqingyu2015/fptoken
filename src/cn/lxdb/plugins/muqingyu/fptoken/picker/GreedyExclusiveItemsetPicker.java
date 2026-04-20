package cn.lxdb.plugins.muqingyu.fptoken.picker;

import cn.lxdb.plugins.muqingyu.fptoken.model.CandidateItemset;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * 词级互斥下的贪心选择器：先按业务偏好排序，再线性扫描，已选集合中任意两个项集不得共享同一 termId。
 *
 * <p><b>复杂度</b>：排序 O(C log C)，扫描 O(C · L)，其中 C 为候选数，L 为项集平均长度。
 *
 * <p><b>与精确最优</b>：不保证全局最优；作为低成本基线，并为 {@link TwoPhaseExclusiveItemsetPicker} 提供初解。
 *
 * @author muqingyu
 */
public final class GreedyExclusiveItemsetPicker {

    /**
     * 从候选项中选出互斥子集（尽量不共享 termId）。
     *
     * @param candidates 候选项列表；本方法会拷贝后再排序，不修改调用方传入的列表顺序
     * @param dictionarySize 词典规模（termId 上界 + 1 的保守上界），用于分配 {@link BitSet}
     * @return 按排序顺序接受的互斥项集列表
     */
    public List<CandidateItemset> pick(List<CandidateItemset> candidates, int dictionarySize) {
        List<CandidateItemset> sorted = new ArrayList<>(candidates);
        sortCandidates(sorted);
        BitSet usedTermIds = new BitSet(dictionarySize);
        List<CandidateItemset> out = new ArrayList<>();
        for (CandidateItemset candidate : sorted) {
            if (!hasConflict(usedTermIds, candidate.getTermIds())) {
                markUsed(usedTermIds, candidate.getTermIds());
                out.add(candidate);
            }
        }
        return out;
    }

    /**
     * 排序键（优先级从高到低）：
     * <ol>
     *   <li>项集长度降序（偏好更长词组）</li>
     *   <li>{@link CandidateItemset#getEstimatedSaving()} 降序</li>
     *   <li>{@link CandidateItemset#getSupport()} 降序</li>
     *   <li>首 termId 升序（稳定打破平局）</li>
     * </ol>
     */
    private void sortCandidates(List<CandidateItemset> candidates) {
        Collections.sort(candidates, new Comparator<CandidateItemset>() {
            @Override
            public int compare(CandidateItemset a, CandidateItemset b) {
                int v = Integer.compare(b.length(), a.length());
                if (v != 0) {
                    return v;
                }
                v = Integer.compare(b.getEstimatedSaving(), a.getEstimatedSaving());
                if (v != 0) {
                    return v;
                }
                v = Integer.compare(b.getSupport(), a.getSupport());
                if (v != 0) {
                    return v;
                }
                return Integer.compare(a.getTermIds()[0], b.getTermIds()[0]);
            }
        });
    }

    /** 若 {@code termIds} 中任一词已在 {@code usedTermIds} 中置位，则冲突。 */
    private boolean hasConflict(BitSet usedTermIds, int[] termIds) {
        for (int termId : termIds) {
            if (usedTermIds.get(termId)) {
                return true;
            }
        }
        return false;
    }

    /** 将本候选所含 termId 全部标记为已占用。 */
    private void markUsed(BitSet usedTermIds, int[] termIds) {
        for (int termId : termIds) {
            usedTermIds.set(termId);
        }
    }
}
