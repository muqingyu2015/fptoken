package cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.picker;

import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.CandidateItemset;
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
 * <p><b>健壮性</b>：{@code dictionarySize} 为位图容量的下界（须为正）；实际分配 {@code max(dictionarySize, maxTermId+1)}，避免传入略小的词典规模时频繁扩容。排序末键对空 {@code termIds} 使用 {@link Integer#MAX_VALUE} 作为占位，使空项集在该键上稳定靠后。负 {@code termId} 非法。
 *
 * <p>热路径上对 {@code termId} 另附有 {@code assert}（相对 {@code effectiveSize}），仅在启用断言时生效，默认 JVM 无开销。
 *
 * @author muqingyu
 */
public final class GreedyExclusiveItemsetPicker {

    /**
     * 从候选项中选出互斥子集（尽量不共享 termId）。
     *
     * @param candidates 候选项列表；本方法会拷贝后再排序，不修改调用方传入的列表顺序
     * @param dictionarySize 词典规模（termId 上界 + 1 的保守上界），用于分配 {@link BitSet} 的下界
     * @return 按排序顺序接受的互斥项集列表
     */
    public List<CandidateItemset> pick(List<CandidateItemset> candidates, int dictionarySize) {
        return pick(
                candidates,
                dictionarySize,
                cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.config.EngineTuningConfig.PICKER_ESTIMATED_BYTES_PER_TERM,
                cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.config.EngineTuningConfig.PICKER_DEFAULT_COVERAGE_REWARD_PER_TERM,
                TwoPhaseExclusiveItemsetPicker.getRuntimeScoringWeights()
        );
    }

    public List<CandidateItemset> pick(
            List<CandidateItemset> candidates,
            int dictionarySize,
            int estimatedBytesPerTerm,
            int coverageRewardPerTerm,
            TwoPhaseExclusiveItemsetPicker.ScoringWeights scoringWeights
    ) {
        if (candidates == null) {
            throw new IllegalArgumentException("candidates must not be null");
        }
        if (candidates.isEmpty()) {
            return Collections.emptyList();
        }
        if (dictionarySize <= 0) {
            throw new IllegalArgumentException("dictionarySize must be > 0");
        }
        if (estimatedBytesPerTerm <= 0) {
            throw new IllegalArgumentException("estimatedBytesPerTerm must be > 0");
        }
        if (coverageRewardPerTerm < 0) {
            throw new IllegalArgumentException("coverageRewardPerTerm must be >= 0");
        }
        if (scoringWeights == null) {
            throw new IllegalArgumentException("scoringWeights must not be null");
        }
        validateCandidates(candidates);

        List<CandidateItemset> sorted = new ArrayList<>(candidates);
        sorted.sort(buildCandidateOrder(estimatedBytesPerTerm, coverageRewardPerTerm, scoringWeights));

        int maxTermId = computeMaxTermId(sorted);
        int effectiveSize = Math.max(dictionarySize, maxTermId + 1);
        BitSet usedTermIds = new BitSet(effectiveSize);
        int estimated = Math.min(sorted.size(), effectiveSize);
        List<CandidateItemset> out = new ArrayList<>(estimated);

        for (CandidateItemset candidate : sorted) {
            int[] termIds = candidate.getTermIdsUnsafe();
            int len = termIds.length;

            boolean conflict = false;
            for (int i = 0; i < len; i++) {
                int termId = termIds[i];
                assert termId >= 0 && termId < effectiveSize : termId;
                if (usedTermIds.get(termId)) {
                    conflict = true;
                    break;
                }
            }

            if (!conflict) {
                for (int i = 0; i < len; i++) {
                    int termId = termIds[i];
                    assert termId >= 0 && termId < effectiveSize : termId;
                    usedTermIds.set(termId);
                }
                out.add(candidate);
            }
        }
        return out;
    }

    private static Comparator<CandidateItemset> buildCandidateOrder(
            int estimatedBytesPerTerm,
            int coverageRewardPerTerm,
            TwoPhaseExclusiveItemsetPicker.ScoringWeights scoringWeights
    ) {
        return (a, b) -> {
            long sa = objective(a, estimatedBytesPerTerm, coverageRewardPerTerm, scoringWeights);
            long sb = objective(b, estimatedBytesPerTerm, coverageRewardPerTerm, scoringWeights);
            int scoreCompare = Long.compare(sb, sa);
            if (scoreCompare != 0) {
                return scoreCompare;
            }
            int savingCompare = Integer.compare(b.getEstimatedSaving(), a.getEstimatedSaving());
            if (savingCompare != 0) {
                return savingCompare;
            }
            int supportCompare = Integer.compare(b.getSupport(), a.getSupport());
            if (supportCompare != 0) {
                return supportCompare;
            }
            int[] ta = a.getTermIdsUnsafe();
            int[] tb = b.getTermIdsUnsafe();
            int firstA = ta.length > 0 ? ta[0] : Integer.MAX_VALUE;
            int firstB = tb.length > 0 ? tb[0] : Integer.MAX_VALUE;
            return Integer.compare(firstA, firstB);
        };
    }

    private static long objective(
            CandidateItemset c,
            int estimatedBytesPerTerm,
            int coverageRewardPerTerm,
            TwoPhaseExclusiveItemsetPicker.ScoringWeights scoringWeights
    ) {
        long len = c.length();
        long support = c.getSupport();
        long businessValue = c.getEstimatedSaving();
        long cost = len * (long) estimatedBytesPerTerm;
        long priorityBoost = c.getPriorityBoost();
        long coverage = ((long) coverageRewardPerTerm) * len;
        return businessValue * scoringWeights.getBusinessValueWeight()
                - cost * scoringWeights.getCostWeight()
                + support * scoringWeights.getSupportWeight()
                + len * scoringWeights.getLengthWeight()
                + priorityBoost * scoringWeights.getPriorityBoostWeight()
                + coverage * scoringWeights.getCoverageWeight();
    }

    /** 扫描全部候选项中的词 id，求最大值并校验非负。 */
    private static int computeMaxTermId(List<CandidateItemset> candidates) {
        int max = -1;
        for (int ci = 0, cn = candidates.size(); ci < cn; ci++) {
            int[] termIds = candidates.get(ci).getTermIdsUnsafe();
            int len = termIds.length;
            for (int i = 0; i < len; i++) {
                int termId = termIds[i];
                if (termId < 0) {
                    throw new IllegalArgumentException("termId must be >= 0, got " + termId);
                }
                if (termId > max) {
                    max = termId;
                }
            }
        }
        return max;
    }

    private static void validateCandidates(List<CandidateItemset> candidates) {
        for (int i = 0; i < candidates.size(); i++) {
            CandidateItemset candidate = candidates.get(i);
            if (candidate == null) {
                throw new IllegalArgumentException("candidates[" + i + "] must not be null");
            }
        }
    }
}
