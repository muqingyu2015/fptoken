package cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.picker;

import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.config.EngineTuningConfig;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.CandidateItemset;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * 两阶段互斥选择：第一阶段用 {@link GreedyExclusiveItemsetPicker} 快速得可行解；第二阶段在预算内做 1-opt 替换以改进目标。
 *
 * <p><b>第二阶段规则</b>：
 * <ul>
 *   <li>按候选列表顺序遍历「挑战者」；对每个挑战者，尝试替换当前解中某一「在位者」。</li>
 *   <li>仅当 {@link #objective(CandidateItemset, int, int)} 严格优于在位者时才尝试；每次尝试计入 {@code maxSwapTrials}，无论是否替换成功。</li>
 *   <li>替换需保持互斥：除被替换位置外，其余已选项占用的 termId 与挑战者无交集。</li>
 *   <li>每个挑战者至多成功替换一次（内层成功后 {@code break}），不做多步连锁优化。</li>
 * </ul>
 *
 * <p>第二阶段位图容量与 {@link GreedyExclusiveItemsetPicker} 对齐：{@code max(dictionarySize, maxTermId+1)}，其中 {@code maxTermId} 在全部 {@code candidates} 上统计，以便在 {@code dictionarySize} 偏小时仍与第一阶段一致。
 *
 * <p>替换可行性判断与位图增量更新内联在 {@link #pick} 的内层循环中，避免额外方法调用开销。
 *
 * @author muqingyu
 */
public final class TwoPhaseExclusiveItemsetPicker {

    /**
     * @param candidates 候选项（通常与挖掘输出顺序一致；影响第二阶段扫描次序）
     * @param dictionarySize 词占用位图大小，同 {@link GreedyExclusiveItemsetPicker#pick}
     * @param maxSwapTrials 1-opt 尝试次数上限；{@code <= 0} 时跳过第二阶段
     * @return 互斥项集列表（可能为空）
     */
    public List<CandidateItemset> pick(
            List<CandidateItemset> candidates,
            int dictionarySize,
            int maxSwapTrials
    ) {
        return pick(
                candidates,
                dictionarySize,
                maxSwapTrials,
                EngineTuningConfig.PICKER_DEFAULT_MIN_NET_GAIN,
                EngineTuningConfig.PICKER_ESTIMATED_BYTES_PER_TERM,
                EngineTuningConfig.PICKER_DEFAULT_COVERAGE_REWARD_PER_TERM
        );
    }

    public List<CandidateItemset> pick(
            List<CandidateItemset> candidates,
            int dictionarySize,
            int maxSwapTrials,
            int minNetGain
    ) {
        return pick(
                candidates,
                dictionarySize,
                maxSwapTrials,
                minNetGain,
                EngineTuningConfig.PICKER_ESTIMATED_BYTES_PER_TERM,
                EngineTuningConfig.PICKER_DEFAULT_COVERAGE_REWARD_PER_TERM
        );
    }

    public List<CandidateItemset> pick(
            List<CandidateItemset> candidates,
            int dictionarySize,
            int maxSwapTrials,
            int minNetGain,
            int estimatedBytesPerTerm
    ) {
        return pick(
                candidates,
                dictionarySize,
                maxSwapTrials,
                minNetGain,
                estimatedBytesPerTerm,
                EngineTuningConfig.PICKER_DEFAULT_COVERAGE_REWARD_PER_TERM
        );
    }

    public List<CandidateItemset> pick(
            List<CandidateItemset> candidates,
            int dictionarySize,
            int maxSwapTrials,
            int minNetGain,
            int estimatedBytesPerTerm,
            int coverageRewardPerTerm
    ) {
        if (candidates == null) {
            throw new IllegalArgumentException("candidates must not be null");
        }
        if (dictionarySize <= 0) {
            throw new IllegalArgumentException("dictionarySize must be > 0");
        }
        if (maxSwapTrials < 0) {
            throw new IllegalArgumentException("maxSwapTrials must be >= 0");
        }
        if (minNetGain < 0) {
            throw new IllegalArgumentException("minNetGain must be >= 0");
        }
        if (estimatedBytesPerTerm <= 0) {
            throw new IllegalArgumentException("estimatedBytesPerTerm must be > 0");
        }
        if (coverageRewardPerTerm < 0) {
            throw new IllegalArgumentException("coverageRewardPerTerm must be >= 0");
        }
        validateCandidates(candidates);

        int bitsetSize = effectiveBitsetSize(candidates, dictionarySize);
        List<CandidateItemset> filteredCandidates =
                filterByMinNetGain(candidates, minNetGain, estimatedBytesPerTerm);
        if (filteredCandidates.isEmpty()) {
            return new ArrayList<>();
        }

        GreedyExclusiveItemsetPicker greedy = new GreedyExclusiveItemsetPicker();
        List<CandidateItemset> selected = new ArrayList<>(greedy.pick(filteredCandidates, dictionarySize));
        if (selected.isEmpty() || maxSwapTrials <= 0) {
            return selected;
        }

        // 预计算分数，避免在双层循环中重复 objective()。
        long[] selectedScores = new long[selected.size()];
        for (int i = 0; i < selected.size(); i++) {
            selectedScores[i] = objective(selected.get(i), estimatedBytesPerTerm, coverageRewardPerTerm);
        }
        long[] challengerScores = new long[filteredCandidates.size()];
        for (int i = 0; i < filteredCandidates.size(); i++) {
            challengerScores[i] = objective(filteredCandidates.get(i), estimatedBytesPerTerm, coverageRewardPerTerm);
        }
        Set<CandidateItemset> selectedSet = new HashSet<>(selected);

        BitSet currentSelectionBits = buildSelectionBitSet(selected, bitsetSize);
        BitSet scratch = new BitSet(bitsetSize);
        int usedSwapTrials = 0;
        for (int i = 0; i < filteredCandidates.size() && usedSwapTrials < maxSwapTrials; i++) {
            CandidateItemset challenger = filteredCandidates.get(i);
            if (selectedSet.contains(challenger)) {
                continue;
            }
            long challengerScore = challengerScores[i];
            int[] challengerTermIds = challenger.getTermIdsUnsafe();
            int challengerLen = challengerTermIds.length;
            for (int j = 0; j < selected.size() && usedSwapTrials < maxSwapTrials; j++) {
                if (challengerScore <= selectedScores[j]) {
                    continue;
                }
                usedSwapTrials++;
                CandidateItemset incumbent = selected.get(j);
                int[] incumbentTermIds = incumbent.getTermIdsUnsafe();
                int incumbentLen = incumbentTermIds.length;

                scratch.clear();
                scratch.or(currentSelectionBits);
                for (int k = 0; k < incumbentLen; k++) {
                    int termId = incumbentTermIds[k];
                    ensureTermIdInRange(termId, bitsetSize);
                    scratch.clear(termId);
                }
                boolean conflict = false;
                for (int k = 0; k < challengerLen; k++) {
                    int termId = challengerTermIds[k];
                    ensureTermIdInRange(termId, bitsetSize);
                    if (scratch.get(termId)) {
                        conflict = true;
                        break;
                    }
                }
                if (!conflict) {
                    for (int k = 0; k < incumbentLen; k++) {
                        int termId = incumbentTermIds[k];
                        ensureTermIdInRange(termId, bitsetSize);
                        currentSelectionBits.clear(termId);
                    }
                    for (int k = 0; k < challengerLen; k++) {
                        int termId = challengerTermIds[k];
                        ensureTermIdInRange(termId, bitsetSize);
                        currentSelectionBits.set(termId);
                    }
                    selected.set(j, challenger);
                    selectedScores[j] = challengerScore;
                    selectedSet.remove(incumbent);
                    selectedSet.add(challenger);
                    break;
                }
            }
        }
        return selected;
    }

    /**
     * 与 {@link GreedyExclusiveItemsetPicker} 相同：在 {@code candidates} 上扫描最大词 id，并与 {@code dictionarySize} 取较大者作为位图容量。
     */
    private static int effectiveBitsetSize(List<CandidateItemset> candidates, int dictionarySize) {
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
        return Math.max(dictionarySize, max + 1);
    }

    private static void validateCandidates(List<CandidateItemset> candidates) {
        for (int i = 0; i < candidates.size(); i++) {
            CandidateItemset candidate = candidates.get(i);
            if (candidate == null) {
                throw new IllegalArgumentException("candidates[" + i + "] must not be null");
            }
        }
    }

    private static List<CandidateItemset> filterByMinNetGain(
            List<CandidateItemset> candidates,
            int minNetGain,
            int estimatedBytesPerTerm
    ) {
        if (minNetGain <= 0) {
            return candidates;
        }
        List<CandidateItemset> out = new ArrayList<>(candidates.size());
        for (int i = 0; i < candidates.size(); i++) {
            CandidateItemset candidate = candidates.get(i);
            if (netGain(candidate, estimatedBytesPerTerm) >= minNetGain) {
                out.add(candidate);
            }
        }
        return out;
    }

    private static void ensureTermIdInRange(int termId, int bitsetSize) {
        if (termId < 0 || termId >= bitsetSize) {
            throw new IllegalArgumentException(
                    "termId out of bounds: " + termId + ", expected [0, " + bitsetSize + ")");
        }
    }

    /** 由当前解构建“已占用 termId”位图。 */
    private BitSet buildSelectionBitSet(List<CandidateItemset> selected, int bitsetSize) {
        BitSet used = new BitSet(bitsetSize);
        for (int i = 0; i < selected.size(); i++) {
            CandidateItemset c = selected.get(i);
            int[] termIds = c.getTermIdsUnsafe();
            int len = termIds.length;
            for (int t = 0; t < len; t++) {
                int termId = termIds[t];
                ensureTermIdInRange(termId, bitsetSize);
                used.set(termId);
            }
        }
        return used;
    }

    /**
     * 替换阶段使用的标量目标（越大越好）。主项为 {@link CandidateItemset#getEstimatedSaving()}，
     * 其次 {@link CandidateItemset#getSupport()}，再次项集长度；系数拉开量级避免比较歧义。
     */
    private long objective(CandidateItemset c, int estimatedBytesPerTerm, int coverageRewardPerTerm) {
        long coverageBonus = ((long) coverageRewardPerTerm) * ((long) c.length()) * 1000L;
        long priorityBonus = ((long) c.getPriorityBoost()) * 1000L;
        return ((long) netGain(c, estimatedBytesPerTerm)) * 1000000L
                + ((long) c.getSupport()) * 1000L
                + coverageBonus
                + priorityBonus
                + (long) c.length();
    }

    private static int netGain(CandidateItemset c, int estimatedBytesPerTerm) {
        int dictionaryCost = c.length() * estimatedBytesPerTerm;
        return c.getEstimatedSaving() - dictionaryCost;
    }
}
