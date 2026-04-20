package cn.lxdb.plugins.muqingyu.fptoken.picker;

import cn.lxdb.plugins.muqingyu.fptoken.model.CandidateItemset;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

/**
 * 两阶段互斥选择：第一阶段用 {@link GreedyExclusiveItemsetPicker} 快速得可行解；第二阶段在预算内做 1-opt 替换以改进目标。
 *
 * <p><b>第二阶段规则</b>：
 * <ul>
 *   <li>按候选列表顺序遍历「挑战者」；对每个挑战者，尝试替换当前解中某一「在位者」。</li>
 *   <li>仅当 {@link #objective(CandidateItemset)} 严格优于在位者时才尝试；每次尝试计入 {@code maxSwapTrials}，无论是否替换成功。</li>
 *   <li>替换需保持互斥：除被替换位置外，其余已选项占用的 termId 与挑战者无交集。</li>
 *   <li>每个挑战者至多成功替换一次（内层成功后 {@code break}），不做多步连锁优化。</li>
 * </ul>
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
        GreedyExclusiveItemsetPicker greedy = new GreedyExclusiveItemsetPicker();
        List<CandidateItemset> selected = new ArrayList<>(greedy.pick(candidates, dictionarySize));
        if (selected.isEmpty() || maxSwapTrials <= 0) {
            return selected;
        }

        int usedSwapTrials = 0;
        for (int i = 0; i < candidates.size() && usedSwapTrials < maxSwapTrials; i++) {
            CandidateItemset challenger = candidates.get(i);
            long challengerScore = objective(challenger);
            for (int j = 0; j < selected.size() && usedSwapTrials < maxSwapTrials; j++) {
                CandidateItemset incumbent = selected.get(j);
                long incumbentScore = objective(incumbent);
                if (challengerScore <= incumbentScore) {
                    continue;
                }
                usedSwapTrials++;
                if (canReplace(selected, j, challenger, dictionarySize)) {
                    selected.set(j, challenger);
                    break;
                }
            }
        }
        return selected;
    }

    /**
     * 判断将 {@code selected[replaceIndex]} 换成 {@code challenger} 后是否仍词级互斥。
     *
     * @param selected 当前解
     * @param replaceIndex 被替换下标
     * @param challenger 候选替换项
     * @param dictionarySize 位图容量
     */
    private boolean canReplace(
            List<CandidateItemset> selected,
            int replaceIndex,
            CandidateItemset challenger,
            int dictionarySize
    ) {
        BitSet used = new BitSet(dictionarySize);
        for (int i = 0; i < selected.size(); i++) {
            if (i == replaceIndex) {
                continue;
            }
            CandidateItemset c = selected.get(i);
            for (int termId : c.getTermIds()) {
                used.set(termId);
            }
        }
        for (int termId : challenger.getTermIds()) {
            if (used.get(termId)) {
                return false;
            }
        }
        return true;
    }

    /**
     * 替换阶段使用的标量目标（越大越好）。主项为 {@link CandidateItemset#getEstimatedSaving()}，
     * 其次 {@link CandidateItemset#getSupport()}，再次项集长度；系数拉开量级避免比较歧义。
     */
    private long objective(CandidateItemset c) {
        return ((long) c.getEstimatedSaving()) * 1000000L
                + ((long) c.getSupport()) * 1000L
                + (long) c.length();
    }
}
