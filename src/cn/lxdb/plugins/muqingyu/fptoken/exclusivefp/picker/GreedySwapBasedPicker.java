package cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.picker;

import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.CandidateItemset;
import java.util.List;

/**
 * 更直观命名的两阶段选择器：贪心初选 + 局部交换优化。
 *
 * <p>该类作为兼容包装，内部复用 {@link TwoPhaseExclusiveItemsetPicker} 与
 * {@link GreedyExclusiveItemsetPicker}，不改变既有算法语义。
 */
public final class GreedySwapBasedPicker {
    private final TwoPhaseExclusiveItemsetPicker twoPhasePicker = new TwoPhaseExclusiveItemsetPicker();
    private final GreedyExclusiveItemsetPicker greedyPicker = new GreedyExclusiveItemsetPicker();

    /**
     * 选择互斥项集（包含贪心 + 交换优化两个阶段）。
     */
    public List<CandidateItemset> selectOptimalMutuallyExclusiveSets(
            List<CandidateItemset> candidateItemsetsToEvaluate,
            int vocabularySize,
            int maxSwapTrials
    ) {
        return twoPhasePicker.pick(candidateItemsetsToEvaluate, vocabularySize, maxSwapTrials);
    }

    /**
     * 只执行第一阶段贪心挑选。
     */
    public List<CandidateItemset> selectUsingGreedyApproach(
            List<CandidateItemset> candidateItemsetsToEvaluate,
            int vocabularySize
    ) {
        return greedyPicker.pick(candidateItemsetsToEvaluate, vocabularySize);
    }

    /**
     * 在给定候选上执行本地交换改进（语义同两阶段选择器）。
     */
    public List<CandidateItemset> improveSolutionByLocalSwaps(
            List<CandidateItemset> candidateItemsetsToEvaluate,
            int vocabularySize,
            int maxSwapTrials
    ) {
        return twoPhasePicker.pick(candidateItemsetsToEvaluate, vocabularySize, maxSwapTrials);
    }
}
