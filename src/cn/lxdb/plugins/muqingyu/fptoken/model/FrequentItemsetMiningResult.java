package cn.lxdb.plugins.muqingyu.fptoken.model;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * {@link cn.lxdb.plugins.muqingyu.fptoken.miner.BeamFrequentItemsetMiner} 的输出：候选项列表与过程统计。
 *
 * <p>其中 {@link #getGeneratedCandidateCount()} 与 {@link #getCandidates()} 的 size 在当前实现中一致，
 * 前者便于与历史日志字段对齐；{@link #getFrequentTermCount()} 为参与扩展的频繁 1-项个数。
 *
 * @author muqingyu
 */
public final class FrequentItemsetMiningResult {
    private final List<CandidateItemset> candidates;
    private final int frequentTermCount;
    private final int generatedCandidateCount;
    private final int intersectionCount;
    private final boolean truncatedByCandidateLimit;

    /**
     * @param candidates 挖掘得到的列表（可能因上限截断而未满搜索空间）
     * @param frequentTermCount 频繁 1-项数量
     * @param generatedCandidateCount 写入 {@link CandidateItemset} 的条数
     * @param intersectionCount tidset 交集次数
     * @param truncatedByCandidateLimit 是否因候选上限停止
     */
    public FrequentItemsetMiningResult(
            List<CandidateItemset> candidates,
            int frequentTermCount,
            int generatedCandidateCount,
            int intersectionCount,
            boolean truncatedByCandidateLimit
    ) {
        this.candidates = Collections.unmodifiableList(new ArrayList<>(Objects.requireNonNull(candidates, "candidates")));
        this.frequentTermCount = frequentTermCount;
        this.generatedCandidateCount = generatedCandidateCount;
        this.intersectionCount = intersectionCount;
        this.truncatedByCandidateLimit = truncatedByCandidateLimit;
    }

    public List<CandidateItemset> getCandidates() {
        return candidates;
    }

    public int getFrequentTermCount() {
        return frequentTermCount;
    }

    public int getGeneratedCandidateCount() {
        return generatedCandidateCount;
    }

    public int getIntersectionCount() {
        return intersectionCount;
    }

    public boolean isTruncatedByCandidateLimit() {
        return truncatedByCandidateLimit;
    }
}
