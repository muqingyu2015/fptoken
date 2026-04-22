package cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * 门面 API 的完整返回：互斥词组列表 + 挖掘/预算相关统计，便于线上日志与参数回放。
 *
 * <p><b>统计字段说明</b>：
 * <ul>
 *   <li>{@link #getFrequentTermCount()}：参与 Beam 扩展的频繁 1-项数量（挖掘器返回）。</li>
 *   <li>{@link #getCandidateCount()}：实际生成的候选项条数（含未进入最终互斥集的项）。</li>
 *   <li>{@link #getIntersectionCount()}：挖掘过程中 tidset 交集（{@code BitSet#and}）执行次数。</li>
 *   <li>{@link #getMaxCandidateCount()}：本次调用传入的候选上限（与 {@link cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.config.SelectorConfig} 中值相同，便于对照）。</li>
 *   <li>{@link #isTruncatedByCandidateLimit()}：是否因达到候选上限而提前结束挖掘。</li>
 * </ul>
 *
 * @author muqingyu
 */
public final class ExclusiveSelectionResult {
    private final List<SelectedGroup> groups;
    private final int frequentTermCount;
    private final int candidateCount;
    private final int intersectionCount;
    private final int maxCandidateCount;
    private final boolean truncatedByCandidateLimit;

    /**
     * @param groups 最终互斥词组，可为空列表
     * @param frequentTermCount 频繁 1-项数
     * @param candidateCount 生成候选数
     * @param intersectionCount 交集次数
     * @param maxCandidateCount 本次调用的候选上限配置
     * @param truncatedByCandidateLimit 是否触发截断
     */
    public ExclusiveSelectionResult(
            List<SelectedGroup> groups,
            int frequentTermCount,
            int candidateCount,
            int intersectionCount,
            int maxCandidateCount,
            boolean truncatedByCandidateLimit
    ) {
        this.groups = Collections.unmodifiableList(new ArrayList<>(Objects.requireNonNull(groups, "groups")));
        this.frequentTermCount = frequentTermCount;
        this.candidateCount = candidateCount;
        this.intersectionCount = intersectionCount;
        this.maxCandidateCount = maxCandidateCount;
        this.truncatedByCandidateLimit = truncatedByCandidateLimit;
    }

    public List<SelectedGroup> getGroups() {
        return groups;
    }

    public int getFrequentTermCount() {
        return frequentTermCount;
    }

    public int getCandidateCount() {
        return candidateCount;
    }

    public int getIntersectionCount() {
        return intersectionCount;
    }

    public int getMaxCandidateCount() {
        return maxCandidateCount;
    }

    public boolean isTruncatedByCandidateLimit() {
        return truncatedByCandidateLimit;
    }
}
