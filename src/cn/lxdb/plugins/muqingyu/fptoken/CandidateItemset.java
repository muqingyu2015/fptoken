package cn.lxdb.plugins.muqingyu.fptoken;

import java.util.BitSet;

/**
 * 内部候选项集模型。
 *
 * 字段说明：
 * - termIds: 项集内词的 ID 列表（升序构造，便于后续比较/调试）。
 * - docBits: 该项集命中的文档集合（可直接转成 doclist）。
 * - support: |docBits|，用于频繁性判断和排序。
 * - estimatedSaving: 粗略估算节省值，用于贪心排序的第二优先级。
 *
 * estimatedSaving 解释：
 * - 若一个项集长度为 k，支持度为 s，可理解为共享存储潜力与 (k-1)*s 相关。
 * - 这里是工程启发式，不是严格压缩率公式。
 */
public final class CandidateItemset {
    private final int[] termIds;
    private final BitSet docBits;
    private final int support;
    private final int estimatedSaving;

    public CandidateItemset(int[] termIds, BitSet docBits) {
        this.termIds = termIds;
        this.docBits = docBits;
        this.support = docBits.cardinality();
        this.estimatedSaving = Math.max(0, (termIds.length - 1) * support);
    }

    public int[] getTermIds() {
        return termIds;
    }

    public BitSet getDocBits() {
        return docBits;
    }

    public int getSupport() {
        return support;
    }

    public int getEstimatedSaving() {
        return estimatedSaving;
    }

    public int length() {
        return termIds.length;
    }
}
