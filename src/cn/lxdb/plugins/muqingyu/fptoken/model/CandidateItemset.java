package cn.lxdb.plugins.muqingyu.fptoken.model;

import java.util.BitSet;

/**
 * 挖掘阶段的一条候选项集（内部表示）。
 *
 * <p><b>字段</b>：
 * <ul>
 *   <li>{@code termIds}：组成该项集的词 id，挖掘过程中按扩展顺序形成升序（便于调试与稳定比较）。</li>
 *   <li>{@code docBits}：该项集命中的文档集合；支持度 {@code support = cardinality(docBits)}。</li>
 *   <li>{@code estimatedSaving}：启发式价值 {@code max(0, (k - 1) * support)}，用于贪心/替换排序，非严格压缩率。</li>
 * </ul>
 *
 * @author muqingyu
 */
public final class CandidateItemset {
    private final int[] termIds;
    private final BitSet docBits;
    private final int support;
    private final int estimatedSaving;

    /**
     * @param termIds 非 null；长度至少为 1
     * @param docBits 非 null；与 {@code termIds} 的语义交集一致
     */
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

    /** 项集中词的个数。 */
    public int length() {
        return termIds.length;
    }
}
