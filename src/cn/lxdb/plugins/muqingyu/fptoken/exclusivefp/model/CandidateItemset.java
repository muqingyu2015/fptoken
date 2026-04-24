package cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model;

import java.util.BitSet;
import java.util.Objects;

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
    private final int priorityBoost;

    /**
     * @param termIds 非 null；长度可为 0（用于鲁棒性测试场景）
     * @param docBits 非 null；与 {@code termIds} 的语义交集一致
     */
    public CandidateItemset(int[] termIds, BitSet docBits) {
        this(termIds, docBits, docBits.cardinality());
    }

    /**
     * @param termIds 非 null；长度可为 0
     * @param docBits 非 null；与 {@code termIds} 的语义交集一致
     * @param support 预先算好的支持度，避免重复 cardinality()
     */
    public CandidateItemset(int[] termIds, BitSet docBits, int support) {
        this(termIds, docBits, support, 0, false);
    }

    private CandidateItemset(
            int[] termIds,
            BitSet docBits,
            int support,
            int priorityBoost,
            boolean trustedReferences
    ) {
        Objects.requireNonNull(termIds, "termIds");
        Objects.requireNonNull(docBits, "docBits");
        if (trustedReferences) {
            this.termIds = termIds;
            this.docBits = docBits;
        } else {
            this.termIds = java.util.Arrays.copyOf(termIds, termIds.length);
            this.docBits = (BitSet) docBits.clone();
        }
        this.support = support;
        this.estimatedSaving = computeEstimatedSaving(this.termIds.length, support);
        this.priorityBoost = Math.max(0, priorityBoost);
    }

    /**
     * 高性能内部路径：调用方保证 {@code termIds/docBits} 后续只读，不再修改引用内容。
     */
    public static CandidateItemset trusted(int[] termIds, BitSet docBits, int support) {
        return new CandidateItemset(termIds, docBits, support, 0, true);
    }

    /** 高性能内部路径（含优先级加权）。 */
    public static CandidateItemset trusted(
            int[] termIds,
            BitSet docBits,
            int support,
            int priorityBoost
    ) {
        return new CandidateItemset(termIds, docBits, support, priorityBoost, true);
    }

    public int[] getTermIds() {
        return java.util.Arrays.copyOf(termIds, termIds.length);
    }

    /** 仅供性能敏感内部调用；调用方不得修改返回数组。 */
    public int[] getTermIdsUnsafe() {
        return termIds;
    }

    public BitSet getDocBits() {
        return (BitSet) docBits.clone();
    }

    /** 仅供性能敏感内部调用；调用方不得修改返回位图。 */
    public BitSet getDocBitsUnsafe() {
        return docBits;
    }

    public int getSupport() {
        return support;
    }

    public int getEstimatedSaving() {
        return estimatedSaving;
    }

    /** 候选优先级附加分（用于 merge hint 场景）。 */
    public int getPriorityBoost() {
        return priorityBoost;
    }

    /** 返回带优先级附加分的新实例。 */
    public CandidateItemset withPriorityBoost(int boost) {
        if (boost <= 0) {
            return this;
        }
        return new CandidateItemset(termIds, docBits, support, boost, true);
    }

    /** 项集中词的个数。 */
    public int length() {
        return termIds.length;
    }

    private static int computeEstimatedSaving(int termCount, int support) {
        long raw = ((long) termCount - 1L) * (long) support;
        if (raw <= 0L) {
            return 0;
        }
        if (raw > Integer.MAX_VALUE) {
            return Integer.MAX_VALUE;
        }
        return (int) raw;
    }
}
