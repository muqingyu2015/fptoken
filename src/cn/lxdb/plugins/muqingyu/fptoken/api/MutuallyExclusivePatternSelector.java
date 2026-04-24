package cn.lxdb.plugins.muqingyu.fptoken.api;

import cn.lxdb.plugins.muqingyu.fptoken.ExclusiveFrequentItemsetSelector;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.DocTerms;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.ExclusiveSelectionResult;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.SelectedGroup;
import java.util.List;

/**
 * 更直观命名的门面入口：从文档中选择“互斥频繁模式”。
 *
 * <p>这是对 {@link ExclusiveFrequentItemsetSelector} 的兼容包装，行为保持一致，便于新代码采用更清晰的领域命名。
 */
public final class MutuallyExclusivePatternSelector {

    private MutuallyExclusivePatternSelector() {
    }

    /**
     * 仅返回模式列表（使用默认最大模式长度与候选上限）。
     *
     * <p><b>前置条件</b>：{@code minSupport >= 1}、{@code minItemsetSize >= 1}。
     * 当 {@code docs == null || docs.isEmpty()} 时返回空结果（与底层选择器保持一致）。</p>
     */
    public static List<SelectedGroup> select(
            List<DocTerms> docs,
            int minSupport,
            int minItemsetSize
    ) {
        return ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsets(docs, minSupport, minItemsetSize);
    }

    /**
     * 仅返回模式列表（显式指定最大模式长度与候选上限）。
     *
     * <p><b>前置条件</b>：{@code minSupport >= 1}、{@code minItemsetSize >= 1}、
     * {@code maxItemsetSize >= minItemsetSize}、{@code maxCandidateCount >= 1}。
     * 当 {@code docs == null || docs.isEmpty()} 时返回空结果（与底层选择器保持一致）。</p>
     */
    public static List<SelectedGroup> select(
            List<DocTerms> docs,
            int minSupport,
            int minItemsetSize,
            int maxItemsetSize,
            int maxCandidateCount
    ) {
        return ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsets(
                docs, minSupport, minItemsetSize, maxItemsetSize, maxCandidateCount);
    }

    /**
     * 返回模式与统计（使用默认最大模式长度与候选上限）。
     *
     * <p><b>前置条件</b>：{@code minSupport >= 1}、{@code minItemsetSize >= 1}。
     * 当 {@code docs == null || docs.isEmpty()} 时返回空结果（与底层选择器保持一致）。</p>
     */
    public static ExclusiveSelectionResult selectWithStats(
            List<DocTerms> docs,
            int minSupport,
            int minItemsetSize
    ) {
        return ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(docs, minSupport, minItemsetSize);
    }

    /**
     * 返回模式与统计。
     *
     * <p><b>前置条件</b>：{@code minSupport >= 1}、{@code minItemsetSize >= 1}、
     * {@code maxItemsetSize >= minItemsetSize}、{@code maxCandidateCount >= 1}。
     * 当 {@code docs == null || docs.isEmpty()} 时返回空结果（与底层选择器保持一致）。</p>
     */
    public static ExclusiveSelectionResult selectWithStats(
            List<DocTerms> docs,
            int minSupport,
            int minItemsetSize,
            int maxItemsetSize,
            int maxCandidateCount
    ) {
        return ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(
                docs, minSupport, minItemsetSize, maxItemsetSize, maxCandidateCount);
    }
}
