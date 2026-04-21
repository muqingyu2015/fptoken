package cn.lxdb.plugins.muqingyu.fptoken;

import cn.lxdb.plugins.muqingyu.fptoken.model.DocTerms;
import cn.lxdb.plugins.muqingyu.fptoken.model.ExclusiveSelectionResult;
import cn.lxdb.plugins.muqingyu.fptoken.model.SelectedGroup;
import java.util.List;

/**
 * 更直观命名的门面入口：从文档中选择“互斥频繁模式”。
 *
 * <p>这是对 {@link ExclusiveFrequentItemsetSelector} 的兼容包装，行为保持一致，便于新代码采用更清晰的领域命名。
 *
 * <p>主要能力：
 * <ul>
 *   <li>根据最小支持度与模式长度，输出一组互不共享 term 的模式（{@link SelectedGroup}）。</li>
 *   <li>可选返回过程统计（候选数、交集次数、是否被候选上限截断等）。</li>
 * </ul>
 */
public final class MutuallyExclusivePatternSelector {

    private MutuallyExclusivePatternSelector() {
    }

    /** 仅返回模式列表（使用默认最大模式长度与候选上限）。 */
    public static List<SelectedGroup> select(
            List<DocTerms> docs,
            int minSupport,
            int minItemsetSize
    ) {
        return ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsets(docs, minSupport, minItemsetSize);
    }

    /** 仅返回模式列表（显式指定最大模式长度与候选上限）。 */
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

    /** 返回模式与统计（使用默认最大模式长度与候选上限）。 */
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
     * @param docs 输入文档列表（每个 {@link DocTerms} 代表一个 docId 对应的一组词）
     * @param minSupport 最小支持度（至少命中的文档数）
     * @param minItemsetSize 最小模式长度（最少包含多少词）
     * @param maxItemsetSize 最大模式长度（最多包含多少词）
     * @param maxCandidateCount 挖掘阶段候选上限（用于控制时间/内存）
     * @return 选择结果与关键统计
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
