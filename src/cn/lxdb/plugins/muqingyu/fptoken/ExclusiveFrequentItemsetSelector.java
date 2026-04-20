package cn.lxdb.plugins.muqingyu.fptoken;

import cn.lxdb.plugins.muqingyu.fptoken.config.SelectorConfig;
import cn.lxdb.plugins.muqingyu.fptoken.index.TermTidsetIndex;
import cn.lxdb.plugins.muqingyu.fptoken.miner.BeamFrequentItemsetMiner;
import cn.lxdb.plugins.muqingyu.fptoken.model.CandidateItemset;
import cn.lxdb.plugins.muqingyu.fptoken.model.DocTerms;
import cn.lxdb.plugins.muqingyu.fptoken.model.ExclusiveSelectionResult;
import cn.lxdb.plugins.muqingyu.fptoken.model.FrequentItemsetMiningResult;
import cn.lxdb.plugins.muqingyu.fptoken.model.SelectedGroup;
import cn.lxdb.plugins.muqingyu.fptoken.picker.TwoPhaseExclusiveItemsetPicker;
import cn.lxdb.plugins.muqingyu.fptoken.util.ByteArrayUtils;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;

/**
 * 互斥频繁项集选择的门面（Facade）：对调用方屏蔽索引、挖掘与挑选的实现细节。
 *
 * <p><b>处理流程</b>（固定单一路径，便于线上行为一致、排障简单）：
 * <ol>
 *   <li>将 {@link DocTerms} 列表建成垂直索引 {@link TermTidsetIndex}（词 → 文档位图）。</li>
 *   <li>在 tidset 上做 {@link BeamFrequentItemsetMiner} 近似挖掘，得到 {@link CandidateItemset} 列表及统计。</li>
 *   <li>用 {@link TwoPhaseExclusiveItemsetPicker} 做词级互斥选择（贪心初解 + 预算内 1-opt 替换）。</li>
 *   <li>将 termId 还原为词字节、doc 位图转为 docId 列表，封装为 {@link SelectedGroup}。</li>
 * </ol>
 *
 * <p><b>输入与索引约定</b>：
 * <ul>
 *   <li>{@link TermTidsetIndex} 中 {@link BitSet} 的下标与 {@link DocTerms#getDocId()} 一致；因此项集支持度 = 位图基数，
 *       交集即频繁性判定。</li>
 *   <li>若 {@code rows} 为 null 或空，或建索引后无任何词，则返回空 {@link ExclusiveSelectionResult#getGroups()}，
 *       统计字段按「无挖掘」语义置零（{@code maxCandidateCount} 仍为本次调用入参，便于日志对齐）。</li>
 * </ul>
 *
 * <p><b>默认性能参数</b>：见本类 {@code DEFAULT_*} 常量；需要与业务 SLA 一起调优时可改常量或后续扩展配置对象。
 *
 * @author muqingyu
 */
public final class ExclusiveFrequentItemsetSelector {

    /** 默认最大项集长度；增大可召回更长词组，但组合与耗时上升。 */
    private static final int DEFAULT_MAX_ITEMSET_SIZE = 6;
    /** 默认候选项硬上限；达到后挖掘提前结束，{@link ExclusiveSelectionResult#isTruncatedByCandidateLimit()} 为 true。 */
    private static final int DEFAULT_MAX_CANDIDATE_COUNT = 200000;
    /** 默认参与 Beam 扩展的频繁 1-项数量上限；截断低频词以控搜索空间。 */
    private static final int DEFAULT_MAX_FREQUENT_TERM_COUNT = 1000;
    /** 默认每个前缀每层最多扩展的子分支数；限制单层宽度。 */
    private static final int DEFAULT_MAX_BRANCHING_FACTOR = 16;
    /** 默认 Beam 每层保留的前缀数；越大越准、越慢。 */
    private static final int DEFAULT_BEAM_WIDTH = 32;
    /** 默认两阶段挑选中 1-opt 替换的尝试次数上限。 */
    private static final int DEFAULT_MAX_SWAP_TRIALS = 80;

    private ExclusiveFrequentItemsetSelector() {
    }

    /**
     * 使用 {@link #DEFAULT_MAX_ITEMSET_SIZE}、{@link #DEFAULT_MAX_CANDIDATE_COUNT}，仅返回互斥词组列表。
     *
     * @param rows 文档及词列表；见类注释「输入与索引约定」
     * @param minSupport 最小支持度（文档数），须为正整数
     * @param minItemsetSize 输出项集最小长度（词个数），须为正整数
     * @return 互斥条件下的 {@link SelectedGroup} 列表，可能为空
     */
    public static List<SelectedGroup> selectExclusiveBestItemsets(
            List<DocTerms> rows,
            int minSupport,
            int minItemsetSize
    ) {
        return selectExclusiveBestItemsetsWithStats(
                rows,
                minSupport,
                minItemsetSize,
                DEFAULT_MAX_ITEMSET_SIZE,
                DEFAULT_MAX_CANDIDATE_COUNT
        ).getGroups();
    }

    /**
     * 指定最大项集长度与候选上限，仅返回互斥词组列表。
     *
     * @param rows 文档及词列表
     * @param minSupport 最小支持度
     * @param minItemsetSize 最小项集长度
     * @param maxItemsetSize 最大项集长度（与最小长度等一并由 {@link SelectorConfig} 校验）
     * @param maxCandidateCount 候选项数量上限
     * @return 互斥条件下的 {@link SelectedGroup} 列表，可能为空
     */
    public static List<SelectedGroup> selectExclusiveBestItemsets(
            List<DocTerms> rows,
            int minSupport,
            int minItemsetSize,
            int maxItemsetSize,
            int maxCandidateCount
    ) {
        return selectExclusiveBestItemsetsWithStats(rows, minSupport, minItemsetSize, maxItemsetSize, maxCandidateCount)
                .getGroups();
    }

    /**
     * 使用默认 {@code maxItemsetSize}、{@code maxCandidateCount}，返回词组与完整统计。
     *
     * @param rows 文档及词列表
     * @param minSupport 最小支持度
     * @param minItemsetSize 最小项集长度
     * @return 结果与统计；统计含义见 {@link ExclusiveSelectionResult}
     */
    public static ExclusiveSelectionResult selectExclusiveBestItemsetsWithStats(
            List<DocTerms> rows,
            int minSupport,
            int minItemsetSize
    ) {
        return selectExclusiveBestItemsetsWithStats(
                rows,
                minSupport,
                minItemsetSize,
                DEFAULT_MAX_ITEMSET_SIZE,
                DEFAULT_MAX_CANDIDATE_COUNT
        );
    }

    /**
     * 完整流程入口：索引 → 挖掘 → 互斥挑选 → 组装对外结果。
     *
     * @param rows 文档及词列表；null 或 empty 时返回空组与零统计（{@code maxCandidateCount} 仍为入参）
     * @param minSupport 最小支持度
     * @param minItemsetSize 最小项集长度
     * @param maxItemsetSize 最大项集长度
     * @param maxCandidateCount 候选项上限
     * @return {@link ExclusiveSelectionResult}，含 {@link SelectedGroup} 与挖掘阶段计数
     */
    public static ExclusiveSelectionResult selectExclusiveBestItemsetsWithStats(
            List<DocTerms> rows,
            int minSupport,
            int minItemsetSize,
            int maxItemsetSize,
            int maxCandidateCount
    ) {
        if (rows == null || rows.isEmpty()) {
            return emptyResult(maxCandidateCount);
        }

        SelectorConfig config = new SelectorConfig(
                minSupport,
                minItemsetSize,
                maxItemsetSize,
                maxCandidateCount
        );

        TermTidsetIndex index = TermTidsetIndex.build(rows);
        if (index.getIdToTerm().isEmpty()) {
            return emptyResult(maxCandidateCount);
        }

        FrequentItemsetMiningResult miningResult = mineCandidates(index, config);
        List<CandidateItemset> candidates = miningResult.getCandidates();
        if (candidates.isEmpty()) {
            return buildResult(Collections.<SelectedGroup>emptyList(), miningResult, maxCandidateCount);
        }

        List<CandidateItemset> selected = pickExclusiveCandidates(candidates, index.getIdToTerm().size());
        List<SelectedGroup> groups = toSelectedGroups(selected, index.getIdToTerm());
        return buildResult(groups, miningResult, maxCandidateCount);
    }

    /**
     * 调用 Beam 挖掘器，并传入门面层固定的扩展/束宽参数。
     *
     * @param index 已构建的垂直索引
     * @param config 支持度与长度、候选上限
     * @return 候选项与统计
     */
    private static FrequentItemsetMiningResult mineCandidates(TermTidsetIndex index, SelectorConfig config) {
        BeamFrequentItemsetMiner miner = new BeamFrequentItemsetMiner();
        return miner.mineWithStats(
                index.getTidsetsByTermId(),
                config,
                DEFAULT_MAX_FREQUENT_TERM_COUNT,
                DEFAULT_MAX_BRANCHING_FACTOR,
                DEFAULT_BEAM_WIDTH
        );
    }

    /**
     * 两阶段互斥挑选；词典大小用于分配词占用位图。
     *
     * @param candidates 挖掘得到的候选项（顺序会影响替换阶段扫描次序）
     * @param dictionarySize {@code idToTerm.size()}，即 termId 上界（不含）参考
     * @return 互斥子集
     */
    private static List<CandidateItemset> pickExclusiveCandidates(
            List<CandidateItemset> candidates,
            int dictionarySize
    ) {
        TwoPhaseExclusiveItemsetPicker picker = new TwoPhaseExclusiveItemsetPicker();
        return picker.pick(candidates, dictionarySize, DEFAULT_MAX_SWAP_TRIALS);
    }

    /** 空输入或无词时的统一返回形态，避免调用方判 null。 */
    private static ExclusiveSelectionResult emptyResult(int maxCandidateCount) {
        return new ExclusiveSelectionResult(
                Collections.<SelectedGroup>emptyList(),
                0,
                0,
                0,
                maxCandidateCount,
                false
        );
    }

    /**
     * 将挖掘统计原样并入对外结果；{@code maxCandidateCount} 使用本次调用入参而非配置内字段，便于日志对比。
     */
    private static ExclusiveSelectionResult buildResult(
            List<SelectedGroup> groups,
            FrequentItemsetMiningResult miningResult,
            int maxCandidateCount
    ) {
        return new ExclusiveSelectionResult(
                groups,
                miningResult.getFrequentTermCount(),
                miningResult.getGeneratedCandidateCount(),
                miningResult.getIntersectionCount(),
                maxCandidateCount,
                miningResult.isTruncatedByCandidateLimit()
        );
    }

    /**
     * 将内部 {@link CandidateItemset} 转为对外 {@link SelectedGroup}。
     * <ul>
     *   <li>词字节通过 {@link ByteArrayUtils#copy(byte[])} 拷贝，避免暴露索引内部数组引用。</li>
     *   <li>doc 列表由 {@link #bitSetToDocIds(BitSet)} 按升序 docId 输出。</li>
     * </ul>
     */
    private static List<SelectedGroup> toSelectedGroups(
            List<CandidateItemset> selected,
            List<byte[]> idToTerm
    ) {
        List<SelectedGroup> out = new ArrayList<>(selected.size());
        for (CandidateItemset candidate : selected) {
            List<byte[]> terms = new ArrayList<>(candidate.getTermIds().length);
            for (int termId : candidate.getTermIds()) {
                terms.add(ByteArrayUtils.copy(idToTerm.get(termId)));
            }
            out.add(new SelectedGroup(
                    terms,
                    bitSetToDocIds(candidate.getDocBits()),
                    candidate.getSupport(),
                    candidate.getEstimatedSaving()
            ));
        }
        return out;
    }

    /**
     * 将项集命中的文档集合（位图）转为 docId 列表。
     * <p>前提：位下标与 {@link DocTerms#getDocId()} 一致（与 {@link TermTidsetIndex} 构建方式一致）。
     *
     * @param docBits 命中文档位图，可为空集
     * @return 升序 docId 列表
     */
    private static List<Integer> bitSetToDocIds(BitSet docBits) {
        List<Integer> out = new ArrayList<>();
        for (int i = docBits.nextSetBit(0); i >= 0; i = docBits.nextSetBit(i + 1)) {
            out.add(i);
        }
        return out;
    }
}
