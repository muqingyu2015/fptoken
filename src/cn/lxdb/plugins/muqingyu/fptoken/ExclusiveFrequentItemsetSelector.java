package cn.lxdb.plugins.muqingyu.fptoken;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 作者：muqingyu
 *
 * 门面类：对外提供互斥频繁项集选择 API。
 *
 * 设计原理（高层）：
 * 1) 先把输入文档转换为「termId -> tidset(BitSet)」的垂直结构。
 * 2) 在垂直结构上挖掘频繁项集，保留每个项集对应的 docBits（即 doclist）。
 * 3) 对候选项集做贪心互斥选择（同一个词只能进入一个结果组）。
 * 4) 返回最终词组 + doclist。
 *
 * 这样做的好处：
 * - 可以直接得到你关心的「项集 -> 原始 docid 列表」。
 * - 利用 BitSet 交集，候选支持度计算非常快。
 * - 贪心路径在大规模数据下更稳定，便于线上落地。
 */
public final class ExclusiveFrequentItemsetSelector {

    private static final int DEFAULT_MAX_ITEMSET_SIZE = 6;
    private static final int DEFAULT_MAX_CANDIDATE_COUNT = 200_000;

    private ExclusiveFrequentItemsetSelector() {
    }

    public static List<SelectedGroup> selectExclusiveBestItemsets(
            List<DocTerms> rows,
            int minSupport,
            int minItemsetSize
    ) {
        return selectExclusiveBestItemsets(
                rows,
                minSupport,
                minItemsetSize,
                DEFAULT_MAX_ITEMSET_SIZE,
                DEFAULT_MAX_CANDIDATE_COUNT
        );
    }

    public static List<SelectedGroup> selectExclusiveBestItemsets(
            List<DocTerms> rows,
            int minSupport,
            int minItemsetSize,
            int maxItemsetSize,
            int maxCandidateCount
    ) {
        // 空输入直接返回，避免后续构建结构的无效开销。
        if (rows == null || rows.isEmpty()) {
            return Collections.emptyList();
        }
        // 所有边界参数统一在配置类中校验。
        SelectorConfig config = new SelectorConfig(
                minSupport,
                minItemsetSize,
                maxItemsetSize,
                maxCandidateCount
        );

        // 建立 docId <-> 行下标映射。
        // BitSet 内部使用连续下标，最终再映射回真实 docId。
        Map<Integer, Integer> docIdToIndex = new HashMap<Integer, Integer>(rows.size() * 2);
        List<Integer> indexToDocId = new ArrayList<Integer>(rows.size());
        for (int i = 0; i < rows.size(); i++) {
            int docId = rows.get(i).getDocId();
            docIdToIndex.put(docId, i);
            indexToDocId.add(docId);
        }

        // 词典 + 倒排位图索引构建。
        TermTidsetIndex index = TermTidsetIndex.build(rows, docIdToIndex);
        if (index.getIdToTerm().isEmpty()) {
            return Collections.emptyList();
        }

        // 频繁项集挖掘：输出候选项集及其 docBits。
        FrequentItemsetMiner miner = new FrequentItemsetMiner();
        List<CandidateItemset> candidates = miner.mine(index.getTidsetsByTermId(), config);
        if (candidates.isEmpty()) {
            return Collections.emptyList();
        }

        // 贪心互斥选择：长度优先、价值优先、支持度优先。
        GreedyExclusiveItemsetPicker picker = new GreedyExclusiveItemsetPicker();
        List<CandidateItemset> selected = picker.pick(candidates, index.getIdToTerm().size());
        return toSelectedGroups(selected, index.getIdToTerm(), indexToDocId);
    }

    private static List<SelectedGroup> toSelectedGroups(
            List<CandidateItemset> selected,
            List<byte[]> idToTerm,
            List<Integer> indexToDocId
    ) {
        List<SelectedGroup> out = new ArrayList<SelectedGroup>(selected.size());
        for (CandidateItemset candidate : selected) {
            List<byte[]> terms = new ArrayList<byte[]>(candidate.getTermIds().length);
            for (int termId : candidate.getTermIds()) {
                terms.add(ByteArrayUtils.copy(idToTerm.get(termId)));
            }
            out.add(new SelectedGroup(
                    terms,
                    bitSetToDocIds(candidate.getDocBits(), indexToDocId),
                    candidate.getSupport(),
                    candidate.getEstimatedSaving()
            ));
        }
        return out;
    }

    private static List<Integer> bitSetToDocIds(BitSet docBits, List<Integer> indexToDocId) {
        List<Integer> out = new ArrayList<Integer>();
        // BitSet 的 set bit 即命中文档下标，映射回真实 docId。
        for (int i = docBits.nextSetBit(0); i >= 0; i = docBits.nextSetBit(i + 1)) {
            out.add(indexToDocId.get(i));
        }
        return out;
    }
}
