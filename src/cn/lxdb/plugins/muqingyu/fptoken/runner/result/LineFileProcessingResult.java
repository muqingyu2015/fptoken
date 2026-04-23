package cn.lxdb.plugins.muqingyu.fptoken.runner.result;

import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.DocTerms;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.ExclusiveSelectionResult;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.SelectedGroup;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.config.EngineTuningConfig;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.util.ByteArrayKey;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.util.ByteArrayUtils;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * 行记录处理链路的完整输出：输入快照、挖掘结果以及索引派生结果。
 *
 * <p>虽然常见调用来自“文件加载 -> 处理”流程，但本模型同样适用于直接传入 rows 的 API 场景。</p>
 */
public final class LineFileProcessingResult {

    private final List<DocTerms> loadedRows;
    private final ExclusiveSelectionResult selectionResult;
    private final DerivedData derivedData;
    private final FinalIndexData finalIndexData;
    private final ProcessingStats processingStats;

    public LineFileProcessingResult(
            List<DocTerms> loadedRows,
            ExclusiveSelectionResult selectionResult,
            DerivedData derivedData
    ) {
        this(loadedRows, selectionResult, derivedData,
                EngineTuningConfig.DEFAULT_SKIP_HASH_MIN_GRAM,
                EngineTuningConfig.DEFAULT_SKIP_HASH_MAX_GRAM);
    }

    public LineFileProcessingResult(
            List<DocTerms> loadedRows,
            ExclusiveSelectionResult selectionResult,
            DerivedData derivedData,
            int skipHashMinGram,
            int skipHashMaxGram
    ) {
        this.loadedRows = Collections.unmodifiableList(new ArrayList<DocTerms>(
                Objects.requireNonNull(loadedRows, "loadedRows")));
        this.selectionResult = Objects.requireNonNull(selectionResult, "selectionResult");
        this.derivedData = Objects.requireNonNull(derivedData, "derivedData");
        this.finalIndexData = new FinalIndexData(
                this.loadedRows,
                selectionResult.getGroups(),
                derivedData.getHotTerms(),
                derivedData.getCutRes(),
                derivedData.getHotTermThresholdExclusive(),
                skipHashMinGram,
                skipHashMaxGram
        );
        this.processingStats = ProcessingStats.from(this.loadedRows, this.selectionResult, this.finalIndexData);
    }

    public List<DocTerms> getLoadedRows() {
        return loadedRows;
    }

    public ExclusiveSelectionResult getSelectionResult() {
        return selectionResult;
    }

    public DerivedData getDerivedData() {
        return derivedData;
    }

    /**
     * 最终给业务方（如 LXDB）消费的三元结果。
     *
     * <p>该结构明确把数据切成三块：</p>
     * <ul>
     *   <li>groups：高频互斥组合（多个 term 共享同一 docId 倒排表）</li>
     *   <li>hotTerms：高频单 term 倒排表</li>
     *   <li>cutRes：排除上述高频项后的低命中正排行数据（用于 skip index）</li>
     * </ul>
     */
    public FinalIndexData getFinalIndexData() {
        return finalIndexData;
    }

    /**
     * 统一统计视图：把输入规模、挖掘统计、派生层规模与索引规模聚合到一个对象里。
     *
     * <p>用于减少调用方在多个结果对象之间来回取数；旧字段访问方式仍保留兼容。</p>
     */
    public ProcessingStats getProcessingStats() {
        return processingStats;
    }

    /**
     * 便捷访问：等价于 getFinalIndexData().getGroups()。
     */
    public List<SelectedGroup> getGroups() {
        return finalIndexData.getGroups();
    }

    /**
     * 便捷访问：等价于 getFinalIndexData().getHotTerms()。
     */
    public List<HotTermDocList> getHotTerms() {
        return finalIndexData.getHotTerms();
    }

    /**
     * 便捷访问：等价于 getFinalIndexData().getCutRes()。
     */
    public List<DocTerms> getCutRes() {
        return finalIndexData.getCutRes();
    }

    /**
     * 可读性更强的顶层访问器：高频互斥组合倒排。
     * <p>语义等价于 {@link #getGroups()}。</p>
     */
    public List<SelectedGroup> getHighFreqMutexGroupPostings() {
        return finalIndexData.getHighFreqMutexGroupPostings();
    }

    /**
     * 可读性更强的顶层访问器：高频单词项倒排。
     * <p>语义等价于 {@link #getHotTerms()}。</p>
     */
    public List<HotTermDocList> getHighFreqSingleTermPostings() {
        return finalIndexData.getHighFreqSingleTermPostings();
    }

    /**
     * 可读性更强的顶层访问器：低命中残差正排行。
     * <p>语义等价于 {@link #getCutRes()}。</p>
     */
    public List<DocTerms> getLowHitForwardRows() {
        return finalIndexData.getLowHitForwardRows();
    }

    /**
     * 便捷访问：等价于 getFinalIndexData().getHighFreqMutexGroupTermsToIndex()。
     */
    public List<TermsPostingIndexRef> getHighFreqMutexGroupTermsToIndex() {
        return finalIndexData.getHighFreqMutexGroupTermsToIndex();
    }

    /**
     * 便捷访问：等价于 getFinalIndexData().getHighFreqSingleTermToIndex()。
     */
    public List<TermsPostingIndexRef> getHighFreqSingleTermToIndex() {
        return finalIndexData.getHighFreqSingleTermToIndex();
    }

    /**
     * 便捷访问：等价于 getFinalIndexData().getLowHitTermToIndexes()。
     */
    public List<TermsPostingIndexRef> getLowHitTermToIndexes() {
        return finalIndexData.getLowHitTermToIndexes();
    }

    /**
     * 便捷访问：等价于 getFinalIndexData().getOneByteDocidBitsetIndex()。
     */
    public OneByteDocidBitsetIndex getOneByteDocidBitsetIndex() {
        return finalIndexData.getOneByteDocidBitsetIndex();
    }

    /**
     * 二次加工输出：从 cut_res 与 hot_terms 中剔除 result 词项后的结果。
     */
    public static final class DerivedData {
        private final int hotTermThresholdExclusive;
        private final List<DocTerms> cutRes;
        private final List<HotTermDocList> hotTerms;

        public DerivedData(
                int hotTermThresholdExclusive,
                List<DocTerms> cutRes,
                List<HotTermDocList> hotTerms
        ) {
            this.hotTermThresholdExclusive = hotTermThresholdExclusive;
            this.cutRes = Collections.unmodifiableList(new ArrayList<DocTerms>(
                    Objects.requireNonNull(cutRes, "cutRes")));
            this.hotTerms = Collections.unmodifiableList(new ArrayList<HotTermDocList>(
                    Objects.requireNonNull(hotTerms, "hotTerms")));
        }

        /**
         * hotTerms 的筛选阈值（严格大于该值才保留）。
         */
        public int getHotTermThresholdExclusive() {
            return hotTermThresholdExclusive;
        }

        /**
         * 原始 rows 的拷贝视图，已移除 selectionResult 中出现过的词项。
         */
        public List<DocTerms> getCutRes() {
            return cutRes;
        }

        /**
         * 高频词文档表，已移除 selectionResult 中出现过的词项。
         */
        public List<HotTermDocList> getHotTerms() {
            return hotTerms;
        }
    }

    /**
     * 处理链路统一统计快照（只读）。
     *
     * <p>把原本分散在 {@link ExclusiveSelectionResult} 与 {@link FinalIndexData}
     * 的常用计数整合到一个入口，避免调用方在多个数据结构间跳转。</p>
     */
    public static final class ProcessingStats {
        private final int inputRowCount;
        private final int selectedGroupCount;
        private final int highFreqSingleTermCount;
        private final int lowHitForwardRowCount;
        private final int highFreqMutexGroupTermsToIndexCount;
        private final int highFreqSingleTermToIndexCount;
        private final int lowHitTermToIndexesCount;
        private final int frequentTermCount;
        private final int candidateCount;
        private final int intersectionCount;
        private final int maxCandidateCount;
        private final boolean truncatedByCandidateLimit;
        private final int hotTermThresholdExclusive;
        private final int skipHashMinGram;
        private final int skipHashMaxGram;
        private final int oneByteIndexMaxDocId;
        private final int highFreqSingleTermCandidateCountBeforeDedup;
        private final int highFreqSingleTermMovedToMutexGroupCount;
        private final double highFreqSingleTermMovedToMutexGroupPercent;
        private final double lowHitForwardRowsPercent;
        private final double lowHitForwardTermsPercent;

        private ProcessingStats(
                int inputRowCount,
                int selectedGroupCount,
                int highFreqSingleTermCount,
                int lowHitForwardRowCount,
                int highFreqMutexGroupTermsToIndexCount,
                int highFreqSingleTermToIndexCount,
                int lowHitTermToIndexesCount,
                int frequentTermCount,
                int candidateCount,
                int intersectionCount,
                int maxCandidateCount,
                boolean truncatedByCandidateLimit,
                int hotTermThresholdExclusive,
                int skipHashMinGram,
                int skipHashMaxGram,
                int oneByteIndexMaxDocId,
                int highFreqSingleTermCandidateCountBeforeDedup,
                int highFreqSingleTermMovedToMutexGroupCount,
                double highFreqSingleTermMovedToMutexGroupPercent,
                double lowHitForwardRowsPercent,
                double lowHitForwardTermsPercent
        ) {
            this.inputRowCount = inputRowCount;
            this.selectedGroupCount = selectedGroupCount;
            this.highFreqSingleTermCount = highFreqSingleTermCount;
            this.lowHitForwardRowCount = lowHitForwardRowCount;
            this.highFreqMutexGroupTermsToIndexCount = highFreqMutexGroupTermsToIndexCount;
            this.highFreqSingleTermToIndexCount = highFreqSingleTermToIndexCount;
            this.lowHitTermToIndexesCount = lowHitTermToIndexesCount;
            this.frequentTermCount = frequentTermCount;
            this.candidateCount = candidateCount;
            this.intersectionCount = intersectionCount;
            this.maxCandidateCount = maxCandidateCount;
            this.truncatedByCandidateLimit = truncatedByCandidateLimit;
            this.hotTermThresholdExclusive = hotTermThresholdExclusive;
            this.skipHashMinGram = skipHashMinGram;
            this.skipHashMaxGram = skipHashMaxGram;
            this.oneByteIndexMaxDocId = oneByteIndexMaxDocId;
            this.highFreqSingleTermCandidateCountBeforeDedup = highFreqSingleTermCandidateCountBeforeDedup;
            this.highFreqSingleTermMovedToMutexGroupCount = highFreqSingleTermMovedToMutexGroupCount;
            this.highFreqSingleTermMovedToMutexGroupPercent = highFreqSingleTermMovedToMutexGroupPercent;
            this.lowHitForwardRowsPercent = lowHitForwardRowsPercent;
            this.lowHitForwardTermsPercent = lowHitForwardTermsPercent;
        }

        private static ProcessingStats from(
                List<DocTerms> loadedRows,
                ExclusiveSelectionResult selectionResult,
                FinalIndexData finalIndexData
        ) {
            HighFreqTermMergeStats mergeStats = computeHighFreqTermMergeStats(
                    loadedRows,
                    finalIndexData.getHighFreqMutexGroupPostings(),
                    finalIndexData.getHotTermThresholdExclusive()
            );
            int lowHitRows = finalIndexData.getLowHitForwardRows().size();
            int loadedRowsCount = loadedRows.size();
            int totalInputTerms = countTerms(loadedRows);
            int lowHitTerms = countTerms(finalIndexData.getLowHitForwardRows());
            return new ProcessingStats(
                    loadedRowsCount,
                    finalIndexData.getHighFreqMutexGroupPostings().size(),
                    finalIndexData.getHighFreqSingleTermPostings().size(),
                    lowHitRows,
                    finalIndexData.getHighFreqMutexGroupTermsToIndex().size(),
                    finalIndexData.getHighFreqSingleTermToIndex().size(),
                    finalIndexData.getLowHitTermToIndexes().size(),
                    selectionResult.getFrequentTermCount(),
                    selectionResult.getCandidateCount(),
                    selectionResult.getIntersectionCount(),
                    selectionResult.getMaxCandidateCount(),
                    selectionResult.isTruncatedByCandidateLimit(),
                    finalIndexData.getHotTermThresholdExclusive(),
                    finalIndexData.getSkipHashMinGram(),
                    finalIndexData.getSkipHashMaxGram(),
                    finalIndexData.getOneByteDocidBitsetIndex().getMaxDocId(),
                    mergeStats.totalHighFreqSingleTermsBeforeDedup,
                    mergeStats.movedIntoMutexGroups,
                    ratioPercent(mergeStats.movedIntoMutexGroups, mergeStats.totalHighFreqSingleTermsBeforeDedup),
                    ratioPercent(lowHitRows, loadedRowsCount),
                    ratioPercent(lowHitTerms, totalInputTerms)
            );
        }

        private static int countTerms(List<DocTerms> rows) {
            int total = 0;
            for (DocTerms row : rows) {
                total += row.getTermsUnsafe().size();
            }
            return total;
        }

        private static HighFreqTermMergeStats computeHighFreqTermMergeStats(
                List<DocTerms> loadedRows,
                List<SelectedGroup> highFreqMutexGroupPostings,
                int hotTermThresholdExclusive
        ) {
            Map<ByteArrayKey, Set<Integer>> termToDocIds = new HashMap<ByteArrayKey, Set<Integer>>();
            for (DocTerms row : loadedRows) {
                int docId = row.getDocId();
                for (byte[] term : row.getTermsUnsafe()) {
                    ByteArrayKey key = new ByteArrayKey(term);
                    Set<Integer> docIds = termToDocIds.get(key);
                    if (docIds == null) {
                        docIds = new HashSet<Integer>();
                        termToDocIds.put(key, docIds);
                    }
                    docIds.add(Integer.valueOf(docId));
                }
            }

            Set<ByteArrayKey> mutexTerms = new HashSet<ByteArrayKey>();
            for (SelectedGroup group : highFreqMutexGroupPostings) {
                for (byte[] term : group.getTerms()) {
                    mutexTerms.add(new ByteArrayKey(term));
                }
            }

            int totalHighFreqSingleTermsBeforeDedup = 0;
            int movedIntoMutexGroups = 0;
            for (Map.Entry<ByteArrayKey, Set<Integer>> entry : termToDocIds.entrySet()) {
                if (entry.getValue().size() > hotTermThresholdExclusive) {
                    totalHighFreqSingleTermsBeforeDedup++;
                    if (mutexTerms.contains(entry.getKey())) {
                        movedIntoMutexGroups++;
                    }
                }
            }
            return new HighFreqTermMergeStats(totalHighFreqSingleTermsBeforeDedup, movedIntoMutexGroups);
        }

        private static double ratioPercent(int numerator, int denominator) {
            if (denominator <= 0) {
                return 0.0d;
            }
            return (numerator * 100.0d) / denominator;
        }

        public int getInputRowCount() {
            return inputRowCount;
        }

        public int getSelectedGroupCount() {
            return selectedGroupCount;
        }

        public int getHighFreqSingleTermCount() {
            return highFreqSingleTermCount;
        }

        public int getLowHitForwardRowCount() {
            return lowHitForwardRowCount;
        }

        public int getHighFreqMutexGroupTermsToIndexCount() {
            return highFreqMutexGroupTermsToIndexCount;
        }

        public int getHighFreqSingleTermToIndexCount() {
            return highFreqSingleTermToIndexCount;
        }

        public int getLowHitTermToIndexesCount() {
            return lowHitTermToIndexesCount;
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

        public int getHotTermThresholdExclusive() {
            return hotTermThresholdExclusive;
        }

        public int getSkipHashMinGram() {
            return skipHashMinGram;
        }

        public int getSkipHashMaxGram() {
            return skipHashMaxGram;
        }

        public int getOneByteIndexMaxDocId() {
            return oneByteIndexMaxDocId;
        }

        /**
         * “高频单词候选（扣除组合层之前）”总数。
         *
         * <p>口径：在输入 rows 中，doc 覆盖数严格大于 hotTermThresholdExclusive 的单词数量。</p>
         */
        public int getHighFreqSingleTermCandidateCountBeforeDedup() {
            return highFreqSingleTermCandidateCountBeforeDedup;
        }

        /**
         * 被高频组合层吸收（从高频单词层移出以减少重复）的单词数量。
         */
        public int getHighFreqSingleTermMovedToMutexGroupCount() {
            return highFreqSingleTermMovedToMutexGroupCount;
        }

        /**
         * 被高频组合层吸收的单词占比（相对“高频单词候选总数”）。
         */
        public double getHighFreqSingleTermMovedToMutexGroupPercent() {
            return highFreqSingleTermMovedToMutexGroupPercent;
        }

        /**
         * 低频剩余层在整体行数中的占比（百分比）。
         */
        public double getLowHitForwardRowsPercent() {
            return lowHitForwardRowsPercent;
        }

        /**
         * 低频剩余层在整体词项数中的占比（百分比）。
         */
        public double getLowHitForwardTermsPercent() {
            return lowHitForwardTermsPercent;
        }

        private static final class HighFreqTermMergeStats {
            private final int totalHighFreqSingleTermsBeforeDedup;
            private final int movedIntoMutexGroups;

            private HighFreqTermMergeStats(int totalHighFreqSingleTermsBeforeDedup, int movedIntoMutexGroups) {
                this.totalHighFreqSingleTermsBeforeDedup = totalHighFreqSingleTermsBeforeDedup;
                this.movedIntoMutexGroups = movedIntoMutexGroups;
            }
        }
    }

    /**
     * 最终索引数据模型：把业务真正需要的三部分放在一个对象里返回。
     *
     * <p>语义约束（按你的落库模型定义）：</p>
     * <ul>
     *   <li><b>groups</b>：高频互斥组合。一个 group 内包含多个 term，这些 term 共享同一份 docId 列表（倒排）。</li>
     *   <li><b>hotTerms</b>：高频单 term 倒排。每个 term 独立对应自己的 docId 列表。</li>
     *   <li><b>cutRes</b>：低命中残差正排。它是从原始 rows 中移除 groups/hotTerms 中 term 后剩下的行数据，
     *       适合配合 skip index 做低频路径检索。</li>
     * </ul>
     *
     * <p>因此这三块共同形成“高频倒排 + 低频正排”的双层索引输入。</p>
     */
    public static final class FinalIndexData {
        private final List<DocTerms> loadedRows;
        private final List<SelectedGroup> highFreqMutexGroupPostings;
        private final List<HotTermDocList> highFreqSingleTermPostings;
        private final List<DocTerms> lowHitForwardRows;
        private final List<TermsPostingIndexRef> highFreqMutexGroupTermsToIndex;
        private final List<TermsPostingIndexRef> highFreqSingleTermToIndex;
        private final List<TermsPostingIndexRef> lowHitTermToIndexes;
        private final TermBlockSkipBitsetIndex highFreqMutexGroupSkipBitsetIndex;
        private final TermBlockSkipBitsetIndex highFreqSingleTermSkipBitsetIndex;
        private final TermBlockSkipBitsetIndex lowHitTermSkipBitsetIndex;
        private final OneByteDocidBitsetIndex oneByteDocidBitsetIndex;
        private final int skipHashMinGram;
        private final int skipHashMaxGram;
        private final int hotTermThresholdExclusive;

        public FinalIndexData(
                List<DocTerms> loadedRows,
                List<SelectedGroup> groups,
                List<HotTermDocList> hotTerms,
                List<DocTerms> cutRes,
                int hotTermThresholdExclusive,
                int skipHashMinGram,
                int skipHashMaxGram
        ) {
            LineFileIndexBuilders.validateSkipHashGramRange(skipHashMinGram, skipHashMaxGram);
            this.loadedRows = Collections.unmodifiableList(new ArrayList<DocTerms>(
                    Objects.requireNonNull(loadedRows, "loadedRows")));
            this.highFreqMutexGroupPostings = Collections.unmodifiableList(new ArrayList<SelectedGroup>(
                    Objects.requireNonNull(groups, "groups")));
            this.highFreqSingleTermPostings = Collections.unmodifiableList(new ArrayList<HotTermDocList>(
                    Objects.requireNonNull(hotTerms, "hotTerms")));
            this.lowHitForwardRows = Collections.unmodifiableList(new ArrayList<DocTerms>(
                    Objects.requireNonNull(cutRes, "cutRes")));
            this.highFreqMutexGroupTermsToIndex = Collections.unmodifiableList(
                    LineFileIndexBuilders.buildHighFreqMutexGroupTermsToIndex(this.highFreqMutexGroupPostings));
            this.highFreqSingleTermToIndex = Collections.unmodifiableList(
                    LineFileIndexBuilders.buildHighFreqSingleTermToIndex(this.highFreqSingleTermPostings));
            this.lowHitTermToIndexes = Collections.unmodifiableList(
                    LineFileIndexBuilders.buildLowHitTermToIndexes(this.lowHitForwardRows));
            this.skipHashMinGram = skipHashMinGram;
            this.skipHashMaxGram = skipHashMaxGram;
            this.highFreqMutexGroupSkipBitsetIndex = LineFileIndexBuilders.buildTermBlockSkipBitsetIndex(
                    this.highFreqMutexGroupTermsToIndex, skipHashMinGram, skipHashMaxGram);
            this.highFreqSingleTermSkipBitsetIndex = LineFileIndexBuilders.buildTermBlockSkipBitsetIndex(
                    this.highFreqSingleTermToIndex, skipHashMinGram, skipHashMaxGram);
            this.lowHitTermSkipBitsetIndex = LineFileIndexBuilders.buildTermBlockSkipBitsetIndex(
                    this.lowHitTermToIndexes, skipHashMinGram, skipHashMaxGram);
            this.oneByteDocidBitsetIndex = LineFileIndexBuilders.buildOneByteDocidBitsetIndex(this.loadedRows);
            this.hotTermThresholdExclusive = hotTermThresholdExclusive;
        }

        /**
         * 高频互斥组合倒排集合。
         *
         * <p>每个 SelectedGroup 表示一个互斥高频模式：
         * group 内多个 term 共享同一 docId 命中集合（倒排关系）。</p>
         */
        public List<SelectedGroup> getHighFreqMutexGroupPostings() {
            return highFreqMutexGroupPostings;
        }

        /**
         * 高频单 term 倒排集合。
         *
         * <p>它与 groups 一起构成“高频层”的倒排存储输入。</p>
         */
        public List<HotTermDocList> getHighFreqSingleTermPostings() {
            return highFreqSingleTermPostings;
        }

        /**
         * 低命中残差正排数据（排除了 groups/hotTerms 中的高频 term）。
         *
         * <p>该部分用于 skip index 路径，避免把低频词也塞进高频倒排层。</p>
         */
        public List<DocTerms> getLowHitForwardRows() {
            return lowHitForwardRows;
        }

        /**
         * 高频互斥组合的“terms -> postingIndex”倒排引用。
         *
         * <p>index 为 {@link #getHighFreqMutexGroupPostings()} 列表内下标（从 0 开始）。
         * 即把原始 "t1 t2 t3 -> docIds" 进一步抽象为 "t1 t2 t3 -> index"。</p>
         */
        public List<TermsPostingIndexRef> getHighFreqMutexGroupTermsToIndex() {
            return highFreqMutexGroupTermsToIndex;
        }

        /**
         * 高频单词项的“term -> postingIndex”倒排引用。
         *
         * <p>index 为 {@link #getHighFreqSingleTermPostings()} 列表内下标（从 0 开始）。
         * 即把原始 "t1 -> docIds" 进一步抽象为 "t1 -> index"。</p>
         */
        public List<TermsPostingIndexRef> getHighFreqSingleTermToIndex() {
            return highFreqSingleTermToIndex;
        }

        /**
         * 低命中层“展开后的倒排明细”引用。
         *
         * <p>每条记录形态与互斥组引用保持一致：{@code terms + postingIndex}。
         * 其中 terms 只包含 1 个 term，postingIndex 为
         * 对应 {@link DocTerms#getDocId()} 的值。</p>
         *
         * <p>同一 postingIndex（同一 docId）会对应多条记录，因为一条记录可包含多个 term。</p>
         */
        public List<TermsPostingIndexRef> getLowHitTermToIndexes() {
            return lowHitTermToIndexes;
        }

        /**
         * 高频互斥组块的 skip-index BitSet 结构。
         */
        public TermBlockSkipBitsetIndex getHighFreqMutexGroupSkipBitsetIndex() {
            return highFreqMutexGroupSkipBitsetIndex;
        }

        /**
         * 高频单词项块的 skip-index BitSet 结构。
         */
        public TermBlockSkipBitsetIndex getHighFreqSingleTermSkipBitsetIndex() {
            return highFreqSingleTermSkipBitsetIndex;
        }

        /**
         * 低频词块的 skip-index BitSet 结构。
         */
        public TermBlockSkipBitsetIndex getLowHitTermSkipBitsetIndex() {
            return lowHitTermSkipBitsetIndex;
        }

        /**
         * 1-byte 粒度倒排 BitSet：byte(0~255) -> docId(bitset)。
         *
         * <p>该结构基于未分词的 {@code loadedRows} 构建，适用于单字节快速检索。</p>
         */
        public OneByteDocidBitsetIndex getOneByteDocidBitsetIndex() {
            return oneByteDocidBitsetIndex;
        }

        /**
         * hotTerms 的阈值定义（严格大于该值才进入 hotTerms）。
         */
        public int getHotTermThresholdExclusive() {
            return hotTermThresholdExclusive;
        }

        public int getSkipHashMinGram() {
            return skipHashMinGram;
        }

        public int getSkipHashMaxGram() {
            return skipHashMaxGram;
        }

        /**
         * 兼容旧命名：groups == highFreqMutexGroupPostings。
         */
        public List<SelectedGroup> getGroups() {
            return getHighFreqMutexGroupPostings();
        }

        /**
         * 兼容旧命名：hotTerms == highFreqSingleTermPostings。
         */
        public List<HotTermDocList> getHotTerms() {
            return getHighFreqSingleTermPostings();
        }

        /**
         * 兼容旧命名：cutRes == lowHitForwardRows。
         */
        public List<DocTerms> getCutRes() {
            return getLowHitForwardRows();
        }

        /**
         * 中间步骤测试入口：用于独立验证每个派生阶段的构建结果。
         *
         * <p>仅透传内部构建方法，不改变生产逻辑。</p>
         */
        public static final class IntermediateSteps {
            private IntermediateSteps() {
            }

            public static List<TermsPostingIndexRef> buildHighFreqMutexGroupTermsToIndex(
                    List<SelectedGroup> groups
            ) {
                return LineFileIndexBuilders.buildHighFreqMutexGroupTermsToIndex(groups);
            }

            public static List<TermsPostingIndexRef> buildHighFreqSingleTermToIndex(
                    List<HotTermDocList> hotTerms
            ) {
                return LineFileIndexBuilders.buildHighFreqSingleTermToIndex(hotTerms);
            }

            public static List<TermsPostingIndexRef> buildLowHitTermToIndexes(
                    List<DocTerms> lowHitForwardRows
            ) {
                return LineFileIndexBuilders.buildLowHitTermToIndexes(lowHitForwardRows);
            }

            public static TermBlockSkipBitsetIndex buildTermBlockSkipBitsetIndex(
                    List<TermsPostingIndexRef> refs,
                    int skipHashMinGram,
                    int skipHashMaxGram
            ) {
                return LineFileIndexBuilders.buildTermBlockSkipBitsetIndex(refs, skipHashMinGram, skipHashMaxGram);
            }

            public static OneByteDocidBitsetIndex buildOneByteDocidBitsetIndex(List<DocTerms> loadedRows) {
                return LineFileIndexBuilders.buildOneByteDocidBitsetIndex(loadedRows);
            }
        }
    }

    /**
     * 统一的索引引用结构：terms + postingIndex。
     *
     * <p>三块新结构（高频互斥组、高频单词项、低命中展开）都使用该同一类型名，
     * 差异仅体现在构建语义：terms 可以是多项（互斥组）或单项（其余两块）。</p>
     */
    public static final class TermsPostingIndexRef {
        private final List<byte[]> terms;
        private final int postingIndex;

        public TermsPostingIndexRef(byte[] term, int postingIndex) {
            this.terms = Collections.singletonList(
                    ByteArrayUtils.copy(Objects.requireNonNull(term, "term")));
            this.postingIndex = postingIndex;
        }

        public TermsPostingIndexRef(List<byte[]> terms, int postingIndex) {
            Objects.requireNonNull(terms, "terms");
            if (terms.size() == 1) {
                this.terms = Collections.singletonList(
                        ByteArrayUtils.copy(Objects.requireNonNull(terms.get(0), "term")));
                this.postingIndex = postingIndex;
                return;
            }
            List<byte[]> copied = new ArrayList<byte[]>(terms.size());
            for (byte[] term : terms) {
                copied.add(ByteArrayUtils.copy(Objects.requireNonNull(term, "term")));
            }
            this.terms = Collections.unmodifiableList(copied);
            this.postingIndex = postingIndex;
        }

        public List<byte[]> getTerms() {
            List<byte[]> copied = new ArrayList<byte[]>(terms.size());
            for (byte[] term : terms) {
                copied.add(ByteArrayUtils.copy(term));
            }
            return copied;
        }

        public int getPostingIndex() {
            return postingIndex;
        }

        /** 仅供 runner.result 包内构建路径使用；外部仍应通过 {@link #getTerms()} 获取防御性副本。 */
        List<byte[]> getTermsUnsafe() {
            return terms;
        }
    }

    /**
     * 单个 Term 块对应的一套 skip-index BitSet。
     *
     * <p>按配置的 n-gram 区间与 term 实际长度按需生成哈希层；每层固定 256 个桶（0..255），
     * 桶内 BitSet 的位号为 postingIndex。</p>
     */
    public static final class TermBlockSkipBitsetIndex {
        private final int maxPostingIndex;
        private final List<HashLevelBitsets> hashLevels;

        public TermBlockSkipBitsetIndex(int maxPostingIndex, List<HashLevelBitsets> hashLevels) {
            this.maxPostingIndex = maxPostingIndex;
            this.hashLevels = Collections.unmodifiableList(new ArrayList<HashLevelBitsets>(
                    Objects.requireNonNull(hashLevels, "hashLevels")));
        }

        public int getMaxPostingIndex() {
            return maxPostingIndex;
        }

        public List<HashLevelBitsets> getHashLevels() {
            return new ArrayList<HashLevelBitsets>(hashLevels);
        }
    }

    /**
     * 某一 n-gram 级别的 256 桶 BitSet 结构。
     */
    public static final class HashLevelBitsets {
        private final int gramLength;
        private final List<BitSet> buckets;

        public HashLevelBitsets(int gramLength, List<BitSet> buckets) {
            this.gramLength = gramLength;
            Objects.requireNonNull(buckets, "buckets");
            if (buckets.size() != 256) {
                throw new IllegalArgumentException("buckets.size must be 256");
            }
            List<BitSet> copied = new ArrayList<BitSet>(buckets.size());
            for (BitSet bucket : buckets) {
                copied.add((BitSet) Objects.requireNonNull(bucket, "bucket").clone());
            }
            this.buckets = Collections.unmodifiableList(copied);
        }

        public int getGramLength() {
            return gramLength;
        }

        public List<BitSet> getBuckets() {
            List<BitSet> copied = new ArrayList<BitSet>(buckets.size());
            for (BitSet bucket : buckets) {
                copied.add((BitSet) bucket.clone());
            }
            return copied;
        }
    }

    /**
     * 单字节倒排 BitSet 结构。
     *
     * <p>固定 256 个桶，桶下标即无符号 byte 值（0~255），桶内 BitSet 位号为 docId。</p>
     */
    public static final class OneByteDocidBitsetIndex {
        private final int maxDocId;
        private final List<BitSet> buckets;

        public OneByteDocidBitsetIndex(int maxDocId, List<BitSet> buckets) {
            this.maxDocId = maxDocId;
            Objects.requireNonNull(buckets, "buckets");
            if (buckets.size() != 256) {
                throw new IllegalArgumentException("buckets.size must be 256");
            }
            List<BitSet> copied = new ArrayList<BitSet>(buckets.size());
            for (BitSet bucket : buckets) {
                copied.add((BitSet) Objects.requireNonNull(bucket, "bucket").clone());
            }
            this.buckets = Collections.unmodifiableList(copied);
        }

        public int getMaxDocId() {
            return maxDocId;
        }

        public BitSet getDocIdBitset(int unsignedByte) {
            if (unsignedByte < 0 || unsignedByte > 255) {
                throw new IllegalArgumentException("unsignedByte must be in [0,255]");
            }
            return (BitSet) buckets.get(unsignedByte).clone();
        }

        public List<BitSet> getBuckets() {
            List<BitSet> copied = new ArrayList<BitSet>(buckets.size());
            for (BitSet bucket : buckets) {
                copied.add((BitSet) bucket.clone());
            }
            return copied;
        }
    }

    /**
     * 高频词及其文档列表。
     */
    public static final class HotTermDocList {
        private final byte[] term;
        private final List<Integer> docIds;
        private final int count;

        public HotTermDocList(byte[] term, List<Integer> docIds) {
            this.term = ByteArrayUtils.copy(Objects.requireNonNull(term, "term"));
            this.docIds = Collections.unmodifiableList(new ArrayList<Integer>(
                    Objects.requireNonNull(docIds, "docIds")));
            this.count = this.docIds.size();
        }

        public byte[] getTerm() {
            return ByteArrayUtils.copy(term);
        }

        /**
         * 返回副本，避免外部修改内部状态。
         */
        public List<Integer> getDocIds() {
            return new ArrayList<Integer>(docIds);
        }

        public int getCount() {
            return count;
        }
    }
}
