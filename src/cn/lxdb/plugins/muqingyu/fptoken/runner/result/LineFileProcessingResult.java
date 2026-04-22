package cn.lxdb.plugins.muqingyu.fptoken.runner.result;

import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.DocTerms;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.ExclusiveSelectionResult;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.util.ByteArrayUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * 行文件入口的完整输出：原始加载结果、挖掘结果以及二次加工结果。
 */
public final class LineFileProcessingResult {

    private final List<DocTerms> loadedRows;
    private final ExclusiveSelectionResult selectionResult;
    private final DerivedData derivedData;

    public LineFileProcessingResult(
            List<DocTerms> loadedRows,
            ExclusiveSelectionResult selectionResult,
            DerivedData derivedData
    ) {
        this.loadedRows = Collections.unmodifiableList(new ArrayList<DocTerms>(
                Objects.requireNonNull(loadedRows, "loadedRows")));
        this.selectionResult = Objects.requireNonNull(selectionResult, "selectionResult");
        this.derivedData = Objects.requireNonNull(derivedData, "derivedData");
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
