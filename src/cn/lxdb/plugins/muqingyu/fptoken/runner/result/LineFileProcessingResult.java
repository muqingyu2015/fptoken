package cn.lxdb.plugins.muqingyu.fptoken.runner.result;

import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.DocTerms;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.ExclusiveSelectionResult;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.SelectedGroup;
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
    private final FinalIndexData finalIndexData;

    public LineFileProcessingResult(
            List<DocTerms> loadedRows,
            ExclusiveSelectionResult selectionResult,
            DerivedData derivedData
    ) {
        this.loadedRows = Collections.unmodifiableList(new ArrayList<DocTerms>(
                Objects.requireNonNull(loadedRows, "loadedRows")));
        this.selectionResult = Objects.requireNonNull(selectionResult, "selectionResult");
        this.derivedData = Objects.requireNonNull(derivedData, "derivedData");
        this.finalIndexData = new FinalIndexData(
                selectionResult.getGroups(),
                derivedData.getHotTerms(),
                derivedData.getCutRes(),
                derivedData.getHotTermThresholdExclusive()
        );
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
        private final List<SelectedGroup> highFreqMutexGroupPostings;
        private final List<HotTermDocList> highFreqSingleTermPostings;
        private final List<DocTerms> lowHitForwardRows;
        private final List<TermsPostingIndexRef> highFreqMutexGroupTermsToIndex;
        private final List<TermsPostingIndexRef> highFreqSingleTermToIndex;
        private final List<TermsPostingIndexRef> lowHitTermToIndexes;
        private final int hotTermThresholdExclusive;

        public FinalIndexData(
                List<SelectedGroup> groups,
                List<HotTermDocList> hotTerms,
                List<DocTerms> cutRes,
                int hotTermThresholdExclusive
        ) {
            this.highFreqMutexGroupPostings = Collections.unmodifiableList(new ArrayList<SelectedGroup>(
                    Objects.requireNonNull(groups, "groups")));
            this.highFreqSingleTermPostings = Collections.unmodifiableList(new ArrayList<HotTermDocList>(
                    Objects.requireNonNull(hotTerms, "hotTerms")));
            this.lowHitForwardRows = Collections.unmodifiableList(new ArrayList<DocTerms>(
                    Objects.requireNonNull(cutRes, "cutRes")));
            this.highFreqMutexGroupTermsToIndex = Collections.unmodifiableList(
                    buildHighFreqMutexGroupTermsToIndex(this.highFreqMutexGroupPostings));
            this.highFreqSingleTermToIndex = Collections.unmodifiableList(
                    buildHighFreqSingleTermToIndex(this.highFreqSingleTermPostings));
            this.lowHitTermToIndexes = Collections.unmodifiableList(
                    buildLowHitTermToIndexes(this.lowHitForwardRows));
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
         * hotTerms 的阈值定义（严格大于该值才进入 hotTerms）。
         */
        public int getHotTermThresholdExclusive() {
            return hotTermThresholdExclusive;
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

        private static List<TermsPostingIndexRef> buildHighFreqMutexGroupTermsToIndex(
                List<SelectedGroup> groups
        ) {
            List<TermsPostingIndexRef> out = new ArrayList<TermsPostingIndexRef>(groups.size());
            for (int i = 0; i < groups.size(); i++) {
                out.add(new TermsPostingIndexRef(groups.get(i).getTerms(), i));
            }
            return out;
        }

        private static List<TermsPostingIndexRef> buildHighFreqSingleTermToIndex(
                List<HotTermDocList> hotTerms
        ) {
            List<TermsPostingIndexRef> out = new ArrayList<TermsPostingIndexRef>(hotTerms.size());
            for (int i = 0; i < hotTerms.size(); i++) {
                List<byte[]> singleTerm = new ArrayList<byte[]>(1);
                singleTerm.add(hotTerms.get(i).getTerm());
                out.add(new TermsPostingIndexRef(singleTerm, i));
            }
            return out;
        }

        private static List<TermsPostingIndexRef> buildLowHitTermToIndexes(
                List<DocTerms> lowHitForwardRows
        ) {
            List<TermsPostingIndexRef> out = new ArrayList<TermsPostingIndexRef>();
            for (int rowIndex = 0; rowIndex < lowHitForwardRows.size(); rowIndex++) {
                DocTerms row = lowHitForwardRows.get(rowIndex);
                int docId = row.getDocId();
                for (byte[] term : row.getTermsUnsafe()) {
                    List<byte[]> singleTerm = new ArrayList<byte[]>(1);
                    singleTerm.add(term);
                    out.add(new TermsPostingIndexRef(singleTerm, docId));
                }
            }
            return out;
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

        public TermsPostingIndexRef(List<byte[]> terms, int postingIndex) {
            Objects.requireNonNull(terms, "terms");
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
