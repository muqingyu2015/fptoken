package cn.lxdb.plugins.muqingyu.fptoken.runner.result;

import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.DocTerms;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.ExclusiveSelectionResult;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.SelectedGroup;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.config.EngineTuningConfig;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.util.ByteArrayUtils;
import java.util.ArrayList;
import java.util.BitSet;
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
            validateSkipHashGramRange(skipHashMinGram, skipHashMaxGram);
            this.loadedRows = Collections.unmodifiableList(new ArrayList<DocTerms>(
                    Objects.requireNonNull(loadedRows, "loadedRows")));
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
            this.skipHashMinGram = skipHashMinGram;
            this.skipHashMaxGram = skipHashMaxGram;
            this.highFreqMutexGroupSkipBitsetIndex = buildTermBlockSkipBitsetIndex(
                    this.highFreqMutexGroupTermsToIndex, skipHashMinGram, skipHashMaxGram);
            this.highFreqSingleTermSkipBitsetIndex = buildTermBlockSkipBitsetIndex(
                    this.highFreqSingleTermToIndex, skipHashMinGram, skipHashMaxGram);
            this.lowHitTermSkipBitsetIndex = buildTermBlockSkipBitsetIndex(
                    this.lowHitTermToIndexes, skipHashMinGram, skipHashMaxGram);
            this.oneByteDocidBitsetIndex = buildOneByteDocidBitsetIndex(this.loadedRows);
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

        private static TermBlockSkipBitsetIndex buildTermBlockSkipBitsetIndex(
                List<TermsPostingIndexRef> refs,
                int skipHashMinGram,
                int skipHashMaxGram
        ) {
            List<byte[]> logicalTerms = new ArrayList<byte[]>();
            List<Integer> termPostingIndexes = new ArrayList<Integer>();
            int maxPostingIndex = -1;
            int maxTermLen = 0;

            for (TermsPostingIndexRef ref : refs) {
                int postingIndex = ref.getPostingIndex();
                if (postingIndex > maxPostingIndex) {
                    maxPostingIndex = postingIndex;
                }
                for (byte[] term : ref.getTerms()) {
                    logicalTerms.add(term);
                    termPostingIndexes.add(postingIndex);
                    if (term.length > maxTermLen) {
                        maxTermLen = term.length;
                    }
                }
            }

            List<HashLevelBitsets> hashLevels = new ArrayList<HashLevelBitsets>();
            for (int gramLength = skipHashMinGram; gramLength <= skipHashMaxGram; gramLength++) {
                if (maxTermLen >= gramLength) {
                    hashLevels.add(buildHashLevelBitsets(gramLength, logicalTerms, termPostingIndexes, maxPostingIndex));
                }
            }
            return new TermBlockSkipBitsetIndex(maxPostingIndex, hashLevels);
        }

        private static HashLevelBitsets buildHashLevelBitsets(
                int gramLength,
                List<byte[]> logicalTerms,
                List<Integer> termPostingIndexes,
                int maxPostingIndex
        ) {
            List<BitSet> buckets = new ArrayList<BitSet>(256);
            int bitsetLength = Math.max(0, maxPostingIndex + 1);
            for (int i = 0; i < 256; i++) {
                buckets.add(new BitSet(bitsetLength));
            }

            for (int termId = 0; termId < logicalTerms.size(); termId++) {
                byte[] term = logicalTerms.get(termId);
                if (term.length < gramLength) {
                    continue;
                }
                int postingIndex = termPostingIndexes.get(termId);
                for (int start = 0; start <= term.length - gramLength; start++) {
                    int bucket = hashWindowToBucket(term, start, gramLength);
                    buckets.get(bucket).set(postingIndex);
                }
            }
            return new HashLevelBitsets(gramLength, buckets);
        }

        private static int hashWindowToBucket(byte[] arr, int start, int len) {
            int h = 1;
            for (int i = 0; i < len; i++) {
                h = 31 * h + (arr[start + i] & 0xFF);
            }
            return h & 0xFF;
        }

        private static void validateSkipHashGramRange(int skipHashMinGram, int skipHashMaxGram) {
            if (skipHashMinGram < 2) {
                throw new IllegalArgumentException("skipHashMinGram must be >= 2");
            }
            if (skipHashMaxGram < skipHashMinGram) {
                throw new IllegalArgumentException("skipHashMaxGram must be >= skipHashMinGram");
            }
        }

        private static OneByteDocidBitsetIndex buildOneByteDocidBitsetIndex(List<DocTerms> loadedRows) {
            int maxDocId = -1;
            for (DocTerms row : loadedRows) {
                if (row.getDocId() > maxDocId) {
                    maxDocId = row.getDocId();
                }
            }

            int bitsetLength = Math.max(0, maxDocId + 1);
            List<BitSet> buckets = new ArrayList<BitSet>(256);
            for (int i = 0; i < 256; i++) {
                buckets.add(new BitSet(bitsetLength));
            }

            for (DocTerms row : loadedRows) {
                int docId = row.getDocId();
                for (byte[] bytes : row.getTermsUnsafe()) {
                    for (int i = 0; i < bytes.length; i++) {
                        int unsignedByte = bytes[i] & 0xFF;
                        buckets.get(unsignedByte).set(docId);
                    }
                }
            }
            return new OneByteDocidBitsetIndex(maxDocId, buckets);
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
