package cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.index;

import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.DocTerms;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.ByteRef;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.protocol.ModuleDataContracts;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.util.ByteArrayUtils;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.util.OpenHashTable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;

/**
 * 词典与垂直 tidset 索引:将「文档 → 词」转为「词 → 文档位图」,供频繁项集挖掘做交集。
 *
 * <p><b>构建步骤</b>(单次扫描 {@code rows}):
 * <ol>
 *   <li>用 {@link ByteArrayKey} 作为 Map 键,为每个唯一词分配连续 {@code termId}。</li>
 *   <li>同步维护 {@code idToTerm.get(termId)} 为原始词字节(与键内数组一致)。</li>
 *   <li>为每个 termId 维护 {@link BitSet}:对文档 {@code d},若含该词则 {@code tidset.set(d 的 docId)}。</li>
 * </ol>
 *
 * <p><b>重要约定</b>:
 * <ul>
 *   <li>位图下标必须与 {@link DocTerms#getDocId()} 一致;否则支持度与 docId 列表会错位。</li>
 *   <li>新建词的 {@link BitSet} 容量用 {@code rows.size()} 估计,仅为 hint,非硬性边界。</li>
 * </ul>
 *
 * @author muqingyu
 */
public final class TermTidsetIndex {
    private final List<byte[]> idToTerm;
    private final List<ByteRef> idToTermRefs;
    private final List<BitSet> tidsetsByTermId;

    private TermTidsetIndex(List<byte[]> idToTerm, List<BitSet> tidsetsByTermId) {
        this.idToTerm = idToTerm;
        List<ByteRef> refs = new ArrayList<>(idToTerm.size());
        for (byte[] term : idToTerm) {
            refs.add(ByteRef.wrap(term));
        }
        this.idToTermRefs = Collections.unmodifiableList(refs);
        this.tidsetsByTermId = tidsetsByTermId;
    }

    /**
     * 从规范化后的 {@link DocTerms} 列表构建索引。
     *
     * @param rows 非 null;单条文档的词已在 {@link DocTerms} 构造时去空、去重
     * @return 索引对象;若全无有效词则 {@link #getIdToTerm()} 为空列表
     */
    public static TermTidsetIndex build(List<DocTerms> rows) {
        if (rows == null) {
            throw new IllegalArgumentException("rows must not be null");
        }
        // 使用自定义开放寻址哈希表替代 HashMap，减少 ByteArrayKey 装箱和 equals 开销
        // 预计可提升 build() 性能 10-15%
        OpenHashTable termIdMap = new OpenHashTable();
        List<byte[]> idToTerm = new ArrayList<>();
        List<BitSet> tidsetsByTermId = new ArrayList<>();

        for (int rowIndex = 0; rowIndex < rows.size(); rowIndex++) {
            DocTerms row = requireValidRow(rows, rowIndex);
            int docId = row.getDocId();
            for (ByteRef rawTermRef : row.getTermRefsUnsafe()) {
                if (rawTermRef == null || rawTermRef.getLength() == 0) {
                    continue;
                }
                int hash = ByteArrayUtils.hash(
                        rawTermRef.getSourceUnsafe(),
                        rawTermRef.getOffset(),
                        rawTermRef.getLength());
                int termId = termIdMap.getOrPut(rawTermRef, hash, true);
                if (termId >= idToTerm.size()) {
                    // 新词：直接从 OpenHashTable 获取已存储的 byte[]，避免重复拷贝
                    idToTerm.add(termIdMap.getKeyBytes(termId));
                    tidsetsByTermId.add(new BitSet());
                }
                tidsetsByTermId.get(termId).set(docId);
            }
        }
        return new TermTidsetIndex(
                Collections.unmodifiableList(idToTerm),
                Collections.unmodifiableList(tidsetsByTermId));
    }

    /**
     * 支持度边界过滤索引构建：两次扫描（统计频率 + 设位图）。
     *
     * <p>相比 {@link #build(List)} + 事后过滤，此方法在大词表场景下显著降低内存峰值，
     * 因为低频词不会占用完整的 BitSet(docCount) 空间。
     *
     * @param rows 文档输入
     * @param minSupport 最小支持度（< 1 时按 1 处理）
     * @param maxDocCoverageRatio 最大文档覆盖率，取值 (0,1]；>=1 表示不过滤"过热词"
     */
    public static TermTidsetIndex buildWithSupportBounds(
            List<DocTerms> rows,
            int minSupport,
            double maxDocCoverageRatio
    ) {
        return buildWithSupportBounds(rows, minSupport, maxDocCoverageRatio, null);
    }

    /**
     * 与 {@link #buildWithSupportBounds(List, int, double)} 语义一致，
     * 但会把关键子步骤耗时写入 {@code timings}（若非 null）。
     */
    public static TermTidsetIndex buildWithSupportBounds(
            List<DocTerms> rows,
            int minSupport,
            double maxDocCoverageRatio,
            BuildWithSupportBoundsTimings timings
    ) {
        if (rows == null) {
            throw new IllegalArgumentException("rows must not be null");
        }
        long t0 = System.nanoTime();
        int docCount = rows.size();
        int safeMinSupport = Math.max(1, minSupport);
        double safeMaxCoverage = maxDocCoverageRatio <= 0d ? 1d : Math.min(1d, maxDocCoverageRatio);
        int maxSupport = safeMaxCoverage >= 1d ? Integer.MAX_VALUE : (int) Math.floor(docCount * safeMaxCoverage);

        FrequencyIndexStage frequencyStage = buildFrequencyIndexStage(rows, timings);
        if (frequencyStage.termTypeCount == 0) {
            if (timings != null) {
                timings.filterPrepareMillis = 0L;
                timings.secondPassMillis = 0L;
                timings.totalMillis = nanosToMillis(System.nanoTime() - t0);
            }
            return new TermTidsetIndex(
                    Collections.<byte[]>emptyList(),
                    Collections.<BitSet>emptyList());
        }

        SupportFilteredStage filteredStage = buildSupportFilteredStage(
                frequencyStage,
                safeMinSupport,
                maxSupport,
                timings);
        if (filteredStage.idToTerm.isEmpty()) {
            if (timings != null) {
                timings.secondPassMillis = 0L;
                timings.totalMillis = nanosToMillis(System.nanoTime() - t0);
            }
            return new TermTidsetIndex(
                    Collections.<byte[]>emptyList(),
                    Collections.<BitSet>emptyList());
        }

        BitsetStageResult bitsetStage = buildBitsetStage(
                frequencyStage,
                filteredStage,
                timings);
        if (timings != null) {
            timings.totalMillis = nanosToMillis(System.nanoTime() - t0);
        }

        return new TermTidsetIndex(
                Collections.unmodifiableList(bitsetStage.idToTerm),
                Collections.unmodifiableList(bitsetStage.tidsetsByTermId));
    }

    public static final class BuildWithSupportBoundsTimings {
        private long firstPassMillis;
        private long filterPrepareMillis;
        private long secondPassMillis;
        private long totalMillis;

        public long getFirstPassMillis() {
            return firstPassMillis;
        }

        public long getFilterPrepareMillis() {
            return filterPrepareMillis;
        }

        public long getSecondPassMillis() {
            return secondPassMillis;
        }

        public long getTotalMillis() {
            return totalMillis;
        }
    }

    private static long nanosToMillis(long nanos) {
        return nanos / 1_000_000L;
    }

    private static DocTerms requireValidRow(List<DocTerms> rows, int rowIndex) {
        DocTerms row = rows.get(rowIndex);
        if (row == null) {
            throw new IllegalArgumentException("rows[" + rowIndex + "] must not be null");
        }
        int docId = row.getDocId();
        if (docId < 0) {
            throw new IllegalArgumentException("docId must be >= 0, got " + docId);
        }
        return row;
    }

    /**
     * 阶段 1：基于输入 rows 构建词频索引（termId、support、按行 termId 缓存、按行 docId 缓存）。
     */
    private static FrequencyIndexStage buildFrequencyIndexStage(
            List<DocTerms> rows,
            BuildWithSupportBoundsTimings timings
    ) {
        long firstPassStart = System.nanoTime();
        OpenHashTable freqTable = new OpenHashTable();
        int[] supports = new int[16];
        int termTypeCount = 0;
        List<int[]> rowTermIds = new ArrayList<int[]>(rows.size());
        int[] docIdsByRow = new int[rows.size()];
        for (int rowIndex = 0; rowIndex < rows.size(); rowIndex++) {
            DocTerms row = requireValidRow(rows, rowIndex);
            docIdsByRow[rowIndex] = row.getDocId();
            List<ByteRef> rowTerms = row.getTermRefsUnsafe();
            int[] ids = new int[rowTerms.size()];
            int used = 0;
            for (ByteRef rawTermRef : rowTerms) {
                if (rawTermRef == null || rawTermRef.getLength() == 0) {
                    continue;
                }
                int hash = ByteArrayUtils.hash(
                        rawTermRef.getSourceUnsafe(),
                        rawTermRef.getOffset(),
                        rawTermRef.getLength());
                int termId = freqTable.getOrPut(rawTermRef, hash, true);
                if (termId >= supports.length) {
                    supports = growSupportsArray(supports, termId);
                }
                supports[termId]++;
                ids[used++] = termId;
                if (termId + 1 > termTypeCount) {
                    termTypeCount = termId + 1;
                }
            }
            if (used == ids.length) {
                rowTermIds.add(ids);
            } else {
                int[] compact = new int[used];
                System.arraycopy(ids, 0, compact, 0, used);
                rowTermIds.add(compact);
            }
        }
        if (timings != null) {
            timings.firstPassMillis = nanosToMillis(System.nanoTime() - firstPassStart);
        }
        return new FrequencyIndexStage(freqTable, supports, termTypeCount, rowTermIds, docIdsByRow);
    }

    private static int[] growSupportsArray(int[] current, int targetTermId) {
        int newCap = current.length;
        while (newCap <= targetTermId) {
            newCap = Math.max(16, newCap * 2);
        }
        int[] grown = new int[newCap];
        System.arraycopy(current, 0, grown, 0, current.length);
        return grown;
    }

    /**
     * 阶段 2：按支持度边界筛选 termId，并建立 originTermId -> filteredTermId 映射。
     */
    private static SupportFilteredStage buildSupportFilteredStage(
            FrequencyIndexStage scan,
            int safeMinSupport,
            int maxSupport,
            BuildWithSupportBoundsTimings timings
    ) {
        long filterPrepareStart = System.nanoTime();
        List<byte[]> idToTerm = new ArrayList<>();
        List<BitSet> tidsetsByTermId = new ArrayList<>();
        int[] originTermIdToFiltered = new int[scan.termTypeCount];
        Arrays.fill(originTermIdToFiltered, -1);

        long[] supportAndTermIds = buildSupportAndTermIdOrder(scan.supports, scan.termTypeCount);

        for (int i = 0; i < supportAndTermIds.length; i++) {
            int termId = (int) supportAndTermIds[i];
            int support = scan.supports[termId];
            if (support >= safeMinSupport && support <= maxSupport) {
                int filteredTermId = idToTerm.size();
                originTermIdToFiltered[termId] = filteredTermId;
                idToTerm.add(scan.freqTable.getKeyBytes(termId));
                tidsetsByTermId.add(new BitSet());
            }
        }
        if (timings != null) {
            timings.filterPrepareMillis = nanosToMillis(System.nanoTime() - filterPrepareStart);
        }
        return new SupportFilteredStage(idToTerm, tidsetsByTermId, originTermIdToFiltered);
    }

    private static long[] buildSupportAndTermIdOrder(int[] supports, int termTypeCount) {
        long[] ordered = new long[termTypeCount];
        for (int termId = 0; termId < termTypeCount; termId++) {
            long supportBits = ((long) supports[termId]) << 32;
            long termBits = termId & 0xffffffffL;
            ordered[termId] = supportBits | termBits;
        }
        Arrays.sort(ordered);
        return ordered;
    }

    /**
     * 阶段 3：将筛选后的 term 映射转换为最终位图索引（term -> docId bitset）。
     */
    private static BitsetStageResult buildBitsetStage(
            FrequencyIndexStage frequencyStage,
            SupportFilteredStage filteredStage,
            BuildWithSupportBoundsTimings timings
    ) {
        long secondPassStart = System.nanoTime();
        int[] docIdsByRow = frequencyStage.docIdsByRow;
        List<int[]> rowTermIds = frequencyStage.rowTermIds;
        int[] originTermIdToFiltered = filteredStage.originTermIdToFiltered;
        BitSet[] filteredTidsets = filteredStage.tidsetsByTermId.toArray(
                new BitSet[filteredStage.tidsetsByTermId.size()]);
        for (int rowIndex = 0; rowIndex < rowTermIds.size(); rowIndex++) {
            int docId = docIdsByRow[rowIndex];
            int[] termIdsInRow = rowTermIds.get(rowIndex);
            for (int i = 0; i < termIdsInRow.length; i++) {
                int originTermId = termIdsInRow[i];
                int filteredTermId = originTermIdToFiltered[originTermId];
                if (filteredTermId >= 0) {
                    filteredTidsets[filteredTermId].set(docId);
                }
            }
        }
        if (timings != null) {
            timings.secondPassMillis = nanosToMillis(System.nanoTime() - secondPassStart);
        }
        return new BitsetStageResult(filteredStage.idToTerm, filteredStage.tidsetsByTermId);
    }

    private static final class FrequencyIndexStage {
        private final OpenHashTable freqTable;
        private final int[] supports;
        private final int termTypeCount;
        private final List<int[]> rowTermIds;
        private final int[] docIdsByRow;

        private FrequencyIndexStage(
                OpenHashTable freqTable,
                int[] supports,
                int termTypeCount,
                List<int[]> rowTermIds,
                int[] docIdsByRow
        ) {
            this.freqTable = freqTable;
            this.supports = supports;
            this.termTypeCount = termTypeCount;
            this.rowTermIds = rowTermIds;
            this.docIdsByRow = docIdsByRow;
        }
    }

    private static final class SupportFilteredStage {
        private final List<byte[]> idToTerm;
        private final List<BitSet> tidsetsByTermId;
        private final int[] originTermIdToFiltered;

        private SupportFilteredStage(
                List<byte[]> idToTerm,
                List<BitSet> tidsetsByTermId,
                int[] originTermIdToFiltered
        ) {
            this.idToTerm = idToTerm;
            this.tidsetsByTermId = tidsetsByTermId;
            this.originTermIdToFiltered = originTermIdToFiltered;
        }
    }

    private static final class BitsetStageResult {
        private final List<byte[]> idToTerm;
        private final List<BitSet> tidsetsByTermId;

        private BitsetStageResult(List<byte[]> idToTerm, List<BitSet> tidsetsByTermId) {
            this.idToTerm = idToTerm;
            this.tidsetsByTermId = tidsetsByTermId;
        }
    }

    /**
     * 可读性别名:语义同 {@link #build(List)}。
     *
     * @param documents 文档列表
     * @return 构建后的词-文档位图索引
     */
    public static TermTidsetIndex createFromDocuments(List<DocTerms> documents) {
        return build(documents);
    }

    /** termId → 词字节;下标即 termId。 */
    public List<byte[]> getIdToTerm() {
        List<byte[]> out = new ArrayList<>(idToTerm.size());
        for (byte[] term : idToTerm) {
            out.add(ByteArrayUtils.copy(term));
        }
        return out;
    }

    /** termId → 出现该词的文档位图。 */
    public List<BitSet> getTidsetsByTermId() {
        List<BitSet> out = new ArrayList<>(tidsetsByTermId.size());
        for (BitSet tidset : tidsetsByTermId) {
            out.add((BitSet) tidset.clone());
        }
        return out;
    }

    /** termId → 词引用视图（每个 term 为完整数组引用）。 */
    public List<ByteRef> getIdToTermRefs() {
        return new ArrayList<>(idToTermRefs);
    }

    /** 仅供性能敏感内部路径;调用方不得修改列表及其中元素。 */
    public List<byte[]> getIdToTermUnsafe() {
        return idToTerm;
    }

    /** 仅供性能敏感内部路径;调用方不得修改列表及其中元素。 */
    public List<ByteRef> getIdToTermRefsUnsafe() {
        return idToTermRefs;
    }

    /** 仅供性能敏感内部路径;调用方不得修改列表及其中元素。 */
    public List<BitSet> getTidsetsByTermIdUnsafe() {
        return tidsetsByTermId;
    }

    /**
     * index -> miner 协议输出：只暴露 termId 对齐的 tidset 视图。
     *
     * <p>调用方可将该对象直接传给 miner，避免跨模块传递额外上下文导致耦合。</p>
     */
    public ModuleDataContracts.TidsetMiningInput toMiningInputProtocol() {
        return new ModuleDataContracts.TidsetMiningInput(tidsetsByTermId);
    }
}
