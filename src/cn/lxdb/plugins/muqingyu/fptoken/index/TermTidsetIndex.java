package cn.lxdb.plugins.muqingyu.fptoken.index;

import cn.lxdb.plugins.muqingyu.fptoken.model.DocTerms;
import cn.lxdb.plugins.muqingyu.fptoken.util.ByteArrayUtils;
import cn.lxdb.plugins.muqingyu.fptoken.util.ByteArrayKey;
import cn.lxdb.plugins.muqingyu.fptoken.util.OpenHashTable;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
    private final List<BitSet> tidsetsByTermId;

    private TermTidsetIndex(List<byte[]> idToTerm, List<BitSet> tidsetsByTermId) {
        this.idToTerm = idToTerm;
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
        int docCount = rows.size();

        for (int rowIndex = 0; rowIndex < rows.size(); rowIndex++) {
            DocTerms row = rows.get(rowIndex);
            if (row == null) {
                throw new IllegalArgumentException("rows[" + rowIndex + "] must not be null");
            }
            int docId = row.getDocId();
            if (docId < 0) {
                throw new IllegalArgumentException("docId must be >= 0, got " + docId);
            }
            for (byte[] rawTerm : row.getTermsUnsafe()) {
                if (rawTerm == null || rawTerm.length == 0) {
                    continue;
                }
                int hash = ByteArrayUtils.hash(rawTerm);
                int termId = termIdMap.getOrPut(rawTerm, hash, true);
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
        if (rows == null) {
            throw new IllegalArgumentException("rows must not be null");
        }
        int docCount = rows.size();
        int safeMinSupport = Math.max(1, minSupport);
        double safeMaxCoverage = maxDocCoverageRatio <= 0d ? 1d : Math.min(1d, maxDocCoverageRatio);
        int maxSupport = safeMaxCoverage >= 1d ? Integer.MAX_VALUE : (int) Math.floor(docCount * safeMaxCoverage);

        if (safeMinSupport <= 1 && maxSupport >= docCount) {
            // 没有过滤条件，退化为原始的 build()（单次扫描，立即分配 BitSet）
            return build(rows);
        }

        // === 第一次扫描：统计频率 ===
        // 使用自定义哈希表统计频率（只计数，不分配 BitSet）
        // 使用 int[2] 同时存储 hash + count，避免额外映射
        // 这里复用 OpenHashTable 的逻辑，但 value 不是 termId，而是 count
        // 简单起见，先统计频率再过滤
        Map<ByteArrayKey, int[]> freqMap = new HashMap<>();
        // 只对首次出现频率 >= safeMinSupport 的词记录 docId，减少内存占用
        // 注意：由于不知道最终哪些词会通过过滤，先全部统计频率
        for (int rowIndex = 0; rowIndex < rows.size(); rowIndex++) {
            DocTerms row = rows.get(rowIndex);
            if (row == null) {
                throw new IllegalArgumentException("rows[" + rowIndex + "] must not be null");
            }
            int docId = row.getDocId();
            if (docId < 0) {
                throw new IllegalArgumentException("docId must be >= 0, got " + docId);
            }
            for (byte[] rawTerm : row.getTermsUnsafe()) {
                if (rawTerm == null || rawTerm.length == 0) {
                    continue;
                }
                ByteArrayKey key = new ByteArrayKey(rawTerm);
                int[] count = freqMap.get(key);
                if (count == null) {
                    count = new int[]{1};
                    freqMap.put(key, count);
                } else {
                    count[0]++;
                }
            }
        }

        if (freqMap.isEmpty()) {
            return new TermTidsetIndex(
                    Collections.<byte[]>emptyList(),
                    Collections.<BitSet>emptyList());
        }

        // === 按频率过滤 + 分配 BitSet + 第二次扫描设位图 ===
        List<byte[]> idToTerm = new ArrayList<>();
        List<BitSet> tidsetsByTermId = new ArrayList<>();
        Map<ByteArrayKey, Integer> termIdMap = new HashMap<>();

        for (Map.Entry<ByteArrayKey, int[]> entry : freqMap.entrySet()) {
            int support = entry.getValue()[0];
            if (support >= safeMinSupport && support <= maxSupport) {
                ByteArrayKey key = entry.getKey();
                int termId = idToTerm.size();
                termIdMap.put(key, Integer.valueOf(termId));
                idToTerm.add(key.bytes());
                tidsetsByTermId.add(new BitSet());
            }
        }
        freqMap.clear();

        if (idToTerm.isEmpty()) {
            return new TermTidsetIndex(
                    Collections.<byte[]>emptyList(),
                    Collections.<BitSet>emptyList());
        }

        // 第二次扫描：使用 forLookup 设位图
        for (int rowIndex = 0; rowIndex < rows.size(); rowIndex++) {
            DocTerms row = rows.get(rowIndex);
            int docId = row.getDocId();
            for (byte[] rawTerm : row.getTermsUnsafe()) {
                if (rawTerm == null || rawTerm.length == 0) {
                    continue;
                }
                int hash = ByteArrayUtils.hash(rawTerm);
                ByteArrayKey lookupKey = ByteArrayKey.forLookup(rawTerm, hash);
                Integer termId = termIdMap.get(lookupKey);
                if (termId != null) {
                    tidsetsByTermId.get(termId.intValue()).set(docId);
                }
            }
        }

        return new TermTidsetIndex(
                Collections.unmodifiableList(idToTerm),
                Collections.unmodifiableList(tidsetsByTermId));
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

    /** 仅供性能敏感内部路径;调用方不得修改列表及其中元素。 */
    public List<byte[]> getIdToTermUnsafe() {
        return idToTerm;
    }

    /** 仅供性能敏感内部路径;调用方不得修改列表及其中元素。 */
    public List<BitSet> getTidsetsByTermIdUnsafe() {
        return tidsetsByTermId;
    }
}
