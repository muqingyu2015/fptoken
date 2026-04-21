package cn.lxdb.plugins.muqingyu.fptoken.index;

import cn.lxdb.plugins.muqingyu.fptoken.model.DocTerms;
import cn.lxdb.plugins.muqingyu.fptoken.util.ByteArrayUtils;
import cn.lxdb.plugins.muqingyu.fptoken.util.ByteArrayKey;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 词典与垂直 tidset 索引：将「文档 → 词」转为「词 → 文档位图」，供频繁项集挖掘做交集。
 *
 * <p><b>构建步骤</b>（单次扫描 {@code rows}）：
 * <ol>
 *   <li>用 {@link ByteArrayKey} 作为 Map 键，为每个唯一词分配连续 {@code termId}。</li>
 *   <li>同步维护 {@code idToTerm.get(termId)} 为原始词字节（与键内数组一致）。</li>
 *   <li>为每个 termId 维护 {@link BitSet}：对文档 {@code d}，若含该词则 {@code tidset.set(d 的 docId)}。</li>
 * </ol>
 *
 * <p><b>重要约定</b>：
 * <ul>
 *   <li>位图下标必须与 {@link DocTerms#getDocId()} 一致；否则支持度与 docId 列表会错位。</li>
 *   <li>新建词的 {@link BitSet} 容量用 {@code rows.size()} 估计，仅为 hint，非硬性边界。</li>
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
     * @param rows 非 null；单条文档的词已在 {@link DocTerms} 构造时去空、去重
     * @return 索引对象；若全无有效词则 {@link #getIdToTerm()} 为空列表
     */
    public static TermTidsetIndex build(List<DocTerms> rows) {
        if (rows == null) {
            throw new IllegalArgumentException("rows must not be null");
        }
        Map<ByteArrayKey, Integer> termIdMap = new HashMap<>();
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
                ByteArrayKey key = new ByteArrayKey(rawTerm);
                Integer termId = termIdMap.get(key);
                if (termId == null) {
                    termId = Integer.valueOf(idToTerm.size());
                    termIdMap.put(key, termId);
                    idToTerm.add(key.bytes());
                    tidsetsByTermId.add(new BitSet(docCount));
                }
                tidsetsByTermId.get(termId.intValue()).set(docId);
            }
        }
        return new TermTidsetIndex(
                Collections.unmodifiableList(idToTerm),
                Collections.unmodifiableList(tidsetsByTermId));
    }

    /**
     * 在构建完成后按支持度范围做一次压缩过滤，用于控制词典/位图体积。
     *
     * @param rows 文档输入
     * @param minSupport 最小支持度（< 1 时按 1 处理）
     * @param maxDocCoverageRatio 最大文档覆盖率，取值 (0,1]；>=1 表示不过滤“过热词”
     */
    public static TermTidsetIndex buildWithSupportBounds(
            List<DocTerms> rows,
            int minSupport,
            double maxDocCoverageRatio
    ) {
        TermTidsetIndex base = build(rows);
        if (base.getIdToTermUnsafe().isEmpty()) {
            return base;
        }
        int docCount = rows.size();
        int safeMinSupport = Math.max(1, minSupport);
        double safeMaxCoverage = maxDocCoverageRatio <= 0d ? 1d : Math.min(1d, maxDocCoverageRatio);
        if (safeMinSupport <= 1 && safeMaxCoverage >= 1d) {
            return base;
        }

        int maxSupport = safeMaxCoverage >= 1d ? Integer.MAX_VALUE : (int) Math.floor(docCount * safeMaxCoverage);
        List<byte[]> idToTerm = new ArrayList<>();
        List<BitSet> tidsetsByTermId = new ArrayList<>();
        List<byte[]> baseTerms = base.getIdToTermUnsafe();
        List<BitSet> baseTidsets = base.getTidsetsByTermIdUnsafe();
        for (int i = 0; i < baseTerms.size(); i++) {
            BitSet bits = baseTidsets.get(i);
            int support = bits.cardinality();
            if (support >= safeMinSupport && support <= maxSupport) {
                idToTerm.add(baseTerms.get(i));
                tidsetsByTermId.add(bits);
            }
        }
        return new TermTidsetIndex(
                Collections.unmodifiableList(idToTerm),
                Collections.unmodifiableList(tidsetsByTermId));
    }

    /**
     * 可读性别名：语义同 {@link #build(List)}。
     *
     * @param documents 文档列表
     * @return 构建后的词-文档位图索引
     */
    public static TermTidsetIndex createFromDocuments(List<DocTerms> documents) {
        return build(documents);
    }

    /** termId → 词字节；下标即 termId。 */
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

    /** 仅供性能敏感内部路径；调用方不得修改列表及其中元素。 */
    public List<byte[]> getIdToTermUnsafe() {
        return idToTerm;
    }

    /** 仅供性能敏感内部路径；调用方不得修改列表及其中元素。 */
    public List<BitSet> getTidsetsByTermIdUnsafe() {
        return tidsetsByTermId;
    }
}
