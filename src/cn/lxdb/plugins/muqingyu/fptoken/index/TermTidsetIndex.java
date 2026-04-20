package cn.lxdb.plugins.muqingyu.fptoken.index;

import cn.lxdb.plugins.muqingyu.fptoken.model.DocTerms;
import cn.lxdb.plugins.muqingyu.fptoken.util.ByteArrayKey;
import java.util.ArrayList;
import java.util.BitSet;
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
        Map<ByteArrayKey, Integer> termIdMap = new HashMap<>();
        List<byte[]> idToTerm = new ArrayList<>();
        List<BitSet> tidsetsByTermId = new ArrayList<>();
        int docCount = rows.size();

        for (DocTerms row : rows) {
            int docId = row.getDocId();
            for (byte[] rawTerm : row.getTerms()) {
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
        return new TermTidsetIndex(idToTerm, tidsetsByTermId);
    }

    /** termId → 词字节；下标即 termId。 */
    public List<byte[]> getIdToTerm() {
        return idToTerm;
    }

    /** termId → 出现该词的文档位图。 */
    public List<BitSet> getTidsetsByTermId() {
        return tidsetsByTermId;
    }
}
