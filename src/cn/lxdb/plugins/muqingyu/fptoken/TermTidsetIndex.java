package cn.lxdb.plugins.muqingyu.fptoken;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 词典与倒排位图索引。
 *
 * 原理说明：
 * - 给每个唯一词分配 termId。
 * - 同时构建 termId 对应的 BitSet（tidset），位为 1 表示该 doc 含该词。
 * - 该结构是后续频繁项集挖掘的基础（交集即可得联合项支持度）。
 */
public final class TermTidsetIndex {
    private final List<byte[]> idToTerm;
    private final List<BitSet> tidsetsByTermId;

    private TermTidsetIndex(List<byte[]> idToTerm, List<BitSet> tidsetsByTermId) {
        this.idToTerm = idToTerm;
        this.tidsetsByTermId = tidsetsByTermId;
    }

    public static TermTidsetIndex build(List<DocTerms> rows, Map<Integer, Integer> docIdToIndex) {
        Map<ByteArrayKey, Integer> termIdMap = new HashMap<ByteArrayKey, Integer>();
        List<byte[]> idToTerm = new ArrayList<byte[]>();
        List<BitSet> tidsetsByTermId = new ArrayList<BitSet>();
        int docCount = rows.size();

        for (DocTerms row : rows) {
            Integer docIndex = docIdToIndex.get(row.getDocId());
            if (docIndex == null) {
                continue;
            }
            for (byte[] rawTerm : row.getTerms()) {
                if (rawTerm == null || rawTerm.length == 0) {
                    continue;
                }
                ByteArrayKey key = new ByteArrayKey(rawTerm);
                Integer termId = termIdMap.get(key);
                if (termId == null) {
                    // 第一次出现：创建新 termId 和对应的空 tidset。
                    termId = Integer.valueOf(idToTerm.size());
                    termIdMap.put(key, termId);
                    idToTerm.add(key.bytes());
                    tidsetsByTermId.add(new BitSet(docCount));
                }
                // 标记该词命中当前文档。
                tidsetsByTermId.get(termId.intValue()).set(docIndex.intValue());
            }
        }
        return new TermTidsetIndex(idToTerm, tidsetsByTermId);
    }

    public List<byte[]> getIdToTerm() {
        return idToTerm;
    }

    public List<BitSet> getTidsetsByTermId() {
        return tidsetsByTermId;
    }
}
