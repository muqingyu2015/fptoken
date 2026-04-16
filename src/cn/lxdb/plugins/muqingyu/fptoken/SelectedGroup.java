package cn.lxdb.plugins.muqingyu.fptoken;

import java.util.List;

/**
 * 作者：muqingyu
 *
 * 输出结构：词组 + 对应 doclist + 统计信息。
 *
 * 字段解释：
 * - terms: 被选中的互斥词组。
 * - docIds: 该词组对应的命中文档列表（即项集 tidset 转换结果）。
 * - support: docIds 的数量。
 * - estimatedSaving: 估算节省值（用于评估该组的收益）。
 */
public final class SelectedGroup {
    private final List<byte[]> terms;
    private final List<Integer> docIds;
    private final int support;
    private final int estimatedSaving;

    public SelectedGroup(List<byte[]> terms, List<Integer> docIds, int support, int estimatedSaving) {
        this.terms = terms;
        this.docIds = docIds;
        this.support = support;
        this.estimatedSaving = estimatedSaving;
    }

    public List<byte[]> getTerms() {
        return terms;
    }

    public List<Integer> getDocIds() {
        return docIds;
    }

    public int getSupport() {
        return support;
    }

    public int getEstimatedSaving() {
        return estimatedSaving;
    }

    @Override
    public String toString() {
        return "SelectedGroup{terms=" + ByteArrayUtils.formatTermsHex(terms)
                + ", support=" + support
                + ", estimatedSaving=" + estimatedSaving
                + ", docIds=" + docIds + "}";
    }
}
