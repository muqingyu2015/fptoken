package cn.lxdb.plugins.muqingyu.fptoken.model;

import cn.lxdb.plugins.muqingyu.fptoken.util.ByteArrayUtils;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

/**
 * 作者：muqingyu
 *
 * 单条输入：docId + 词列表（byte[]）。
 *
 * 说明：
 * - 构造时会自动做词去重和空词过滤。
 * - 保证下游算法拿到的是“规范化后的文档词集合”。
 */
public final class DocTerms {
    private final int docId;
    private final List<byte[]> terms;

    public DocTerms(int docId, Collection<byte[]> terms) {
        this.docId = docId;
        this.terms = ByteArrayUtils.normalizeTerms(Objects.requireNonNull(terms, "terms"));
    }

    public int getDocId() {
        return docId;
    }

    public List<byte[]> getTerms() {
        return terms;
    }
}
