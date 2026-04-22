package cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model;

import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.util.ByteArrayUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

/**
 * 单条文档输入：文档标识 + 词序列（每条词为一条 {@code byte[]}）。
 *
 * <p>构造时会通过 {@link ByteArrayUtils#normalizeTerms(java.util.Collection)} 去掉空词，并在单文档内按出现顺序去重，
 * 保证下游索引与挖掘看到的是规范化词集合。
 *
 * @author muqingyu
 */
public final class DocTerms {
    private final int docId;
    private final List<byte[]> terms;

    /**
     * @param docId 文档 id；须与 {@link cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.index.TermTidsetIndex} 中位图下标约定一致
     * @param terms 词集合，不可为 null
     */
    public DocTerms(int docId, Collection<byte[]> terms) {
        this.docId = docId;
        this.terms = Collections.unmodifiableList(ByteArrayUtils.normalizeTerms(Objects.requireNonNull(terms, "terms")));
    }

    public int getDocId() {
        return docId;
    }

    public List<byte[]> getTerms() {
        List<byte[]> out = new ArrayList<>(terms.size());
        for (byte[] t : terms) {
            out.add(ByteArrayUtils.copy(t));
        }
        return out;
    }

    /** 仅供性能敏感内部调用；调用方不得修改列表元素。 */
    public List<byte[]> getTermsUnsafe() {
        return terms;
    }
}
