package cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model;

import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.util.ByteArrayUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

/**
 * 单条文档输入：文档标识 + 词序列（每条词为一条 {@link ByteRef}）。
 *
 * <p>构造时会去掉空词，并在单文档内按出现顺序做内容去重，
 * 保证下游索引与挖掘看到的是规范化词集合。
 *
 * @author muqingyu
 */
public final class DocTerms {
    private final int docId;
    private final List<ByteRef> termRefs;

    /**
     * @param docId 文档 id；须与 {@link cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.index.TermTidsetIndex} 中位图下标约定一致
     * @param terms 词集合，不可为 null
     */
    public DocTerms(int docId, Collection<?> terms) {
        this.docId = docId;
        List<ByteRef> inputRefs = new ArrayList<>();
        for (Object term : Objects.requireNonNull(terms, "terms")) {
            if (term instanceof ByteRef) {
                ByteRef ref = (ByteRef) term;
                inputRefs.add(new ByteRef(ref.getSourceUnsafe(), ref.getOffset(), ref.getLength()));
            } else if (term instanceof byte[]) {
                inputRefs.add(ByteRef.wrap(ByteArrayUtils.copy((byte[]) term)));
            } else if (term != null) {
                throw new IllegalArgumentException("term must be ByteRef or byte[]");
            }
        }
        List<ByteRef> normalized = ByteArrayUtils.normalizeTermRefs(inputRefs);
        List<ByteRef> refs = new ArrayList<>(normalized.size());
        for (ByteRef term : normalized) {
            refs.add(new ByteRef(term.getSourceUnsafe(), term.getOffset(), term.getLength()));
        }
        this.termRefs = Collections.unmodifiableList(refs);
    }

    /** 兼容入口：显式 ByteRef 构造。 */
    public static DocTerms fromByteRefs(int docId, Collection<ByteRef> termRefs) {
        return new DocTerms(docId, termRefs);
    }

    public int getDocId() {
        return docId;
    }

    public List<ByteRef> getTermRefs() {
        return new ArrayList<>(termRefs);
    }

    /** 仅供性能敏感内部调用；调用方不得修改返回列表。 */
    public List<ByteRef> getTermRefsUnsafe() {
        return termRefs;
    }

    /** 兼容入口：返回 byte[] 副本视图。 */
    public List<byte[]> getTerms() {
        List<byte[]> out = new ArrayList<>(termRefs.size());
        for (ByteRef ref : termRefs) {
            out.add(ref.copyBytes());
        }
        return out;
    }

    /** 兼容入口：返回 byte[] 视图（元素为复制后的独立数组）。 */
    public List<byte[]> getTermsUnsafe() {
        return getTerms();
    }
}
