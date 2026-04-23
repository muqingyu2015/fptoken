package cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.index;

import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.DocTerms;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.ByteRef;
import java.util.BitSet;
import java.util.List;

/**
 * {@link TermTidsetIndex} 的可读性命名包装：强调“词 -> 文档集合”的语义。
 */
public final class TermDocumentIndex {
    private final TermTidsetIndex delegate;

    private TermDocumentIndex(TermTidsetIndex delegate) {
        this.delegate = delegate;
    }

    /** 从文档列表构建索引。 */
    public static TermDocumentIndex createFromDocuments(List<DocTerms> documents) {
        return new TermDocumentIndex(TermTidsetIndex.build(documents));
    }

    /** termId -> 原始词字节。 */
    public List<byte[]> getIdToTerm() {
        return delegate.getIdToTerm();
    }

    /** termId -> 文档位图。 */
    public List<BitSet> getDocumentBitsetsByTermId() {
        return delegate.getTidsetsByTermId();
    }

    /** 仅供性能敏感路径使用。 */
    public List<byte[]> getIdToTermUnsafe() {
        return delegate.getIdToTermUnsafe();
    }

    /** termId -> 词引用（ByteRef）。 */
    public List<ByteRef> getIdToTermRefs() {
        return delegate.getIdToTermRefs();
    }

    /** 仅供性能敏感路径使用。 */
    public List<ByteRef> getIdToTermRefsUnsafe() {
        return delegate.getIdToTermRefsUnsafe();
    }

    /** 仅供性能敏感路径使用。 */
    public List<BitSet> getDocumentBitsetsByTermIdUnsafe() {
        return delegate.getTidsetsByTermIdUnsafe();
    }
}
