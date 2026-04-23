package cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model;

import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.util.ByteArrayUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * 对外输出的一条互斥词组：词字节列表、命中文档 id、支持度与启发式节省值。
 *
 * <p>{@link #getTerms()} 一般为门面层拷贝后的字节数组；{@link #getDocIds()} 由项集 tidset 按升序展开。
 *
 * @author muqingyu
 */
public final class SelectedGroup {
    private final List<ByteRef> terms;
    private final List<Integer> docIds;
    private final int support;
    private final int estimatedSaving;

    /**
     * @param terms 词组（每条为独立 byte[]）
     * @param docIds 命中文档 id 列表
     * @param support 通常等于 {@code docIds.size()}
     * @param estimatedSaving 与 {@link CandidateItemset#getEstimatedSaving()} 一致含义
     */
    public SelectedGroup(List<?> terms, List<Integer> docIds, int support, int estimatedSaving) {
        Objects.requireNonNull(terms, "terms");
        Objects.requireNonNull(docIds, "docIds");
        List<ByteRef> copiedTerms = new ArrayList<>(terms.size());
        for (Object t : terms) {
            if (t instanceof ByteRef) {
                ByteRef ref = (ByteRef) Objects.requireNonNull(t, "term");
                copiedTerms.add(new ByteRef(ref.getSourceUnsafe(), ref.getOffset(), ref.getLength()));
            } else if (t instanceof byte[]) {
                copiedTerms.add(ByteRef.wrap(ByteArrayUtils.copy((byte[]) t)));
            } else if (t != null) {
                throw new IllegalArgumentException("term must be ByteRef or byte[]");
            }
        }
        this.terms = Collections.unmodifiableList(copiedTerms);
        this.docIds = Collections.unmodifiableList(new ArrayList<>(docIds));
        this.support = support;
        this.estimatedSaving = estimatedSaving;
    }

    public List<ByteRef> getTermRefs() {
        return new ArrayList<>(terms);
    }

    /** 兼容入口：返回 byte[] 副本。 */
    public List<byte[]> getTerms() {
        List<byte[]> out = new ArrayList<>(terms.size());
        for (ByteRef ref : terms) {
            out.add(ref.copyBytes());
        }
        return out;
    }

    public List<Integer> getDocIds() {
        return new ArrayList<>(docIds);
    }

    public int getSupport() {
        return support;
    }

    public int getEstimatedSaving() {
        return estimatedSaving;
    }

    @Override
    public String toString() {
        return "SelectedGroup{terms=" + ByteArrayUtils.formatTermRefsHex(terms)
                + ", support=" + support
                + ", estimatedSaving=" + estimatedSaving
                + ", docIds=" + docIds + "}";
    }
}
