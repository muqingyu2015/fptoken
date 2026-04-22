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
    private final List<byte[]> terms;
    private final List<Integer> docIds;
    private final int support;
    private final int estimatedSaving;

    /**
     * @param terms 词组（每条为独立 byte[]）
     * @param docIds 命中文档 id 列表
     * @param support 通常等于 {@code docIds.size()}
     * @param estimatedSaving 与 {@link CandidateItemset#getEstimatedSaving()} 一致含义
     */
    public SelectedGroup(List<byte[]> terms, List<Integer> docIds, int support, int estimatedSaving) {
        Objects.requireNonNull(terms, "terms");
        Objects.requireNonNull(docIds, "docIds");
        List<byte[]> copiedTerms = new ArrayList<>(terms.size());
        for (byte[] t : terms) {
            copiedTerms.add(ByteArrayUtils.copy(Objects.requireNonNull(t, "term")));
        }
        this.terms = Collections.unmodifiableList(copiedTerms);
        this.docIds = Collections.unmodifiableList(new ArrayList<>(docIds));
        this.support = support;
        this.estimatedSaving = estimatedSaving;
    }

    public List<byte[]> getTerms() {
        List<byte[]> out = new ArrayList<>(terms.size());
        for (byte[] t : terms) {
            out.add(ByteArrayUtils.copy(t));
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
        return "SelectedGroup{terms=" + ByteArrayUtils.formatTermsHex(terms)
                + ", support=" + support
                + ", estimatedSaving=" + estimatedSaving
                + ", docIds=" + docIds + "}";
    }
}
