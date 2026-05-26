package cn.lxdb.plugins.muqingyu.fptoken.dataset.common;

import java.io.IOException;
import java.util.Arrays;

import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.SparseFixedBitSet;
import org.apache.lucene.util.offheap.OffheapPoolName;

/**
 * 单 term 的 doc 列表：默认用严格递增的 {@code int[]}；若某次 {@link #addDoc(int)} 的 doc 不大于当前最后一个
 *（含重复、乱序），或条数达到上限，则立即升为 {@link SparseFixedBitSet}。
 */
public final class FPDocList {

	private static final int MAX_ARRAY_DOCS = 1024;
	private static final int INITIAL_ARRAY = 32;

	public final int maxDoc;
	public int[] orderedDocs;
	public int docCount;
	public SparseFixedBitSet docsSparse;

	public FPDocList(int maxDoc) {
		this.maxDoc = maxDoc;
		this.orderedDocs = new int[INITIAL_ARRAY];
		this.docCount = 0;
	}

	public void addDoc(int doc) {
		if (docsSparse != null) {
			docsSparse.set(doc);
			return;
		}
		if (docCount > 0 && doc <= orderedDocs[docCount - 1]) {
			promoteToSparse();
			docsSparse.set(doc);
			return;
		}
		if (docCount >= MAX_ARRAY_DOCS) {
			promoteToSparse();
			docsSparse.set(doc);
			return;
		}
		if (docCount == orderedDocs.length) {
			orderedDocs = Arrays.copyOf(orderedDocs, Math.min(orderedDocs.length << 1, MAX_ARRAY_DOCS));
		}
		orderedDocs[docCount++] = doc;
	}

	public void foreach(FpDocListEach callback) throws IOException {
		if (docsSparse != null) {
			for (int d = docsSparse.nextSetBit(0); d >= 0 && d < maxDoc; d = nextSetBitInSparse(docsSparse, maxDoc, d)) {
				callback.each_doc(d);
			}
			return;
		}
		for (int i = 0; i < docCount; i++) {
			callback.each_doc(orderedDocs[i]);
		}
	}

	public int docsize() {
		if (docsSparse != null) {
			return docsSparse.cardinality();
		}
		return docCount;
	}

	/**
	 * 并入 {@code other} 的全部 doc。双方已是 {@link SparseFixedBitSet} 时，对 {@code this} 调用
	 * {@link SparseFixedBitSet#or(org.apache.lucene.search.DocIdSetIterator)} 并传入 {@link BitSetIterator}
	 * 以走内部对另一 {@link SparseFixedBitSet} 的快速路径。
	 */
	/**
	 * 从本列表移除在 {@code other} 中出现的 doc（写段时父热词剔除可拼回子档 posting）。
	 */
	public void removeAllDocsPresentIn(FPDocList other) throws IOException {
		if (other == null || other.docsize() == 0) {
			return;
		}
		if (docsSparse != null) {
			if (other.docsSparse != null) {
				for (int d = other.docsSparse.nextSetBit(0); d >= 0 && d < maxDoc; d = nextSetBitInSparse(other.docsSparse,
						maxDoc, d)) {
					docsSparse.clear(d);
				}
			} else {
				for (int i = 0; i < other.docCount; i++) {
					docsSparse.clear(other.orderedDocs[i]);
				}
			}
			return;
		}
		if (other.docsSparse != null) {
			promoteToSparse();
			removeAllDocsPresentIn(other);
			return;
		}
		int w = 0;
		for (int i = 0; i < docCount; i++) {
			int d = orderedDocs[i];
			if (!otherContainsDoc(other, d)) {
				orderedDocs[w++] = d;
			}
		}
		docCount = w;
	}

	private static boolean otherContainsDoc(FPDocList other, int doc) {
		if (other.docsSparse != null) {
			return other.docsSparse.get(doc);
		}
		for (int i = 0; i < other.docCount; i++) {
			if (other.orderedDocs[i] == doc) {
				return true;
			}
		}
		return false;
	}

	public void addAllDocsFrom(FPDocList other) throws IOException {
		if (other.docsSparse == null && other.docCount == 0) {
			return;
		}
		if (docsSparse != null) {
			if (other.docsSparse != null) {
				final long cost = other.docsSparse.cardinality();
				docsSparse.or(new BitSetIterator(other.docsSparse, cost));
				return;
			}
			for (int i = 0; i < other.docCount; i++) {
				docsSparse.set(other.orderedDocs[i]);
			}
			return;
		}
		if (other.docsSparse != null) {
			promoteToSparse();
			final long cost = other.docsSparse.cardinality();
			docsSparse.or(new BitSetIterator(other.docsSparse, cost));
			return;
		}
		for (int i = 0; i < other.docCount; i++) {
			addDoc(other.orderedDocs[i]);
		}
	}

	/**
	 * {@link SparseFixedBitSet#nextSetBit(int)} 要求 index &lt; 位图长度；当当前 doc 为 {@code maxDoc - 1}
	 * 时不可再调用 {@code nextSetBit(maxDoc)}。
	 */
	public static int nextSetBitInSparse(SparseFixedBitSet bits, int maxDocExclusive, int currentDoc) {
		final int next = currentDoc + 1;
		if (next >= maxDocExclusive) {
			return -1;
		}
		return bits.nextSetBit(next);
	}

	private void promoteToSparse() {
		if (docsSparse != null) {
			return;
		}
		docsSparse = new SparseFixedBitSet(OffheapPoolName.fptoken, maxDoc);
		for (int i = 0; i < docCount; i++) {
			docsSparse.set(orderedDocs[i]);
		}
		orderedDocs = null;
		docCount = 0;
	}
}
