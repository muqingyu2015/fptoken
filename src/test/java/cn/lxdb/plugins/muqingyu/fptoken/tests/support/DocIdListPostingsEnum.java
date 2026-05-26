package cn.lxdb.plugins.muqingyu.fptoken.tests.support;

import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.util.BytesRef;

/** 测试用：固定 doc 列表的 {@link PostingsEnum}。 */
public final class DocIdListPostingsEnum extends PostingsEnum {

	private final int[] docs;
	private int upto = -1;

	public DocIdListPostingsEnum(int[] docs) {
		this.docs = docs == null ? new int[0] : docs;
	}

	@Override
	public int freq() {
		return 1;
	}

	@Override
	public int nextPosition() {
		return -1;
	}

	@Override
	public int startOffset() {
		return -1;
	}

	@Override
	public int endOffset() {
		return -1;
	}

	@Override
	public BytesRef getPayload() {
		return null;
	}

	@Override
	public int docID() {
		return upto < 0 ? -1 : docs[upto];
	}

	@Override
	public int nextDoc() {
		upto++;
		return upto >= docs.length ? NO_MORE_DOCS : docs[upto];
	}

	@Override
	public int advance(int target) {
		while (upto + 1 < docs.length && docs[upto + 1] < target) {
			upto++;
		}
		return nextDoc();
	}

	@Override
	public long cost() {
		return docs.length;
	}
}
