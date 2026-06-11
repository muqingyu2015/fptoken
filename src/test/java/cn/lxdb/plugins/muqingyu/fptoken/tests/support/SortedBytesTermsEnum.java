package cn.lxdb.plugins.muqingyu.fptoken.tests.support;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.lucene.index.ImpactsEnum;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.TermState;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.index.TermsEnum$SeekStatus;
import org.apache.lucene.util.BytesRef;

/** 按字节序排序的内存词项表，供 {@link InMemoryFpTerms} 查询。 */
public final class SortedBytesTermsEnum extends TermsEnum {

	private final TreeMap<byte[], int[]> postingsByTerm;
	private final byte[][] sortedKeys;
	private int ord = -1;
	private BytesRef current;

	public SortedBytesTermsEnum(Map<byte[], int[]> postingsByTerm) {
		this.postingsByTerm = new TreeMap<>(BytesRefLexicographicComparator.INSTANCE);
		if (postingsByTerm != null) {
			this.postingsByTerm.putAll(postingsByTerm);
		}
		this.sortedKeys = this.postingsByTerm.keySet().toArray(new byte[0][]);
	}

	@Override
	public BytesRef term() throws IOException {
		return current;
	}

	@Override
	public int docFreq() {
		int[] docs = postingsByTerm.get(sortedKeys[ord]);
		return docs == null ? 0 : docs.length;
	}

	@Override
	public long totalTermFreq() {
		return docFreq();
	}

	@Override
	public PostingsEnum postings(PostingsEnum reuse, int flags) {
		if (ord < 0 || ord >= sortedKeys.length) {
			return null;
		}
		return new DocIdListPostingsEnum(postingsByTerm.get(sortedKeys[ord]));
	}

	@Override
	public TermsEnum$SeekStatus seekCeil(BytesRef target) throws IOException {
		int idx = ceilIndex(target);
		if (idx >= sortedKeys.length) {
			ord = sortedKeys.length;
			current = null;
			return TermsEnum$SeekStatus.END;
		}
		ord = idx;
		current = new BytesRef(sortedKeys[ord]);
		if (current.equals(target)) {
			return TermsEnum$SeekStatus.FOUND;
		}
		return TermsEnum$SeekStatus.NOT_FOUND;
	}

	private int ceilIndex(BytesRef target) {
		for (int i = 0; i < sortedKeys.length; i++) {
			BytesRef t = new BytesRef(sortedKeys[i]);
			if (t.compareTo(target) >= 0) {
				return i;
			}
		}
		return sortedKeys.length;
	}

	@Override
	public boolean seekExact(BytesRef target) throws IOException {
		return seekCeil(target) == TermsEnum$SeekStatus.FOUND;
	}

	@Override
	public TermState termState() {
		return null;
	}

	@Override
	public ImpactsEnum impacts(int flags) {
		return null;
	}

	@Override
	public AttributeSource attributes() {
		return null;
	}

	@Override
	public void seekExact(long ord) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void seekExact(BytesRef term, TermState state) throws IOException {
		seekExact(term);
	}

	@Override
	public BytesRef next() throws IOException {
		if (ord + 1 >= sortedKeys.length) {
			ord = sortedKeys.length;
			current = null;
			return null;
		}
		ord++;
		current = new BytesRef(sortedKeys[ord]);
		return current;
	}

	@Override
	public long ord() throws IOException {
		return ord;
	}

}
