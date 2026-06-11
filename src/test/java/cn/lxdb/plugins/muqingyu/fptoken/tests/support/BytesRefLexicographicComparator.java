package cn.lxdb.plugins.muqingyu.fptoken.tests.support;

import java.util.Comparator;

import org.apache.lucene.util.BytesRef;

/** Java 8 兼容的无符号字节序比较（用于 {@code byte[]} 词项键）。 */
public final class BytesRefLexicographicComparator implements Comparator<byte[]> {

	public static final BytesRefLexicographicComparator INSTANCE = new BytesRefLexicographicComparator();

	private BytesRefLexicographicComparator() {
	}

	@Override
	public int compare(byte[] a, byte[] b) {
		return new BytesRef(a).compareTo(new BytesRef(b));
	}
}
