package cn.lxdb.plugins.muqingyu.fptoken.tests.support;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.lucene.util.FixedBitSet;

/** 从 {@link FixedBitSet} 提取 doc 集合以便断言（避免 off-heap {@code nextSetBit} 兼容问题）。 */
public final class FpTestDocSets {

	private FpTestDocSets() {
	}

	public static int[] toSortedArray(FixedBitSet bits, int maxDoc) {
		if (bits == null || maxDoc <= 0) {
			return new int[0];
		}
		final List<Integer> docs = new ArrayList<>();
		for (int d = 0; d < maxDoc; d++) {
			if (bits.get(d)) {
				docs.add(d);
			}
		}
		final int[] arr = new int[docs.size()];
		for (int i = 0; i < docs.size(); i++) {
			arr[i] = docs.get(i).intValue();
		}
		return arr;
	}

	public static boolean equals(FixedBitSet bits, int maxDoc, int... expected) {
		final int[] actual = toSortedArray(bits, maxDoc);
		final int[] exp = Arrays.copyOf(expected, expected.length);
		Arrays.sort(exp);
		return Arrays.equals(actual, exp);
	}
}
