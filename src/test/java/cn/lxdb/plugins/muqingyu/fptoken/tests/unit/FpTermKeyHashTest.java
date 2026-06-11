package cn.lxdb.plugins.muqingyu.fptoken.tests.unit;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.lucene.util.BytesRef;
import org.junit.jupiter.api.Test;

import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpTermKey;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpTermKeyHash;

class FpTermKeyHashTest {

	@Test
	void abcd_equals_abc_plus_d() {
		byte[] bytes = "abcd".getBytes();
		final int full = FpTermKeyHash.hashOf(bytes, 0, 4);
		final int abc = FpTermKeyHash.hashOf(bytes, 0, 3);
		assertEquals(full, FpTermKeyHash.rollAppend(abc, bytes[3]));
	}

	@Test
	void abcd_equals_abcde_minus_e() {
		byte[] bytes = "abcde".getBytes();
		final int abcd = FpTermKeyHash.hashOf(bytes, 0, 4);
		final int abcde = FpTermKeyHash.hashOf(bytes, 0, 5);
		assertEquals(abcd, FpTermKeyHash.rollRemoveLast(abcde, bytes[4]));
	}

	@Test
	void abcd_equals_abcdef_minus_ef() {
		byte[] bytes = "abcdef".getBytes();
		final int abcd = FpTermKeyHash.hashOf(bytes, 0, 4);
		final int abcdef = FpTermKeyHash.hashOf(bytes, 0, 6);
		assertEquals(abcd, FpTermKeyHash.rollRemoveSuffix(abcdef, bytes, 6, 2));
	}

	@Test
	void abcd_equals_ab_plus_cd_and_a_plus_bcd() {
		byte[] bytes = "abcd".getBytes();
		final int full = FpTermKeyHash.hashOf(bytes, 0, 4);
		final int ab = FpTermKeyHash.hashOf(bytes, 0, 2);
		final int a = FpTermKeyHash.hashOf(bytes, 0, 1);
		assertEquals(full, FpTermKeyHash.rollAppend(ab, bytes, 2, 2));
		assertEquals(full, FpTermKeyHash.rollAppend(a, bytes, 1, 3));
	}

	@Test
	void rollAppend_matchesFullHash_byLength() {
		byte[] bytes = { 'a', 'b', 'c', 'd', 'e' };
		int h = 0;
		for (int len = 1; len <= 5; len++) {
			h = FpTermKeyHash.rollAppend(h, bytes[len - 1]);
			assertEquals(FpTermKeyHash.hashOf(bytes, 0, len), h);
		}
	}

	@Test
	void rollStartForward_matchesFullHash() {
		byte[] bytes = "hello world".getBytes();
		final int len = 4;
		int h = FpTermKeyHash.hashOf(bytes, 0, len);
		for (int start = 1; start + len <= bytes.length; start++) {
			h = FpTermKeyHash.rollStartForward(h, bytes, start - 1, len);
			assertEquals(FpTermKeyHash.hashOf(bytes, start, len), h);
		}
	}

	@Test
	void viewOf_usesSameRollingHash() {
		byte[] buf = "abcde".getBytes();
		final BytesRef slice = new BytesRef(buf, 0, 0);
		int h = 0;
		for (int n = 1; n <= 5; n++) {
			h = FpTermKeyHash.rollAppend(h, buf[n - 1]);
			slice.length = n;
			assertEquals(FpTermKey.viewOf(slice, h), FpTermKey.viewOf(slice));
		}
	}
}
