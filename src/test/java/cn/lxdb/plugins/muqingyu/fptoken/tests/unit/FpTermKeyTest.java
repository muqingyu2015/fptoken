package cn.lxdb.plugins.muqingyu.fptoken.tests.unit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.TreeMap;

import org.apache.lucene.util.BytesRef;
import org.junit.jupiter.api.Test;

import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpTermKey;

class FpTermKeyTest {

	@Test
	void copyOf_and_treeMapOrder() {
		TreeMap<FpTermKey, String> map = new TreeMap<>();
		map.put(FpTermKey.copyOf(new BytesRef(new byte[] { 2 })), "b");
		map.put(FpTermKey.copyOf(new BytesRef(new byte[] { 1 })), "a");
		assertEquals("a", map.firstEntry().getValue());
	}

	@Test
	void viewOf_matchesCopyWhenBufferStable() {
		byte[] buf = { 9, 8, 7 };
		BytesRef slice = new BytesRef(buf, 1, 2);
		FpTermKey view = FpTermKey.viewOf(slice);
		FpTermKey copy = FpTermKey.copyOf(slice);
		assertEquals(copy, view);
		assertEquals(copy.hashCode(), view.hashCode());
	}

	@Test
	void viewOf_seesBufferMutation() {
		byte[] buf = { 1, 2 };
		BytesRef slice = new BytesRef(buf, 0, 2);
		FpTermKey view = FpTermKey.viewOf(slice);
		FpTermKey expectedBefore = FpTermKey.copyOf(new BytesRef(new byte[] { 1, 2 }));
		assertEquals(expectedBefore, view);
		buf[0] = 99;
		assertNotEquals(expectedBefore, view);
	}

	@Test
	void equals_and_hashCode_useCachedHash() {
		FpTermKey a = FpTermKey.copyOf(new BytesRef("ab"));
		FpTermKey b = FpTermKey.copyOf(new BytesRef("ab"));
		FpTermKey c = FpTermKey.copyOf(new BytesRef("ac"));
		assertEquals(a, b);
		assertEquals(a.hashCode(), b.hashCode());
		assertNotEquals(a, c);
		assertNotEquals(a, null);
		assertNotEquals(a, "ab");
	}

	@Test
	void copyOf_fromExistingKey() {
		FpTermKey orig = FpTermKey.copyOf(new BytesRef(new byte[] { 7, 8 }));
		FpTermKey dup = FpTermKey.copyOf(orig);
		assertEquals(orig, dup);
		assertNotSame(orig.bytesRef().bytes, dup.bytesRef().bytes);
	}

	@Test
	void orderByLengthThenBytes_sortsShorterFirst() {
		FpTermKey shortKey = FpTermKey.copyOf(new BytesRef(new byte[] { 9 }));
		FpTermKey longKey = FpTermKey.copyOf(new BytesRef(new byte[] { 1, 2 }));
		assertTrue(FpTermKey.ORDER_BY_LENGTH_THEN_BYTES.compare(shortKey, longKey) < 0);
	}

	@Test
	void toString_includesHash() {
		FpTermKey k = FpTermKey.copyOf(new BytesRef("x"));
		assertTrue(k.toString().contains("hash="));
		assertTrue(k.toString().contains("FpTermKey"));
	}
}
