package cn.lxdb.plugins.muqingyu.fptoken.tests.unit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import java.util.TreeMap;

import org.apache.lucene.util.BytesRef;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpTermKey;

@Tag("lxdb-runtime")
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
}
