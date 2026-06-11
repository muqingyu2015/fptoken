package cn.lxdb.plugins.muqingyu.fptoken.tests.unit;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertSame;

import org.junit.jupiter.api.Test;

import cn.lxdb.plugins.muqingyu.fptoken.token.ByteRef;

class ByteRefTest {

	@Test
	void wrap_and_copyBytes() {
		byte[] src = { 10, 20, 30 };
		ByteRef ref = ByteRef.wrap(src);
		assertSame(src, ref.getSourceUnsafe());
		org.junit.jupiter.api.Assertions.assertEquals(0, ref.getOffset());
		org.junit.jupiter.api.Assertions.assertEquals(3, ref.getLength());
		org.junit.jupiter.api.Assertions.assertEquals(20, ref.byteAt(1));
		org.junit.jupiter.api.Assertions.assertArrayEquals(src, ref.copyBytes());
	}

	@Test
	void slice_bounds() {
		byte[] src = { 1, 2, 3, 4, 5 };
		ByteRef ref = new ByteRef(src, 1, 3);
		org.junit.jupiter.api.Assertions.assertArrayEquals(new byte[] { 2, 3, 4 }, ref.copyBytes());
	}

	@Test
	void byteAt_outOfRange() {
		ByteRef ref = ByteRef.wrap(new byte[] { 1 });
		assertThrows(IllegalArgumentException.class, () -> ref.byteAt(1));
	}

	@Test
	void ctor_validatesArguments() {
		byte[] src = { 1, 2, 3 };
		assertThrows(IllegalArgumentException.class, () -> new ByteRef(null, 0, 1));
		assertThrows(IllegalArgumentException.class, () -> new ByteRef(src, -1, 1));
		assertThrows(IllegalArgumentException.class, () -> new ByteRef(src, 0, -1));
		assertThrows(IllegalArgumentException.class, () -> new ByteRef(src, 4, 1));
		assertThrows(IllegalArgumentException.class, () -> new ByteRef(src, 2, 2));
	}
}
