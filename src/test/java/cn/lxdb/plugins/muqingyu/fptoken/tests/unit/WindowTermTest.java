package cn.lxdb.plugins.muqingyu.fptoken.tests.unit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

import cn.lxdb.plugins.muqingyu.fptoken.token.ByteRef;
import cn.lxdb.plugins.muqingyu.fptoken.token.WindowTerm;

class WindowTermTest {

	@Test
	void ctor_copiesWindowBytes() {
		byte[] win = { 1, 2, 3 };
		ByteRef ref = new ByteRef(new byte[] { 9, 1, 2, 3, 8 }, 1, 3);
		WindowTerm wt = new WindowTerm(win, ref);
		assertArrayEquals(win, wt.getWindowBytes());
		win[0] = 99;
		assertEquals(1, wt.getWindowBytes()[0]);
		assertEquals(ref.getOffset(), wt.getSourceRef().getOffset());
		assertEquals(ref.getLength(), wt.getSourceRef().getLength());
	}

	@Test
	void ctor_rejectsNull() {
		ByteRef ref = ByteRef.wrap(new byte[] { 1 });
		assertThrows(IllegalArgumentException.class, () -> new WindowTerm(null, ref));
		assertThrows(IllegalArgumentException.class, () -> new WindowTerm(new byte[] { 1 }, null));
	}

	@Test
	void getWindowBytes_returnsDefensiveCopy() {
		byte[] win = { 5, 6 };
		WindowTerm wt = new WindowTerm(win, ByteRef.wrap(new byte[] { 1, 2, 3 }));
		byte[] first = wt.getWindowBytes();
		byte[] second = wt.getWindowBytes();
		assertArrayEquals(win, first);
		assertNotSame(first, second);
		win[0] = 99;
		assertEquals(5, wt.getWindowBytes()[0]);
	}
}
