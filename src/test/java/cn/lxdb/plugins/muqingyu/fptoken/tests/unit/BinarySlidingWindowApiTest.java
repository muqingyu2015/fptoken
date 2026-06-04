package cn.lxdb.plugins.muqingyu.fptoken.tests.unit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.Test;

import cn.lxdb.plugins.muqingyu.fptoken.token.BinarySlidingWindowApi;
import cn.lxdb.plugins.muqingyu.fptoken.token.ByteRef;
import cn.lxdb.plugins.muqingyu.fptoken.token.WindowTerm;

class BinarySlidingWindowApiTest {

	@Test
	void slidingWindows_shortInput_singleWindow() {
		byte[] src = { 1, 2, 3 };
		List<WindowTerm> wins = BinarySlidingWindowApi.slidingWindows(src, 0, 3, 64, 32);
		assertEquals(1, wins.size());
		assertEquals(3, wins.get(0).getWindowBytes().length);
	}

	@Test
	void slidingWindows_64x32_producesMultipleDistinctStarts() {
		byte[] src = new byte[128];
		for (int i = 0; i < src.length; i++) {
			src[i] = (byte) i;
		}
		List<WindowTerm> wins = BinarySlidingWindowApi.bitsetWindowsforToken(src, 0, src.length);
		Set<Integer> starts = new HashSet<>();
		for (WindowTerm w : wins) {
			ByteRef ref = w.getSourceRef();
			starts.add(ref.getOffset());
			assertEquals(64, w.getWindowBytes().length);
		}
		assertTrue(wins.size() >= 2, "expected step windows plus tail alignment");
		assertTrue(starts.contains(0));
		assertTrue(starts.contains(64), "tail-aligned last window at offset 64 for length 128");
	}

	@Test
	void slidingWindows_emptyLength_returnsEmpty() {
		byte[] src = { 1, 2 };
		assertTrue(BinarySlidingWindowApi.slidingWindows(src, 0, 0, 8, 4).isEmpty());
	}

	@Test
	void slidingWindows_invalidArgs_throw() {
		byte[] src = { 1 };
		assertThrows(IllegalArgumentException.class,
				() -> BinarySlidingWindowApi.slidingWindows(null, 0, 1, 4, 2));
		assertThrows(IllegalArgumentException.class,
				() -> BinarySlidingWindowApi.slidingWindows(src, 0, 2, 4, 2));
		assertThrows(IllegalArgumentException.class,
				() -> BinarySlidingWindowApi.slidingWindows(src, 0, 1, 0, 2));
	}
}
