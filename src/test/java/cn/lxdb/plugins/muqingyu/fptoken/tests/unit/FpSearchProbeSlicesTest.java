package cn.lxdb.plugins.muqingyu.fptoken.tests.unit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.lucene.util.BytesRef;
import org.junit.jupiter.api.Test;

import cn.lxdb.plugins.muqingyu.fptoken.api.FpSearch;
class FpSearchProbeSlicesTest {

	@Test
	void buildProbeSlices_abcd_includesAnchorAndLength3Windows() {
		final BytesRef abcd = new BytesRef(new byte[] { 'a', 'b', 'c', 'd' });
		final BytesRef[] probes = FpSearch.buildProbeSlices(abcd);
		assertEquals(3, probes.length);
		assertTrue(probes[0].equals(abcd));
		assertTrue(contains(probes, new BytesRef(new byte[] { 'a', 'b', 'c' })));
		assertTrue(contains(probes, new BytesRef(new byte[] { 'b', 'c', 'd' })));
	}

	@Test
	void buildProbeSlices_length2_includesLength1Substrings() {
		final BytesRef ab = new BytesRef(new byte[] { 'a', 'b' });
		final BytesRef[] probes = FpSearch.buildProbeSlices(ab);
		assertEquals(3, probes.length);
		assertTrue(probes[0].equals(ab));
		assertTrue(contains(probes, new BytesRef(new byte[] { 'a' })));
		assertTrue(contains(probes, new BytesRef(new byte[] { 'b' })));
	}

	private static boolean contains(BytesRef[] probes, BytesRef target) {
		for (BytesRef p : probes) {
			if (p.equals(target)) {
				return true;
			}
		}
		return false;
	}
}
