package cn.lxdb.plugins.muqingyu.fptoken.tests.unit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.lucene.util.BytesRef;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpTokenTermLayout;

@Tag("lxdb-runtime")
class FpTokenTermLayoutTest {

	@Test
	void make_fp_term_roundTripHeaderFields() {
		byte[] buf = new byte[32];
		BytesRef reuse = new BytesRef(buf);
		reuse.offset = 4;
		BytesRef payload = new BytesRef(new byte[] { 0x41, 0x42 });
		FpTokenTermLayout.make_fp_term(reuse, (short) 7, 12345, (byte) 2, true, 10, false, payload);
		assertEquals(7, FpTokenTermLayout.read_index_id(reuse.bytes));
		assertEquals(2, FpTokenTermLayout.readLevel(reuse));
		assertTrue(FpTokenTermLayout.isHotTerm(reuse));
		assertEquals(10, FpTokenTermLayout.readTermIndex(reuse));
		assertFalse(FpTokenTermLayout.readIsDelTerm(reuse));
		assertEquals(2, FpTokenTermLayout.removeHeaderBytes(reuse).length);
	}

	@Test
	void read_group_id_usesIntSlot_butReturnsShortCast() {
		byte[] buf = new byte[FpTokenTermLayout.FP_HEADER_BYTES + 1];
		BytesRef reuse = new BytesRef(buf);
		FpTokenTermLayout.make_fp_term(reuse, (short) 1, 70000, (byte) 1, false, 0, false, new BytesRef(new byte[] { 1 }));
		short gid = FpTokenTermLayout.read_group_id(reuse.bytes);
		assertEquals((short) 70000, gid);
	}
}
