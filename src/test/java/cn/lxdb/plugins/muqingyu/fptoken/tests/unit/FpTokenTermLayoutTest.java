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
	void make_fp_term_roundTripHeaderFields_atOffsetZero() {
		byte[] buf = new byte[32];
		BytesRef reuse = new BytesRef(buf);
		reuse.offset = 0;
		BytesRef payload = new BytesRef(new byte[] { 0x41, 0x42 });
		// index_id 占位为 0 时与 read_index_id(byte[]) 一致；非 0 见 read_index_id_nonZero_mismatch
		FpTokenTermLayout.make_fp_term(reuse, (short) 0, 12345, (byte) 2, true, 10, false, payload);
		assertEquals(0, FpTokenTermLayout.read_index_id(reuse.bytes));
		assertEquals(2, FpTokenTermLayout.readLevel(reuse));
		assertTrue(FpTokenTermLayout.isHotTerm(reuse));
		assertEquals(10, FpTokenTermLayout.readTermIndex(reuse));
		assertFalse(FpTokenTermLayout.readIsDelTerm(reuse));
		assertEquals(2, FpTokenTermLayout.removeHeaderBytes(reuse).length);
	}

	@Test
	void read_index_id_nonZeroIndexId_mismatchWithPatchedNumericUtils() {
		byte[] buf = new byte[32];
		BytesRef reuse = new BytesRef(buf);
		FpTokenTermLayout.make_fp_term(reuse, (short) 7, 1, (byte) 1, false, 0, false, new BytesRef(new byte[] { 1 }));
		assertNotEquals(7, FpTokenTermLayout.read_index_id(reuse.bytes),
				"补丁 Lucene NumericUtils.shortToSortableBytes 实为 int 四字节写入，read_index_id 用 sortableBytesToShort 读前两字节");
	}

	@Test
	void read_index_id_withNonZeroBytesRefOffset_readsWrongSlot() {
		byte[] buf = new byte[32];
		BytesRef reuse = new BytesRef(buf);
		reuse.offset = 4;
		FpTokenTermLayout.make_fp_term(reuse, (short) 7, 12345, (byte) 1, false, 0, false,
				new BytesRef(new byte[] { 1 }));
		assertNotEquals(7, FpTokenTermLayout.read_index_id(reuse.bytes),
				"read_index_id(byte[]) ignores BytesRef.offset; header written at offset+4");
	}

	@Test
	void read_group_id_roundTrip_withinProductRange() {
		byte[] buf = new byte[FpTokenTermLayout.FP_HEADER_BYTES + 1];
		BytesRef reuse = new BytesRef(buf);
		int groupId = 12345;
		FpTokenTermLayout.make_fp_term(reuse, (short) 1, groupId, (byte) 1, false, 0, false,
				new BytesRef(new byte[] { 1 }));
		assertEquals((short) groupId, FpTokenTermLayout.read_group_id(reuse.bytes));
	}
}
