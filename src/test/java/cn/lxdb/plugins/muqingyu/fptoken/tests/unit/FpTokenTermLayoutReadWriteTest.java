package cn.lxdb.plugins.muqingyu.fptoken.tests.unit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.lucene.util.BytesRef;
import org.junit.jupiter.api.Test;

import cn.lxdb.plugins.muqingyu.fptoken.config.Lucene80FPSearchConfig;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpTokenTermLayout;

/** 写段 {@link FpTokenTermLayout#make_fp_term} 与查询读字段一一对应。 */
class FpTokenTermLayoutReadWriteTest {

	@Test
	void make_fp_term_roundTrip_allHeaderFields() {
		final byte[] buf = new byte[64];
		final BytesRef reuse = new BytesRef(buf);
		final BytesRef payload = new BytesRef(new byte[] { 10, 20, 30 });
		final short indexId = Lucene80FPSearchConfig.DEFAULT_INDEX_ID;
		final int groupId = 0x01020304;
		final byte level = 2;
		final int termIndex = 7;
		final byte hotScan = 3;

		FpTokenTermLayout.make_fp_term(reuse, indexId, groupId, level, true, termIndex, true, hotScan, payload);

		assertEquals(FpTokenTermLayout.FP_HEADER_BYTES + payload.length, reuse.length);
		assertEquals(groupId, FpTokenTermLayout.read_group_id(reuse));
		assertEquals(level & 0xFF, FpTokenTermLayout.readLevel(reuse));
		assertTrue(FpTokenTermLayout.isHotTerm(reuse));
		assertEquals(termIndex, FpTokenTermLayout.readTermIndex(reuse));
		assertTrue(FpTokenTermLayout.readIsDelTerm(reuse));
		assertEquals(hotScan & 0xFF, FpTokenTermLayout.readHotDownTierBudget(reuse));
		final BytesRef payloadOut = FpTokenTermLayout.removeHeaderBytes(reuse);
		assertEquals(payload.length, payloadOut.length);
		for (int i = 0; i < payload.length; i++) {
			assertEquals(payload.bytes[payload.offset + i], payloadOut.bytes[payloadOut.offset + i]);
		}
	}

	@Test
	void make_fp_search_prefix_matchesFirst12BytesOfFullTerm() {
		final byte[] buf = new byte[64];
		final BytesRef full = new BytesRef(buf);
		final BytesRef prefix = new BytesRef(new byte[64]);
		final BytesRef payload = new BytesRef(new byte[] { 1, 2 });

		FpTokenTermLayout.make_fp_term(full, (short) 0, 99, (byte) 1, false, 4, false, (byte) 5, payload);
		FpTokenTermLayout.make_fp_search_prefix(prefix, (short) 0, 99, (byte) 1, false, 4, false);

		assertEquals(FpTokenTermLayout.TERM_PREFIX_BYTES, prefix.length);
		for (int i = 0; i < FpTokenTermLayout.TERM_PREFIX_BYTES; i++) {
			assertEquals(full.bytes[full.offset + i], prefix.bytes[prefix.offset + i]);
		}
		assertEquals(5, FpTokenTermLayout.readHotDownTierBudget(full));
	}

	@Test
	void readGroupIdFromIndexAndGroupKey_matchesWrite() {
		final byte[] buf = new byte[32];
		final BytesRef reuse = new BytesRef(buf);
		FpTokenTermLayout.make_fp_term(reuse, (short) 0, 42, (byte) 0, false, 1, false, (byte) 0,
				new BytesRef(new byte[] { 9 }));
		final byte[] six = new byte[FpTokenTermLayout.INDEX_AND_GROUP_BYTES];
		FpTokenTermLayout.copyIndexAndGroup(reuse, six);
		assertEquals(42, FpTokenTermLayout.readGroupIdFromIndexAndGroupKey(six));
		assertTrue(FpTokenTermLayout.indexAndGroupEquals(reuse, six));
	}

	@Test
	void copyGroupKey_and_clearAndCopyGroupBytes() {
		final BytesRef term = new BytesRef(new byte[] { 1, 2, 3, 4, 5, 6, 7, 8 });
		final BytesRef key = FpTokenTermLayout.copyGroupKey(term);
		assertEquals(FpTokenTermLayout.INDEX_AND_GROUP_BYTES, key.length);
		final BytesRef cleared = FpTokenTermLayout.clearAndCopyGroupBytes(term);
		for (int i = 0; i < FpTokenTermLayout.INDEX_AND_GROUP_BYTES; i++) {
			assertEquals(0, cleared.bytes[cleared.offset + i]);
		}
		assertEquals(7, cleared.bytes[cleared.offset + FpTokenTermLayout.INDEX_AND_GROUP_BYTES]);
	}

	@Test
	void commonTerm_markAndDelBit() {
		final BytesRef reuse = new BytesRef(new byte[32]);
		FpTokenTermLayout.make_fp_term(reuse, (short) 0, 1, (byte) 0, FpTokenTermLayout.TERM_MARK_COMMON, 3, false,
				(byte) 0, new BytesRef(new byte[] { 0 }));
		assertFalse(FpTokenTermLayout.isHotTerm(reuse));
		assertFalse(FpTokenTermLayout.readIsDelTerm(reuse));
		assertEquals(3, FpTokenTermLayout.readTermIndex(reuse));
	}
}
