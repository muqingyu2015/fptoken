package cn.lxdb.plugins.muqingyu.fptoken.tests.unit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpTokenTermLayout;

@Tag("lxdb-runtime")
class FpTokenTermLayoutTest {

	private static BytesRef termWithColumnAndHeader(byte[] columnName, byte groupLevel, boolean hot, byte budget,
			byte[] payload) {
		final int colPrefix = FpTokenTermLayout.COLUMN_NAME_LEN_BYTES + columnName.length;
		final byte[] buf = new byte[colPrefix + FpTokenTermLayout.FP_HEADER_BYTES + payload.length];
		NumericUtils.intToSortableBytes(columnName.length, buf, 0);
		System.arraycopy(columnName, 0, buf, FpTokenTermLayout.COLUMN_NAME_LEN_BYTES, columnName.length);
		final int h = colPrefix;
		buf[h + FpTokenTermLayout.GROUP_LEVEL_OFFSET] = groupLevel;
		buf[h + FpTokenTermLayout.TERM_FLAG_OFFSET] = (byte) (hot ? 0 : 1);
		buf[h + FpTokenTermLayout.HOT_TERM_DOWN_TIER_BUDGET_OFFSET] = budget;
		System.arraycopy(payload, 0, buf, h + FpTokenTermLayout.FP_HEADER_BYTES, payload.length);
		return new BytesRef(buf);
	}

	@Test
	void readColumnNameLen_and_prefixBytes() {
		byte[] col = "idx".getBytes();
		assertEquals(3, FpTokenTermLayout.readColumnNameLen(col));
		assertEquals(7, FpTokenTermLayout.columnNamePrefixBytes(new BytesRef(col)));
		assertEquals(7, FpTokenTermLayout.columnNamePrefixBytesFromLen(3));
	}

	@Test
	void readColumnName_and_equals() {
		BytesRef col = new BytesRef("field_a");
		BytesRef term = termWithColumnAndHeader("field_a".getBytes(), (byte) 2, true, (byte) 3, new byte[] { 9 });
		assertTrue(FpTokenTermLayout.columnNameEquals(term, col));
		assertFalse(FpTokenTermLayout.columnNameEquals(term, new BytesRef("other")));
		assertEquals("field_a", FpTokenTermLayout.readColumnName(term).utf8ToString());
	}

	@Test
	void headerReaders() {
		BytesRef term = termWithColumnAndHeader("c".getBytes(), (byte) 4, false, (byte) 5, new byte[] { 1, 2 });
		assertEquals(4, FpTokenTermLayout.readLevel(term));
		assertFalse(FpTokenTermLayout.isHotTerm(term));
		assertEquals(5, FpTokenTermLayout.readHotDownTierBudget(term));
		assertEquals(2, FpTokenTermLayout.removeColumnAndHeaderBytes(term).length);
	}

	@Test
	void maxHotPayloadLen() {
		assertEquals(7, FpTokenTermLayout.maxHotPayloadLen(4, 4));
		BytesRef term = termWithColumnAndHeader("c".getBytes(), (byte) 1, true, (byte) 4, new byte[] { 1 });
		assertEquals(7, FpTokenTermLayout.maxHotPayloadLenFromHeader(term, 4));
	}

	@Test
	void copyGroupKey_and_column_index_group_equals() {
		BytesRef term = termWithColumnAndHeader("xy".getBytes(), (byte) 1, true, (byte) 1, new byte[] { 8 });
		byte[] six = FpTokenTermLayout.column_index_group_copy(term);
		assertTrue(FpTokenTermLayout.column_index_group_equals(term, six));
		assertTrue(FpTokenTermLayout.column_equals(term, six));
		assertEquals(FpTokenTermLayout.INDEX_AND_GROUP_BYTES, FpTokenTermLayout.copyGroupKey(term).length);
	}

	@Test
	void clearAndCopyGroupBytes_zerosIndexAndGroup() {
		BytesRef term = termWithColumnAndHeader("z".getBytes(), (byte) 2, true, (byte) 1, new byte[] { 3 });
		BytesRef cleared = FpTokenTermLayout.clearAndCopyGroupBytes(term);
		for (int i = 0; i < FpTokenTermLayout.INDEX_AND_GROUP_BYTES; i++) {
			assertEquals(0, cleared.bytes[FpTokenTermLayout.headerOffset(cleared) + i]);
		}
	}

	@Test
	void modify_index_id_tooShort_noOp() {
		BytesRef shortTerm = new BytesRef(new byte[] { 1, 2 });
		FpTokenTermLayout.modify_index_id(shortTerm, 9);
		assertEquals(2, shortTerm.length);
	}
}
