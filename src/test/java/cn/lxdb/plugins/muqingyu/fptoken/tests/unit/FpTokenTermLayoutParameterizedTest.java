package cn.lxdb.plugins.muqingyu.fptoken.tests.unit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.lucene.util.BytesRef;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpTokenTermLayout;
import cn.lxdb.plugins.muqingyu.fptoken.tests.support.FpTestColumnNames;

/** {@link FpTokenTermLayout#make_fp_term} 写与读字段一一对应（1000+ 组）。 */
class FpTokenTermLayoutParameterizedTest {

	private static final BytesRef COLUMN = FpTestColumnNames.DEFAULT;

	@ParameterizedTest(name = "group={0} idx={1} hot={2}")
	@MethodSource("cn.lxdb.plugins.muqingyu.fptoken.tests.support.FpParameterizedTestSources#layoutRoundTripCases")
	void make_fp_term_roundTrip_readFields(int groupId, int termIndex, boolean hot) {
		final byte[] buf = new byte[128];
		final BytesRef reuse = new BytesRef(buf);
		final BytesRef payload = new BytesRef(new byte[] { (byte) groupId, (byte) termIndex, 3 });
		final byte scan = (byte) (termIndex & 0xFF);

		FpTokenTermLayout.make_fp_term(reuse, COLUMN, (short) 0, groupId, (byte) 2, hot, termIndex, false, scan,
				payload);

		assertEquals(groupId, FpTokenTermLayout.read_group_id(reuse));
		assertEquals(2, FpTokenTermLayout.readLevel(reuse));
		assertEquals(hot, FpTokenTermLayout.isHotTerm(reuse));
		assertEquals(termIndex, FpTokenTermLayout.readTermIndex(reuse));
		assertFalse(FpTokenTermLayout.readIsDelTerm(reuse));
		assertEquals(scan & 0xFF, FpTokenTermLayout.readHotDownTierBudget(reuse));
		assertEquals(payload, FpTokenTermLayout.removeColumnAndHeaderBytes(reuse));
	}

	@ParameterizedTest(name = "group={0} idx={1}")
	@MethodSource("cn.lxdb.plugins.muqingyu.fptoken.tests.support.FpParameterizedTestSources#searchPrefixAlignmentCases")
	void make_fp_search_prefix_alignsWithFullTermHeader(int groupId, int termIndex) {
		final byte[] buf = new byte[128];
		final BytesRef full = new BytesRef(buf);
		final BytesRef prefix = new BytesRef(new byte[128]);
		final BytesRef payload = new BytesRef(new byte[] { 9 });

		FpTokenTermLayout.make_fp_term(full, COLUMN, (short) 0, groupId, (byte) 1, true, termIndex, false, (byte) 4,
				payload);
		FpTokenTermLayout.make_fp_search_prefix(prefix, COLUMN, (short) 0, groupId, (byte) 1, true, termIndex);

		final int colPrefix = FpTokenTermLayout.columnNamePrefixBytes(COLUMN);
		assertEquals(colPrefix + FpTokenTermLayout.TERM_PREFIX_BYTES, prefix.length);
		for (int i = 0; i < colPrefix + FpTokenTermLayout.TERM_PREFIX_BYTES; i++) {
			assertEquals(full.bytes[full.offset + i], prefix.bytes[prefix.offset + i]);
		}
		assertTrue(FpTokenTermLayout.isHotTerm(full));
	}
}
