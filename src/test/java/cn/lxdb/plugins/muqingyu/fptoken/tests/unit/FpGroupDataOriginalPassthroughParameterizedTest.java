package cn.lxdb.plugins.muqingyu.fptoken.tests.unit;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.lucene.util.BytesRef;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FPDocList;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpTermKey;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpTokenTermLayout;
import cn.lxdb.plugins.muqingyu.fptoken.tests.support.FpTermFlushEncoding;

/** 透传写 {@link FpTermFlushEncoding#encodeOriginalHot} 与读 group_id 对齐。 */
class FpGroupDataOriginalPassthroughParameterizedTest {

	@ParameterizedTest(name = "old={0} new={1}")
	@MethodSource("cn.lxdb.plugins.muqingyu.fptoken.tests.support.FpParameterizedTestSources#originalPassthroughGroupRemapCases")
	void encodeOriginalHot_remapsGroupId(int oldGroup, int newGroup) {
		final byte[] buf = new byte[48];
		final BytesRef stored = new BytesRef(buf);
		FpTokenTermLayout.make_fp_term(stored, (short) 0, oldGroup, (byte) 1, true, 5, false, (byte) 2,
				new BytesRef(new byte[] { 1, 2, 3 }));
		final FpTermKey key = FpTermKey.copyOf(stored);
		final FPDocList docs = new FPDocList(32);
		docs.addDoc(1);

		final BytesRef flushed = FpTermFlushEncoding.encodeOriginalHot(new BytesRef(new byte[64]), newGroup, (byte) 1,
				key, docs);
		assertEquals(newGroup, FpTokenTermLayout.read_group_id(flushed));
		assertEquals(oldGroup, FpTokenTermLayout.read_group_id(stored));
	}
}
