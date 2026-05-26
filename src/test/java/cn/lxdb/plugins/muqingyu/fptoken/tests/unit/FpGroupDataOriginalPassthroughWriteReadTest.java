package cn.lxdb.plugins.muqingyu.fptoken.tests.unit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.apache.lucene.util.BytesRef;
import org.junit.jupiter.api.Test;

import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FPDocList;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpTermKey;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpTokenTermLayout;
import cn.lxdb.plugins.muqingyu.fptoken.tests.support.FpTermFlushEncoding;

/** 透传写段词项头与查询读字段对齐（不依赖 BlockTree writefp）。 */
class FpGroupDataOriginalPassthroughWriteReadTest {

	@Test
	void encodeOriginalHot_remapsGroupId_preservesPayloadAndScanLevel() {
		final byte[] buf = new byte[64];
		final BytesRef stored = new BytesRef(buf);
		final BytesRef payload = new BytesRef(new byte[] { 11, 22 });
		FpTokenTermLayout.make_fp_term(stored, (short) 0, 5, (byte) 2, true, 3, false, (byte) 4, payload);
		final FpTermKey key = FpTermKey.copyOf(stored);
		final FPDocList docs = new FPDocList(16);
		docs.addDoc(1);

		final BytesRef flushed = FpTermFlushEncoding.encodeOriginalHot(new BytesRef(new byte[64]), 99, (byte) 2, key,
				docs);

		assertEquals(99, FpTokenTermLayout.read_group_id(flushed));
		assertEquals(4, FpTokenTermLayout.readHotDownTierBudget(flushed));
		assertEquals(3, FpTokenTermLayout.readTermIndex(flushed));
		assertEquals(payload, FpTokenTermLayout.removeHeaderBytes(flushed));
	}

	@Test
	void encodeOriginalCommon_skipsEmptyDocList() {
		final FpTermKey key = FpTermKey.copyOf(new BytesRef(new byte[] { 7, 8 }));
		final FPDocList empty = new FPDocList(8);
		assertNull(FpTermFlushEncoding.encodeOriginalCommon(new BytesRef(new byte[32]), 1, (byte) 0, key, empty));
	}
}
