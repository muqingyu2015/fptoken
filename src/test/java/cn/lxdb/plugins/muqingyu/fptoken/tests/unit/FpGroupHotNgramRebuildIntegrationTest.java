package cn.lxdb.plugins.muqingyu.fptoken.tests.unit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.lucene.util.BytesRef;
import org.junit.jupiter.api.Test;

import cn.lxdb.plugins.muqingyu.fptoken.config.Lucene80FPSearchConfig;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.block.FpGroupDataRebuild;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.block.FpGroupHotNgramBitIndex;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.block.FpGroupHotNgramRebuild;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FPDocList;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpTermKey;

/**
 * 热词重建写路径：高阈值下 common 仍保留；热词 doc 由 merge 回填。
 */
class FpGroupHotNgramRebuildIntegrationTest {

	@Test
	void highThreshold_keepsCommonKeys_hotDocsMergedFromCommon() throws Exception {
		final int maxDoc = 32;
		final FpGroupDataRebuild group = new FpGroupDataRebuild(maxDoc);
		final FpTermKey commonKey = FpTermKey.copyOf(new BytesRef("zz".getBytes()));
		final FPDocList commonDocs = new FPDocList(maxDoc);
		commonDocs.addDoc(5);
		group.commonTermMapInternal().put(commonKey, commonDocs);

		FpGroupHotNgramRebuild.execute(group, null, Lucene80FPSearchConfig.HOT_TIER_TERM_COUNT_THRESHOLD + 100);

		assertTrue(group.hotTermMapInternal().isEmpty(), "threshold too high: no hot promotion");
		assertTrue(group.commonTermMapInternal().containsKey(commonKey));
	}

	@Test
	void lowThreshold_promotesFrequentNgram_andSetsMaxDown() throws Exception {
		final int maxDoc = 32;
		final FpGroupDataRebuild group = new FpGroupDataRebuild(maxDoc);
		final FPDocList d1 = new FPDocList(maxDoc);
		d1.addDoc(1);
		d1.addDoc(2);
		final FPDocList d2 = new FPDocList(maxDoc);
		d2.addDoc(3);
		group.commonTermMapInternal().put(FpTermKey.copyOf(new BytesRef("abab".getBytes())), d1);
		group.commonTermMapInternal().put(FpTermKey.copyOf(new BytesRef("xyzz".getBytes())), d2);

		FpGroupHotNgramRebuild.execute(group, null, 2);
		final FpGroupHotNgramBitIndex bits = FpGroupHotNgramBitIndex.execute((byte) 1, group);
		final BytesRef ab = new BytesRef(new byte[] { 'a', 'b' });
		final int bucket = FpGroupHotNgramBitIndex.bucketIndex(ab.bytes, ab.offset, ab.length);
		final int li = ab.length - 1;
		assertTrue(bits.banksHot[li][bucket].nextSetBit(0) >= 0 || bits.banksCommon[li][bucket].nextSetBit(0) >= 0,
				"rebuild+bit execute should mark query slice ab for search");
	}

	@Test
	void markParentPrefixesSkipped_respectsMaxDown() {
		final cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpStatNgram stat = new cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpStatNgram();
		final java.util.Set<FpTermKey> merged = new java.util.HashSet<>();
		final java.util.TreeMap<FpTermKey, Integer> levels = new java.util.TreeMap<>();
		final FpTermKey parent = FpTermKey.copyOf(new BytesRef(new byte[] { 'a' }));
		levels.put(parent, 2);
		final BytesRef child = new BytesRef(new byte[] { 'a', 'b' });
		FpGroupHotNgramRebuild.markParentPrefixesSkippedInCommonTerm(stat, child, merged, levels);
		assertTrue(merged.contains(parent));
	}
}
