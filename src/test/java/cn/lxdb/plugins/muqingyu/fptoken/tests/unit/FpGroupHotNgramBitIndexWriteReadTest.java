package cn.lxdb.plugins.muqingyu.fptoken.tests.unit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.junit.jupiter.api.Test;

import cn.lxdb.plugins.muqingyu.fptoken.config.Lucene80FPSearchConfig;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.block.FpGroupDataRebuild;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.block.FpGroupHotNgramBitIndex;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.block.FpGroupHotNgramRebuild;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FPDocList;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpBlockInfo;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpTermKey;

/** 位图写（{@link FpGroupHotNgramBitIndex#execute}/{@link #flushto}）与读（{@link #readfrom}）对齐。 */
class FpGroupHotNgramBitIndexWriteReadTest {

	@Test
	void bucketIndex_singleByte_isRawByte() {
		final byte[] b = { (byte) 0xAB };
		assertEquals(0xAB, FpGroupHotNgramBitIndex.bucketIndex(b, 0, 1));
	}

	@Test
	void bucketIndex_multiByte_isStableHash() {
		final byte[] a = { 1, 2, 3 };
		final byte[] b = { 1, 2, 3 };
		assertEquals(FpGroupHotNgramBitIndex.bucketIndex(a, 0, 3), FpGroupHotNgramBitIndex.bucketIndex(b, 0, 3));
	}

	@Test
	void execute_setsBitsOnHotOrCommonBanks_afterRebuild() throws Exception {
		final FpGroupDataRebuild group = newSearchScenarioGroup();
		FpGroupHotNgramRebuild.execute(group, null, 2);
		final FpGroupHotNgramBitIndex bits = FpGroupHotNgramBitIndex.execute((byte) 1, group);

		final BytesRef slice = new BytesRef(new byte[] { 'a', 'b' });
		final int li = slice.length - 1;
		final int bucket = FpGroupHotNgramBitIndex.bucketIndex(slice.bytes, slice.offset, slice.length);
		final FixedBitSet hot = bits.banksHot[li][bucket];
		final FixedBitSet common = bits.banksCommon[li][bucket];
		assertTrue(hot.nextSetBit(0) >= 0 || common.nextSetBit(0) >= 0,
				"write path bit index should mark ab slice on hot or common bank");
	}

	@Test
	void flushto_populatesBlockInfoLayout() throws Exception {
		final FpGroupHotNgramBitIndex bits = buildSmallBitIndex();
		final Directory dir = new RAMDirectory();
		final FpBlockInfo written;
		try (IndexOutput out = dir.createOutput("bits", IOContext.DEFAULT)) {
			written = bits.flushto(out,"");
		}
		assertTrue(written.bytesPerHotSerialized > 0);
		assertTrue(written.bytesPerCommonSerialized > 0);
		assertEquals(written.fpBanksHot + written.bytesPerHotSerialized, written.fpBanksCommon);
		assertTrue(bits.banksHot[1][sliceBucket("ab")].nextSetBit(0) >= 0
				|| bits.banksCommon[1][sliceBucket("ab")].nextSetBit(0) >= 0);
	}

	@Test
	void readfromBanksSelective_loadsOnlyChosenBucket() throws Exception {
		final FpGroupHotNgramBitIndex bits = buildSmallBitIndex();
		final Directory dir = new RAMDirectory();
		final FpBlockInfo info;
		try (IndexOutput out = dir.createOutput("bits", IOContext.DEFAULT)) {
			info = bits.flushto(out,"");
		}
		final boolean[][] choose = new boolean[Lucene80FPSearchConfig.NGRAM_MAX][Lucene80FPSearchConfig.BUCKETS];
		for (boolean[] row : choose) {
			Arrays.fill(row, false);
		}
		final int li = 1;
		final int bucket = sliceBucket("ab");
		choose[li][bucket] = true;

		try (IndexInput in = dir.openInput("bits", IOContext.DEFAULT)) {
			final FpGroupHotNgramBitIndex partial = FpGroupHotNgramBitIndex.readfromBanksSelective(in, info, choose,
					choose);
			assertNotNull(partial.banksHot[li][bucket]);
			assertTrue(partial.banksHot[li][bucket].nextSetBit(0) >= 0);
			assertTrue(partial.banksHot[0][sliceBucket("x")] == null);
		}
	}

	private static FpGroupDataRebuild newSearchScenarioGroup() {
		final FpGroupDataRebuild group = new FpGroupDataRebuild(32);
		group.commonTermMapInternal().put(FpTermKey.copyOf(new BytesRef("abab".getBytes())), docList(1, 2));
		group.commonTermMapInternal().put(FpTermKey.copyOf(new BytesRef("xyzz".getBytes())), docList(3));
		return group;
	}

	private static FpGroupHotNgramBitIndex buildSmallBitIndex() throws Exception {
		final FpGroupDataRebuild group = newSearchScenarioGroup();
		FpGroupHotNgramRebuild.execute(group, null, 2);
		return FpGroupHotNgramBitIndex.execute((byte) 1, group);
	}

	private static FPDocList docList(int... docs) {
		final FPDocList list = new FPDocList(32);
		for (int d : docs) {
			list.addDoc(d);
		}
		return list;
	}

	private static int sliceBucket(String s) {
		final BytesRef ref = new BytesRef(s.getBytes());
		return FpGroupHotNgramBitIndex.bucketIndex(ref.bytes, ref.offset, ref.length);
	}

}
