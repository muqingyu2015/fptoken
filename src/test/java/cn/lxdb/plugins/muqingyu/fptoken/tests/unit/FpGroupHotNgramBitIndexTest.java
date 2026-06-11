package cn.lxdb.plugins.muqingyu.fptoken.tests.unit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.TreeMap;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.BytesRef;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import cn.lxdb.plugins.muqingyu.fptoken.dataset.block.FpBitIndexSegmentStaging;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.block.FpBitIndexTempDirectory;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.block.FpGroupDataRebuild;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.block.FpGroupHotNgramBitIndex;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FPDocList;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpBlockInfo;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpTermKey;

@Tag("lxdb-runtime")
class FpGroupHotNgramBitIndexTest {

	@Test
	void flushReadRoundTrip_andSelectiveLookup() throws Exception {
		final FpGroupDataRebuild group = new FpGroupDataRebuild(100);
		final FpTermKey hotKey = FpTermKey.copyOf(new BytesRef(new byte[] { 'a', 'b', 'c' }));
		final FpTermKey commonKey = FpTermKey.copyOf(new BytesRef(new byte[] { 'x', 'y' }));
		final FPDocList docs = new FPDocList(100);
		docs.addDoc(1);
		group.hotTermToDocs.put(hotKey, docs);
		group.hotTermToOrder.put(hotKey, 7);
		group.commonTermToDocs.put(commonKey, docs);
		group.commonTermToOrder.put(commonKey, 11);

		final FpGroupHotNgramBitIndex built = FpGroupHotNgramBitIndex.execute(3, group);
		final BytesRef hotSlice = hotKey.bytesRef();
		final BytesRef commonSlice = commonKey.bytesRef();

		final Directory dir = new RAMDirectory();
		final FpBlockInfo info;
		try (IndexOutput out = dir.createOutput("bits", IOContext.DEFAULT)) {
			info = built.flushto(out, "test", new BytesRef("d"), 10);
		}

		try (IndexInput in = dir.openInput("bits", IOContext.DEFAULT)) {
			final FpGroupHotNgramBitIndex full = FpGroupHotNgramBitIndex.readfrom(in, info);
			assertTrue(full.lookupHotOrders(hotSlice).length > 0);
			assertTrue(full.lookupCommonOrders(commonSlice).length > 0);

			final long hotKeyPacked = FpGroupHotNgramBitIndex.packBucketKey(hotSlice.length - 1,
					FpGroupHotNgramBitIndex.bucketIndex(hotSlice));
			final long commonKeyPacked = FpGroupHotNgramBitIndex.packBucketKey(commonSlice.length - 1,
					FpGroupHotNgramBitIndex.bucketIndex(commonSlice));
			final FpGroupHotNgramBitIndex sparse = FpGroupHotNgramBitIndex.readfromBanksSelective(in, info,
					new long[] { hotKeyPacked }, new long[] { commonKeyPacked });
			assertArrayEquals(full.lookupHotOrders(hotSlice), sparse.lookupHotOrders(hotSlice));
			assertArrayEquals(full.lookupCommonOrders(commonSlice), sparse.lookupCommonOrders(commonSlice));
		}
	}

	@Test
	void v4StageMergeRoundTrip_andSelectiveLookup() throws Exception {
		final FpGroupDataRebuild group1 = new FpGroupDataRebuild(100);
		final FpTermKey hotKey1 = FpTermKey.copyOf(new BytesRef(new byte[] { 'a', 'b', 'c' }));
		final FPDocList docs = new FPDocList(100);
		docs.addDoc(1);
		group1.hotTermToDocs.put(hotKey1, docs);
		group1.hotTermToOrder.put(hotKey1, 7);
		group1.commonTermToDocs.put(FpTermKey.copyOf(new BytesRef(new byte[] { 'x', 'y' })), docs);
		group1.commonTermToOrder.put(FpTermKey.copyOf(new BytesRef(new byte[] { 'x', 'y' })), 11);

		final FpGroupDataRebuild group2 = new FpGroupDataRebuild(100);
		final FpTermKey hotKey2 = FpTermKey.copyOf(new BytesRef(new byte[] { 'd', 'e', 'f', 'g', 'h', 'i' }));
		group2.hotTermToDocs.put(hotKey2, docs);
		group2.hotTermToOrder.put(hotKey2, 3);

		final FpGroupHotNgramBitIndex built1 = FpGroupHotNgramBitIndex.execute(3, group1);
		final FpGroupHotNgramBitIndex built2 = FpGroupHotNgramBitIndex.execute(3, group2);

		final Directory dir = new RAMDirectory();
		final TreeMap<Integer, FpBlockInfo> blocks = new TreeMap<>();
		try (FpBitIndexTempDirectory.Session session = FpBitIndexTempDirectory.INS.openSession("test");
				FpBitIndexSegmentStaging staging = new FpBitIndexSegmentStaging(session)) {
			staging.stage(1, built1, "test", new BytesRef("d"), 10);
			staging.stage(2, built2, "test", new BytesRef("d"), 10);
			try (IndexOutput out = dir.createOutput("bits", IOContext.DEFAULT)) {
				staging.finalizeTo(out, blocks);
			}
		}

		assertEquals(2, blocks.size());
		try (IndexInput in = dir.openInput("bits", IOContext.DEFAULT)) {
			final FpBlockInfo info1 = blocks.get(1);
			final FpBlockInfo info2 = blocks.get(2);
			final FpGroupHotNgramBitIndex full1 = FpGroupHotNgramBitIndex.readfrom(in, info1);
			final FpGroupHotNgramBitIndex full2 = FpGroupHotNgramBitIndex.readfrom(in, info2);
			assertTrue(full1.lookupHotOrders(hotKey1.bytesRef()).length > 0);
			assertTrue(full2.lookupHotOrders(hotKey2.bytesRef()).length > 0);

			final long hotKeyPacked = FpGroupHotNgramBitIndex.packBucketKey(hotKey2.bytesRef().length - 1,
					FpGroupHotNgramBitIndex.bucketIndex(hotKey2.bytesRef()));
			final FpGroupHotNgramBitIndex sparse2 = FpGroupHotNgramBitIndex.readfromBanksSelective(in, info2,
					new long[] { hotKeyPacked }, null);
			assertArrayEquals(full2.lookupHotOrders(hotKey2.bytesRef()), sparse2.lookupHotOrders(hotKey2.bytesRef()));
		}
	}

	@Test
	void bucketIndex_shortNgrams_packBytes_longNgrams_useHash() {
		assertEquals(0x61, FpGroupHotNgramBitIndex.bucketIndex(new byte[] { 'a' }, 0, 1));
		assertEquals(0x6162, FpGroupHotNgramBitIndex.bucketIndex(new byte[] { 'a', 'b' }, 0, 2));
		assertEquals(0x010203, FpGroupHotNgramBitIndex.bucketIndex(new byte[] { 1, 2, 3 }, 0, 3));
		assertEquals(0x01020304, FpGroupHotNgramBitIndex.bucketIndex(new byte[] { 1, 2, 3, 4 }, 0, 4));

		final byte[] five = new byte[] { 1, 2, 3, 4, 5 };
		final int hash = FpGroupHotNgramBitIndex.bucketIndex(five, 0, 5);
		assertEquals(hash, FpGroupHotNgramBitIndex.bucketIndex(five, 0, 5));
		assertEquals(0x01020304, FpGroupHotNgramBitIndex.bucketIndex(five, 0, 4));
	}
}
