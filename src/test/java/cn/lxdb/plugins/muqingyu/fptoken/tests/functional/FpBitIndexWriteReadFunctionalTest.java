package cn.lxdb.plugins.muqingyu.fptoken.tests.functional;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.BytesRef;
import org.junit.jupiter.api.Test;

import cn.lxdb.plugins.muqingyu.fptoken.dataset.block.FpGroupDataRebuild;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.block.FpGroupHotNgramBitIndex;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpBlockInfo;
import cn.lxdb.plugins.muqingyu.fptoken.tests.support.FpTestColumnNames;
import cn.lxdb.plugins.muqingyu.fptoken.tests.support.FpTestFixtures;
import cn.lxdb.plugins.muqingyu.fptoken.tests.support.FpTestIndexBuilder;

/**
 * 索引写读链路：rebuild → bit index → flush → readfrom / selective read。
 */
class FpBitIndexWriteReadFunctionalTest {

	@Test
	void rebuild_flush_fullRead_matchesMemoryLookup() throws Exception {
		final FpGroupDataRebuild group = buildSampleGroup();
		final FpTestIndexBuilder.BuiltIndex built = FpTestIndexBuilder.buildFromRebuildGroup(group,
				FpTestColumnNames.DEFAULT, FpTestIndexBuilder.DEFAULT_GROUP_ID);

		final BytesRef slice = new BytesRef(new byte[] { 'a', 'b' });
		assertArrayEquals(built.memoryBitIndex.lookupHotOrders(slice), built.diskBitIndex.lookupHotOrders(slice));
		assertTrue(built.blockInfo.fieldInfo.equals(FpTestColumnNames.DEFAULT));
		assertEquals(FpBlockInfo.FORMAT_VERSION, 3);
	}

	@Test
	void flush_selectiveRead_matchesFullRead() throws Exception {
		final FpGroupDataRebuild group = buildSampleGroup();
		final FpTestIndexBuilder.BuiltIndex built = FpTestIndexBuilder.buildFromRebuildGroup(group,
				FpTestColumnNames.DEFAULT, 2);

		final Directory dir = new RAMDirectory();
		final FpBlockInfo info;
		try (IndexOutput out = dir.createOutput("bits", IOContext.DEFAULT)) {
			info = built.memoryBitIndex.flushto(out, "functional", FpTestColumnNames.DEFAULT, 50);
		}

		final BytesRef hotSlice = new BytesRef(new byte[] { 'a', 'b' });
		final BytesRef commonSlice = new BytesRef(new byte[] { 'z', 'z', 'o' });
		final long hotKey = FpGroupHotNgramBitIndex.packBucketKey(hotSlice.length - 1,
				FpGroupHotNgramBitIndex.bucketIndex(hotSlice));
		final long commonKey = FpGroupHotNgramBitIndex.packBucketKey(commonSlice.length - 1,
				FpGroupHotNgramBitIndex.bucketIndex(commonSlice));

		try (IndexInput in = dir.openInput("bits", IOContext.DEFAULT)) {
			final FpGroupHotNgramBitIndex full = FpGroupHotNgramBitIndex.readfrom(in, info);
			in.seek(0);
			final FpGroupHotNgramBitIndex sparse = FpGroupHotNgramBitIndex.readfromBanksSelective(in, info,
					new long[] { hotKey }, new long[] { commonKey });

			assertArrayEquals(full.lookupHotOrders(hotSlice), sparse.lookupHotOrders(hotSlice));
			assertArrayEquals(full.lookupCommonOrders(commonSlice), sparse.lookupCommonOrders(commonSlice));
			assertTrue(sparse.isSparse());
		}
	}

	@Test
	void flush_selectiveRead_matchesFullRead_whenLenRowExceedsSkipInterval() throws Exception {
		// SKIP_INTERVAL=128：旧 readOrdersForBucket 在第二段及以后会把 keys/meta 偏移算错
		final FpGroupDataRebuild group = new FpGroupDataRebuild(512);
		for (int i = 0; i < 200; i++) {
			FpTestFixtures.putCommonTerm(group, String.format("%03d_padding", i), i);
		}
		group.rebuildCommonTermToOrderFromHotDocs();
		final FpGroupHotNgramBitIndex memory = FpGroupHotNgramBitIndex.execute(2, group);

		final Directory dir = new RAMDirectory();
		final FpBlockInfo info;
		try (IndexOutput out = dir.createOutput("bits", IOContext.DEFAULT)) {
			info = memory.flushto(out, "functional-skip", FpTestColumnNames.DEFAULT, 200);
		}

		final BytesRef slice = new BytesRef("000".getBytes(java.nio.charset.StandardCharsets.US_ASCII));
		final long key = FpGroupHotNgramBitIndex.packBucketKey(slice.length - 1,
				FpGroupHotNgramBitIndex.bucketIndex(slice));

		try (IndexInput in = dir.openInput("bits", IOContext.DEFAULT)) {
			final FpGroupHotNgramBitIndex full = FpGroupHotNgramBitIndex.readfrom(in, info);
			in.seek(0);
			final FpGroupHotNgramBitIndex sparse = FpGroupHotNgramBitIndex.readfromBanksSelective(in, info,
					new long[] { key }, new long[] { key });

			assertTrue(full.lookupCommonOrders(slice).length > 0,
					"fixture must place >128 buckets in one len row");
			assertArrayEquals(full.lookupCommonOrders(slice), sparse.lookupCommonOrders(slice));
		}
	}

	@Test
	void fpBlockInfo_roundTrip_preservesTierOffsets() throws Exception {
		final FpGroupDataRebuild group = buildSampleGroup();
		final FpTestIndexBuilder.BuiltIndex built = FpTestIndexBuilder.buildFromRebuildGroup(group,
				FpTestColumnNames.DEFAULT, 3);

		final Directory dir = new RAMDirectory();
		try (IndexOutput metaOut = dir.createOutput("meta", IOContext.DEFAULT)) {
			built.blockInfo.writeto(metaOut);
		}
		final FpBlockInfo copy = new FpBlockInfo();
		try (IndexInput metaIn = dir.openInput("meta", IOContext.DEFAULT)) {
			copy.readfrom(metaIn);
		}

		assertEquals(built.blockInfo.fpBanksHot, copy.fpBanksHot);
		assertEquals(built.blockInfo.fpBanksCommon, copy.fpBanksCommon);
		assertEquals(built.blockInfo.hotCount, copy.hotCount);
		assertEquals(built.blockInfo.commonCount, copy.commonCount);
		assertEquals(built.blockInfo.targetLevel, copy.targetLevel);
	}

	private static FpGroupDataRebuild buildSampleGroup() {
		final FpGroupDataRebuild group = new FpGroupDataRebuild(128);
		FpTestFixtures.putCommonTermsSharingNgram(group, "ab", 20);
		FpTestFixtures.putCommonTerm(group, "zzonly", 7, 8);
		FpTestFixtures.putCommonTerm(group, "other", 10);
		return group;
	}
}
