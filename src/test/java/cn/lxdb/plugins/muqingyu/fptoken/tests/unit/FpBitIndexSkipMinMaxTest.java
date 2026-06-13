package cn.lxdb.plugins.muqingyu.fptoken.tests.unit;

import static cn.lxdb.plugins.muqingyu.fptoken.tests.support.FpBitIndexTestSupport.buildBitIndexFromCommonPayloads;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Method;
import java.util.List;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RAMDirectory;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import cn.lxdb.plugins.muqingyu.fptoken.dataset.block.FpBitIndexSegmentStaging;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.block.FpGroupHotNgramBitIndex;

@Tag("lxdb-runtime")
class FpBitIndexSkipMinMaxTest {

	@Test
	void stageToTemp_writesSkipWithMinMax() throws Exception {
		final FpGroupHotNgramBitIndex built = buildBitIndexFromCommonPayloads(List.of("abcdef", "abzzzz"));
		final Directory dir = new RAMDirectory();

		final Method stageToTemp = FpGroupHotNgramBitIndex.class.getDeclaredMethod("stageToTemp", Directory.class);
		stageToTemp.setAccessible(true);
		stageToTemp.invoke(built, dir);

		final String skipName = "common_skip_1.dat";
		assertTrue(List.of(dir.listAll()).contains(skipName));

		try (IndexInput in = dir.openInput(skipName, IOContext.READ)) {
			final int entryCount = in.readInt();
			assertTrue(entryCount > 0);
			final int min = in.readInt();
			final int max = in.readInt();
			in.readLong();
			assertTrue(min <= max, "skip 段 min/max 应覆盖 sortedKeys 区间");
		}

		final String orderName = "common_order_1.dat";
		assertTrue(List.of(dir.listAll()).contains(orderName));
		assertEquals("common_order_1.dat", orderName);

		assertTrue(List.of(dir.listAll()).contains("hot_tier_dir.dat"));
		assertTrue(List.of(dir.listAll()).contains("common_tier_dir.dat"));

		try (IndexInput tierDir = dir.openInput("common_tier_dir.dat", IOContext.READ)) {
			assertEquals(FpBitIndexSegmentStaging.TIER_DIR_MAGIC_COMMON, tierDir.readInt());
			long virtualPos = 0L;
			for (int lenIdx = 0; lenIdx < 5; lenIdx++) {
				final String skipName2 = "common_skip_" + lenIdx + ".dat";
				final String keysName = "common_keys_" + lenIdx + ".dat";
				final String orderName2 = "common_order_" + lenIdx + ".dat";
				final long skipSize = fileSize(dir, skipName2);
				final long keysSize = fileSize(dir, keysName);
				final long orderSize = fileSize(dir, orderName2);
				if (skipSize == 0 && keysSize == 0 && orderSize == 0) {
					assertEquals(0L, tierDir.readLong());
					assertEquals(0L, tierDir.readLong());
					assertEquals(0L, tierDir.readLong());
					continue;
				}
				assertEquals(virtualPos, tierDir.readLong());
				virtualPos += skipSize;
				assertEquals(virtualPos, tierDir.readLong());
				virtualPos += keysSize;
				final long orderOff = tierDir.readLong();
				if (orderSize > 0) {
					assertEquals(virtualPos, orderOff);
					virtualPos += orderSize;
				} else {
					assertEquals(0L, orderOff);
				}
			}
		}
	}

	private static long fileSize(Directory dir, String name) throws Exception {
		if (!List.of(dir.listAll()).contains(name)) {
			return 0L;
		}
		try (IndexInput in = dir.openInput(name, IOContext.READ)) {
			return in.length();
		}
	}
}
