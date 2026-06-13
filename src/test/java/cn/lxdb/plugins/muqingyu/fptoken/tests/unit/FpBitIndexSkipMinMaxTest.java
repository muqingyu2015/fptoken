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
	void stageToTemp_writesTwoLevelSkipWithMinMax() throws Exception {
		final FpGroupHotNgramBitIndex built = buildBitIndexFromCommonPayloads(List.of("abcdef", "abzzzz"));
		final Directory dir = new RAMDirectory();

		final Method stageToTemp = FpGroupHotNgramBitIndex.class.getDeclaredMethod("stageToTemp", Directory.class);
		stageToTemp.setAccessible(true);
		stageToTemp.invoke(built, dir);

		final String skip1Name = "common_skip1_1.dat";
		final String skip2Name = "common_skip2_1.dat";
		assertTrue(List.of(dir.listAll()).contains(skip1Name));
		assertTrue(List.of(dir.listAll()).contains(skip2Name));

		try (IndexInput skip1 = dir.openInput(skip1Name, IOContext.READ)) {
			final int entryCount = skip1.readInt();
			assertTrue(entryCount > 0);
			final int min = skip1.readInt();
			final int max = skip1.readInt();
			final long skip2Off = skip1.readLong();
			assertTrue(min <= max);
			assertEquals(0L, skip2Off);
		}

		try (IndexInput skip2 = dir.openInput(skip2Name, IOContext.READ)) {
			final int min = skip2.readInt();
			final int max = skip2.readInt();
			skip2.readLong();
			assertTrue(min <= max);
		}

		assertTrue(List.of(dir.listAll()).contains("hot_tier_dir.dat"));
		assertTrue(List.of(dir.listAll()).contains("common_tier_dir.dat"));

		try (IndexInput tierDir = dir.openInput("common_tier_dir.dat", IOContext.READ)) {
			assertEquals(FpBitIndexSegmentStaging.TIER_DIR_MAGIC_COMMON, tierDir.readInt());
			long virtualPos = 0L;
			for (int lenIdx = 0; lenIdx < 5; lenIdx++) {
				final long skip1Size = fileSize(dir, "common_skip1_" + lenIdx + ".dat");
				final long skip2Size = fileSize(dir, "common_skip2_" + lenIdx + ".dat");
				final long keysSize = fileSize(dir, "common_keys_" + lenIdx + ".dat");
				final long orderSize = fileSize(dir, "common_order_" + lenIdx + ".dat");
				if (skip1Size == 0 && skip2Size == 0 && keysSize == 0 && orderSize == 0) {
					assertEquals(0L, tierDir.readLong());
					assertEquals(0L, tierDir.readLong());
					assertEquals(0L, tierDir.readLong());
					assertEquals(0L, tierDir.readLong());
					continue;
				}
				assertEquals(virtualPos, tierDir.readLong());
				virtualPos += skip1Size;
				assertEquals(virtualPos, tierDir.readLong());
				virtualPos += skip2Size;
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
