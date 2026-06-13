package cn.lxdb.plugins.muqingyu.fptoken.tests.unit;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.lang.reflect.Method;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import cn.lxdb.plugins.muqingyu.fptoken.config.Lucene80FPSearchConfig;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.block.FpBitIndexSegmentStaging;

@Tag("lxdb-runtime")
class FpBitIndexSegmentStagingStaticTest {

	@Test
	void segmentAndTierMagicConstants() {
		assertEquals(0x4650544B, FpBitIndexSegmentStaging.SEGMENT_MAGIC);
		assertEquals(0x46505448, FpBitIndexSegmentStaging.TIER_DIR_MAGIC_HOT);
		assertEquals(0x46505443, FpBitIndexSegmentStaging.TIER_DIR_MAGIC_COMMON);
	}

	@Test
	void tierDirectorySerializedBytes_v6() {
		final int expected = 4 + Lucene80FPSearchConfig.NGRAM_MAX * FpBitIndexSegmentStaging.TIER_DIR_LONGS_PER_LEN
				* Long.BYTES;
		assertEquals(expected, FpBitIndexSegmentStaging.tierDirectorySerializedBytes());
		assertEquals(3, FpBitIndexSegmentStaging.TIER_DIR_LONGS_PER_LEN);
	}

	@Test
	void fileName_and_partPath() throws Exception {
		Method fileName = FpBitIndexSegmentStaging.class.getDeclaredMethod("fileName", String.class, String.class,
				int.class);
		fileName.setAccessible(true);
		assertEquals("hot_skip_3.dat", fileName.invoke(null, "hot", "skip", 3));
		assertEquals("common_keys_2.dat", fileName.invoke(null, "common", "keys", 2));
		assertEquals("common_order_4.dat", fileName.invoke(null, "common", "order", 4));

		Method partPath = FpBitIndexSegmentStaging.class.getDeclaredMethod("partPath", Path.class, String.class,
				String.class, int.class);
		partPath.setAccessible(true);
		Path group = Paths.get("/tmp/g_1");
		assertEquals(group.resolve("common_order_0.dat"), partPath.invoke(null, group, "common", "order", 0));
	}
}
