package cn.lxdb.plugins.muqingyu.fptoken.tests.unit;

import static cn.lxdb.plugins.muqingyu.fptoken.tests.support.FpBitIndexTestSupport.buildBitIndexFromCommonPayloads;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RAMDirectory;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import cn.lxdb.plugins.muqingyu.fptoken.dataset.block.FpBitIndexSegmentStaging;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.block.FpLenRowBloom;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.block.FpGroupHotNgramBitIndex;

@Tag("lxdb-runtime")
class FpBitIndexBloomStagingTest {

	@Test
	void stageToTemp_writesBloomPools_andFiltersAbsentBucket() throws Exception {
		final FpGroupHotNgramBitIndex built = buildBitIndexFromCommonPayloads(List.of("abcdef", "abzzzz"));
		final Directory dir = new RAMDirectory();

		final Method stageToTemp = FpGroupHotNgramBitIndex.class.getDeclaredMethod("stageToTemp", Directory.class);
		stageToTemp.setAccessible(true);
		stageToTemp.invoke(built, dir);

		final Method fileName = FpBitIndexSegmentStaging.class.getDeclaredMethod("fileName", String.class, String.class,
				int.class);
		fileName.setAccessible(true);
		final String bloomName = (String) fileName.invoke(null, "common", "bloom", 1);
		assertTrue(Arrays.asList(dir.listAll()).contains(bloomName), "应写出 common_bloom_1.dat");

		try (IndexInput in = dir.openInput(bloomName, IOContext.READ)) {
			final FpLenRowBloom bloom = FpLenRowBloom.readFrom(in);
			assertFalse(bloom.mightContain(0x12345678), "Bloom 应过滤不在 sortedKeys 中的 bucket");
		}
	}
}
