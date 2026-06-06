package cn.lxdb.plugins.muqingyu.fptoken.tests.performance;

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.BytesRef;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import cn.lxdb.plugins.muqingyu.fptoken.api.FpSearch;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.block.FpGroupDataRebuild;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.block.FpGroupHotNgramBitIndex;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpBlockInfo;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpSearchStat;
import cn.lxdb.plugins.muqingyu.fptoken.tests.support.FpTestColumnNames;
import cn.lxdb.plugins.muqingyu.fptoken.tests.support.FpTestFixtures;
import cn.lxdb.plugins.muqingyu.fptoken.tests.support.FpTestIndexBuilder;

/**
 * 性能冒烟：默认不跑；启用 {@code .\scripts\run-fptoken-tests.ps1 -Perf}。
 */
@Tag("performance")
@EnabledIfSystemProperty(named = "fptoken.runPerfTests", matches = "true")
class FpBitIndexPerfTest {

	@Test
	void selectiveRead_isFasterThanFullRead_onLargeIndex() throws Exception {
		final FpGroupDataRebuild group = new FpGroupDataRebuild(4096);
		for (int i = 0; i < 200; i++) {
			FpTestFixtures.putCommonTerm(group, "payload" + i + "ab", i);
		}
		final FpTestIndexBuilder.BuiltIndex built = FpTestIndexBuilder.buildFromRebuildGroup(group,
				FpTestColumnNames.DEFAULT, 1);

		final Directory dir = new RAMDirectory();
		final FpBlockInfo info;
		try (IndexOutput out = dir.createOutput("bits", IOContext.DEFAULT)) {
			info = built.memoryBitIndex.flushto(out, "perf", FpTestColumnNames.DEFAULT, 200);
		}

		final BytesRef slice = new BytesRef(new byte[] { 'a', 'b' });
		final long[] keys = FpGroupHotNgramBitIndex.selectiveKeysForSlices(new BytesRef[] { slice });

		final int rounds = 50;
		long fullNs = 0;
		long selectiveNs = 0;
		try (IndexInput in = dir.openInput("bits", IOContext.DEFAULT)) {
			for (int r = 0; r < rounds; r++) {
				in.seek(0);
				final long t0 = System.nanoTime();
				FpGroupHotNgramBitIndex.readfrom(in, info);
				fullNs += System.nanoTime() - t0;
			}
			for (int r = 0; r < rounds; r++) {
				in.seek(0);
				final long t0 = System.nanoTime();
				FpGroupHotNgramBitIndex.readfromBanksSelective(in, info, keys, keys);
				selectiveNs += System.nanoTime() - t0;
			}
		}

		System.out.println("[perf] bitIndex fullReadMs=" + (fullNs / rounds / 1_000_000.0) + " selectiveReadMs="
				+ (selectiveNs / rounds / 1_000_000.0));
		assertTrue(selectiveNs <= fullNs, "selective read should not be slower than full read");
	}
}
