package cn.lxdb.plugins.muqingyu.fptoken.tests.performance;

import static org.junit.jupiter.api.Assertions.assertTrue;

import cn.lxdb.plugins.muqingyu.fptoken.ExclusiveFrequentItemsetSelector;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.ExclusiveSelectionResult;
import cn.lxdb.plugins.muqingyu.fptoken.runner.dataset.LineRecordDatasetLoader;
import cn.lxdb.plugins.muqingyu.fptoken.tests.support.FileDatasetTestSupport;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

/**
 * 标准化文件输入 + 抽样参数矩阵性能测试（仅 Perf 开关开启时执行）。
 */
@Tag("performance")
@EnabledIfSystemProperty(named = "fptoken.runPerfTests", matches = "true")
@Timeout(value = 60, unit = TimeUnit.SECONDS)
class FileSamplingStandardizationPerformanceTest {

    @Test
    void PERF_FILE_SAMPLE_001_parameterSweep_shouldStayWithinBudget() throws Exception {
        Path dir = Files.createTempDirectory("fptoken-perf-file-sampling-");
        Path p1 = dir.resolve("p1.txt");
        Path p2 = dir.resolve("p2.txt");
        Path p3 = dir.resolve("p3.txt");
        FileDatasetTestSupport.writeRecordFile(p1, 4000, "P1", 48, 11, 0);
        FileDatasetTestSupport.writeRecordFile(p2, 8000, "P2", 48, 13, 0);
        FileDatasetTestSupport.writeRecordFile(p3, 12000, "P3", 48, 17, 0);

        long budgetMs = PerfTestSupport.longProp("fptoken.perf.file.sample.maxMs", 12000L);
        long autoScaleMs = runOnFile(p1, 0.30d, 80, 0.0d)
                + runOnFile(p2, 0.30d, 80, 0.0d)
                + runOnFile(p3, 0.30d, 80, 0.0d);
        long fixedScaleMs = runOnFile(p1, 0.45d, 120, 0.9d)
                + runOnFile(p2, 0.45d, 120, 0.9d)
                + runOnFile(p3, 0.45d, 120, 0.9d);
        long baselineAllSampledMs = runOnFile(p1, 1.0d, 1, 1.0d)
                + runOnFile(p2, 1.0d, 1, 1.0d)
                + runOnFile(p3, 1.0d, 1, 1.0d);

        assertTrue(autoScaleMs < budgetMs, () -> "autoScaleMs=" + autoScaleMs + ", budgetMs=" + budgetMs);
        assertTrue(fixedScaleMs < budgetMs, () -> "fixedScaleMs=" + fixedScaleMs + ", budgetMs=" + budgetMs);
        assertTrue(baselineAllSampledMs < budgetMs,
                () -> "baselineAllSampledMs=" + baselineAllSampledMs + ", budgetMs=" + budgetMs);

        long extraSlackMs = PerfTestSupport.longProp("fptoken.perf.file.sample.slackMs", 3000L);
        assertTrue(autoScaleMs <= baselineAllSampledMs + extraSlackMs,
                () -> "expected sampled path not much slower than baseline; sampled="
                        + autoScaleMs + ", baseline=" + baselineAllSampledMs + ", slack=" + extraSlackMs);
    }

    private static long runOnFile(
            Path file,
            double ratio,
            int minSampleCount,
            double supportScale
    ) throws Exception {
        LineRecordDatasetLoader.LoadedDataset loaded = LineRecordDatasetLoader.loadSingleFile(file, 2, 4);
        double oldRatio = ExclusiveFrequentItemsetSelector.getSampleRatio();
        int oldMin = ExclusiveFrequentItemsetSelector.getMinSampleCount();
        double oldScale = ExclusiveFrequentItemsetSelector.getSamplingSupportScale();
        try {
            ExclusiveFrequentItemsetSelector.setSampleRatio(ratio);
            ExclusiveFrequentItemsetSelector.setMinSampleCount(minSampleCount);
            ExclusiveFrequentItemsetSelector.setSamplingSupportScale(supportScale);

            final ExclusiveSelectionResult[] holder = new ExclusiveSelectionResult[1];
            long elapsedMs = PerfTestSupport.elapsedMillis(() -> holder[0] =
                    ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(
                            loaded.getRows(), 80, 2, 4, 200_000));
            assertTrue(holder[0] != null);
            assertTrue(holder[0].getCandidateCount() >= 0);
            return elapsedMs;
        } finally {
            ExclusiveFrequentItemsetSelector.setSampleRatio(oldRatio);
            ExclusiveFrequentItemsetSelector.setMinSampleCount(oldMin);
            ExclusiveFrequentItemsetSelector.setSamplingSupportScale(oldScale);
        }
    }
}
