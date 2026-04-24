package cn.lxdb.plugins.muqingyu.fptoken.tests.functional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import cn.lxdb.plugins.muqingyu.fptoken.ExclusiveFrequentItemsetSelector;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.ExclusiveSelectionResult;
import cn.lxdb.plugins.muqingyu.fptoken.runner.dataset.LineRecordDatasetLoader;
import cn.lxdb.plugins.muqingyu.fptoken.tests.ByteArrayTestSupport;
import cn.lxdb.plugins.muqingyu.fptoken.tests.support.FileDatasetTestSupport;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceAccessMode;
import org.junit.jupiter.api.parallel.ResourceLock;

/**
 * 标准化文件输入 + 抽样稳定性功能回归（默认执行）。
 */
@ResourceLock(value = "ExclusiveFrequentItemsetSelector.runtimeTuning", mode = ResourceAccessMode.READ_WRITE)
class FileSamplingStandardizationFunctionalTest {

    @Test
    void fileSizeMatrix_loaderShouldRespectCapsAndLengthConstraints() throws Exception {
        Path dir = Files.createTempDirectory("fptoken-file-matrix-");
        Path small = dir.resolve("small.txt");
        Path medium = dir.resolve("medium.txt");
        Path large = dir.resolve("large.txt");
        Path cap = dir.resolve("cap.txt");
        FileDatasetTestSupport.writeRecordFile(small, 64, "S", 16, 6, 0);
        FileDatasetTestSupport.writeRecordFile(medium, 512, "M", 16, 9, 0);
        FileDatasetTestSupport.writeRecordFile(large, 1500, "L", 16, 11, 0);
        FileDatasetTestSupport.writeRecordFile(cap, 32040, "C", 64, 13, 23);

        assertSingleFileStats(small, 64L, 0L, 0L);
        assertSingleFileStats(medium, 512L, 0L, 0L);
        assertSingleFileStats(large, 1500L, 0L, 0L);
        assertSingleFileStats(cap, 32000L, 40L, 0L);
    }

    @Test
    void samplingParameterMatrix_shouldRunWithoutAnomaly_onFileDataset() throws Exception {
        Path dir = Files.createTempDirectory("fptoken-file-sampling-");
        Path fileA = dir.resolve("a.txt");
        Path fileB = dir.resolve("b.txt");
        Path fileC = dir.resolve("c.txt");
        FileDatasetTestSupport.writeRecordFile(fileA, 300, "A", 24, 7, 0);
        FileDatasetTestSupport.writeRecordFile(fileB, 600, "B", 24, 8, 0);
        FileDatasetTestSupport.writeRecordFile(fileC, 900, "C", 24, 9, 0);

        assertSamplingMatrixPerFile(fileA);
        assertSamplingMatrixPerFile(fileB);
        assertSamplingMatrixPerFile(fileC);
    }

    private static void assertSingleFileStats(
            Path file, long expectLines, long expectDroppedByCap, long minTruncatedLines) throws Exception {
        LineRecordDatasetLoader.LoadOutcome outcome = LineRecordDatasetLoader.loadSingleFileWithStats(file, 2, 4);
        LineRecordDatasetLoader.Stats stats = outcome.getStats();
        assertEquals(1, stats.getFileCount());
        assertEquals(expectLines, stats.getTotalLines());
        assertEquals(expectDroppedByCap, stats.getDroppedByCap());
        assertTrue(stats.getTruncatedLines() >= minTruncatedLines);
        assertEquals((int) stats.getTotalLines(), stats.getDocCount());
    }

    private static void assertSamplingMatrixPerFile(Path file) throws Exception {
        LineRecordDatasetLoader.LoadedDataset loaded = LineRecordDatasetLoader.loadSingleFile(file, 2, 4);
        runWithConfig(loaded, 1.0d, 1, 1.0d);

        ExclusiveSelectionResult ratioZeroFallback = runWithConfig(loaded, 0.0d, 50, 10.0d);
        assertSamplingResultHealthy(ratioZeroFallback);

        ExclusiveSelectionResult sampledAutoScale = runWithConfig(loaded, 0.30d, 80, 0.0d);
        assertSamplingResultHealthy(sampledAutoScale);

        ExclusiveSelectionResult sampledFixedScale = runWithConfig(loaded, 0.45d, 120, 0.8d);
        assertSamplingResultHealthy(sampledFixedScale);
    }

    private static ExclusiveSelectionResult runWithConfig(
            LineRecordDatasetLoader.LoadedDataset loaded,
            double ratio,
            int minSampleCount,
            double supportScale
    ) {
        double oldRatio = ExclusiveFrequentItemsetSelector.getSampleRatio();
        int oldMin = ExclusiveFrequentItemsetSelector.getMinSampleCount();
        double oldScale = ExclusiveFrequentItemsetSelector.getSamplingSupportScale();
        try {
            ExclusiveFrequentItemsetSelector.setSampleRatio(ratio);
            ExclusiveFrequentItemsetSelector.setMinSampleCount(minSampleCount);
            ExclusiveFrequentItemsetSelector.setSamplingSupportScale(supportScale);

            return ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(
                    loaded.getRows(), 40, 2, 4, 120_000);
        } finally {
            ExclusiveFrequentItemsetSelector.setSampleRatio(oldRatio);
            ExclusiveFrequentItemsetSelector.setMinSampleCount(oldMin);
            ExclusiveFrequentItemsetSelector.setSamplingSupportScale(oldScale);
        }
    }

    private static void assertSamplingResultHealthy(ExclusiveSelectionResult result) {
        assertTrue(result.getCandidateCount() >= 0);
        assertTrue(result.getFrequentTermCount() >= 0);
        assertTrue(ByteArrayTestSupport.pairwiseTermsDisjoint(result.getGroups()),
                "selected groups must remain mutually exclusive");
    }
}
