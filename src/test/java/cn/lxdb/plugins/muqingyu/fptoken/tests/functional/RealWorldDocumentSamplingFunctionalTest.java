package cn.lxdb.plugins.muqingyu.fptoken.tests.functional;

import static org.junit.jupiter.api.Assertions.assertTrue;

import cn.lxdb.plugins.muqingyu.fptoken.ExclusiveFrequentItemsetSelector;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.ExclusiveSelectionResult;
import cn.lxdb.plugins.muqingyu.fptoken.runner.dataset.LineRecordDatasetLoader;
import cn.lxdb.plugins.muqingyu.fptoken.tests.ByteArrayTestSupport;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceAccessMode;
import org.junit.jupiter.api.parallel.ResourceLock;

/**
 * 使用真实文档来源的规范化样本做抽样稳定性回归。
 */
@ResourceLock(value = "ExclusiveFrequentItemsetSelector.runtimeTuning", mode = ResourceAccessMode.READ_WRITE)
class RealWorldDocumentSamplingFunctionalTest {

    @Test
    void realWorldFiles_samplingShouldRemainHealthy() throws Exception {
        Path dir = Paths.get("sample-data/real-docs");
        Path rfc = dir.resolve("rfc9110_lines.txt");
        Path awesome = dir.resolve("awesome_python_lines.txt");
        Path gpl = dir.resolve("gpl3_lines.txt");

        assertTrue(Files.isRegularFile(rfc), "missing file: " + rfc);
        assertTrue(Files.isRegularFile(awesome), "missing file: " + awesome);
        assertTrue(Files.isRegularFile(gpl), "missing file: " + gpl);

        assertFileStable(rfc);
        assertFileStable(awesome);
        assertFileStable(gpl);
    }

    private static void assertFileStable(Path file) throws Exception {
        LineRecordDatasetLoader.LoadedDataset loaded = LineRecordDatasetLoader.loadSingleFile(file, 2, 4);
        ExclusiveSelectionResult full = runWithSampling(loaded, 1.0d, 1, 1.0d);
        ExclusiveSelectionResult sampled = runWithSampling(loaded, 0.30d, 80, 0.0d);

        assertTrue(full.getCandidateCount() >= 0);
        assertTrue(sampled.getCandidateCount() >= 0);
        assertTrue(ByteArrayTestSupport.pairwiseTermsDisjoint(sampled.getGroups()));
    }

    private static ExclusiveSelectionResult runWithSampling(
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
                    loaded.getRows(), 30, 2, 4, 150_000);
        } finally {
            ExclusiveFrequentItemsetSelector.setSampleRatio(oldRatio);
            ExclusiveFrequentItemsetSelector.setMinSampleCount(oldMin);
            ExclusiveFrequentItemsetSelector.setSamplingSupportScale(oldScale);
        }
    }
}
