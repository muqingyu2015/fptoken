package cn.lxdb.plugins.muqingyu.fptoken.tests.performance;

import static org.junit.jupiter.api.Assertions.assertTrue;

import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.DocTerms;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

@Tag("performance")
@EnabledIfSystemProperty(named = "fptoken.runPerfTests", matches = "true")
@Timeout(value = 10, unit = TimeUnit.SECONDS)
class SpecificScenarioCompressibilityTest {

    @Test
    void testHttpHeaderLikeDataCompressibility() {
        List<DocTerms> rows = CompressibilityAssessmentFixtures.httpHeaderLikeDataset(9000, 20260427L);
        int[] maxItemsetSizes = new int[] {4, 6, 8, 10};

        int bestLongest = 0;
        double bestRatio = -1d;
        for (int maxSize : maxItemsetSizes) {
            CompressibilityPipelineSupport.PipelineResult result = CompressibilityPipelineSupport.run(
                    rows, 120, 3, maxSize, 200000, 4096, 24, 64, 120);
            CompressionMetricsUtil.CompressionMetrics m = result.getMetrics();
            bestLongest = Math.max(bestLongest, m.getMaxItemsetLength());
            bestRatio = Math.max(bestRatio, m.getCompressionRatio());
            System.out.println("http maxItemsetSize=" + maxSize
                    + ", selected=" + m.getSelectedItemsetCount()
                    + ", longest=" + m.getMaxItemsetLength()
                    + ", coverage=" + String.format("%.4f", m.getCoverageRatio())
                    + ", ratio=" + String.format("%.4f", m.getCompressionRatio())
                    + ", elapsedMs=" + result.getElapsedMs());
        }

        assertTrue(bestLongest >= 5);
        assertTrue(bestRatio > -0.1d);
    }

    @Test
    void testMixedTrafficCompressibility() {
        List<DocTerms> rows = CompressibilityAssessmentFixtures.mixedTrafficDataset(10000, 20260428L);
        CompressibilityPipelineSupport.PipelineResult result = CompressibilityPipelineSupport.run(
                rows, 80, 3, 8, 240000, 5000, 32, 96, 140);
        CompressionMetricsUtil.CompressionMetrics m = result.getMetrics();

        System.out.println("mixed selected=" + m.getSelectedItemsetCount()
                + ", avgLen=" + String.format("%.2f", m.getAverageItemsetLength())
                + ", longest=" + m.getMaxItemsetLength()
                + ", coveredTerms=" + String.format("%.4f", m.getCoveredTermRatio())
                + ", coverageBytes=" + String.format("%.4f", m.getCoverageRatio())
                + ", ratio=" + String.format("%.4f", m.getCompressionRatio())
                + ", elapsedMs=" + result.getElapsedMs());

        assertTrue(m.getSelectedItemsetCount() > 0);
        assertTrue(m.getCoverageRatio() > 0.02d);
        assertTrue(m.getMatchedItemsetOccurrences() > 0L);
    }
}
