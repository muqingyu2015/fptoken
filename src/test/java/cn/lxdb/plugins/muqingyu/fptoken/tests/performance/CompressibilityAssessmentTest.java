package cn.lxdb.plugins.muqingyu.fptoken.tests.performance;

import static org.junit.jupiter.api.Assertions.assertTrue;

import cn.lxdb.plugins.muqingyu.fptoken.model.DocTerms;
import cn.lxdb.plugins.muqingyu.fptoken.tests.ByteArrayTestSupport;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

@Tag("performance")
@EnabledIfSystemProperty(named = "fptoken.runPerfTests", matches = "true")
@Timeout(value = 10, unit = TimeUnit.SECONDS)
class CompressibilityAssessmentTest {

    @Test
    void testLongPatternDiscoveryAndCompressibility() {
        List<DocTerms> rows = createLongPatternDataset(
                PerfTestSupport.intProp("fptoken.perf.compress.docs", 10000),
                20260421L);
        CompressibilityPipelineSupport.PipelineResult result = CompressibilityPipelineSupport.run(
                rows, 80, 3, 10, 260000, 5000, 32, 96, 120);

        CompressionMetricsUtil.CompressionMetrics m = result.getMetrics();
        assertTrue(m.getSelectedItemsetCount() > 0);
        assertTrue(m.getAverageItemsetLength() >= 3d);
        assertTrue(m.getMaxItemsetLength() >= 4);
        assertTrue(m.getCoverageRatio() > 0.05d);
        assertTrue(m.getCompressionRatio() > -0.25d);

        System.out.println("compress-main selected=" + m.getSelectedItemsetCount()
                + ", avgLen=" + String.format("%.2f", m.getAverageItemsetLength())
                + ", maxLen=" + m.getMaxItemsetLength()
                + ", coverage=" + String.format("%.4f", m.getCoverageRatio())
                + ", ratio=" + String.format("%.4f", m.getCompressionRatio())
                + ", elapsedMs=" + result.getElapsedMs());
    }

    @Test
    void testEffectOfMinSupportOnCompressibility() {
        List<DocTerms> rows = createLongPatternDataset(8000, 20260422L);
        int[] minSupports = new int[] {20, 50, 100, 200, 500};
        List<CompressionMetricsUtil.CompressionMetrics> metrics = new ArrayList<>();

        for (int minSupport : minSupports) {
            CompressibilityPipelineSupport.PipelineResult result = CompressibilityPipelineSupport.run(
                    rows, minSupport, 3, 8, 220000, 4096, 24, 64, 100);
            metrics.add(result.getMetrics());
            System.out.println("minSupport=" + minSupport
                    + ", avgLen=" + String.format("%.2f", result.getMetrics().getAverageItemsetLength())
                    + ", maxLen=" + result.getMetrics().getMaxItemsetLength()
                    + ", coverage=" + String.format("%.4f", result.getMetrics().getCoverageRatio())
                    + ", ratio=" + String.format("%.4f", result.getMetrics().getCompressionRatio()));
        }

        assertTrue(metrics.size() == minSupports.length);
        assertTrue(metrics.get(0).getCoverageRatio() + 0.03d >= metrics.get(metrics.size() - 1).getCoverageRatio());
    }

    @Test
    void testEffectOfMaxItemsetSizeOnCompressibility() {
        List<DocTerms> rows = createLongPatternDataset(8000, 20260423L);
        int[] maxItemsetSizes = new int[] {4, 6, 8, 10, 12};
        int observedBestMaxLen = 0;

        for (int maxSize : maxItemsetSizes) {
            CompressibilityPipelineSupport.PipelineResult result = CompressibilityPipelineSupport.run(
                    rows, 100, 3, maxSize, 240000, 4096, 24, 64, 100);
            CompressionMetricsUtil.CompressionMetrics m = result.getMetrics();
            observedBestMaxLen = Math.max(observedBestMaxLen, m.getMaxItemsetLength());
            System.out.println("maxItemsetSize=" + maxSize
                    + ", avgLen=" + String.format("%.2f", m.getAverageItemsetLength())
                    + ", maxLen=" + m.getMaxItemsetLength()
                    + ", coverage=" + String.format("%.4f", m.getCoverageRatio())
                    + ", ratio=" + String.format("%.4f", m.getCompressionRatio())
                    + ", elapsedMs=" + result.getElapsedMs());
            assertTrue(result.getElapsedMs() >= 0L);
        }

        assertTrue(observedBestMaxLen >= 4);
    }

    private static List<DocTerms> createLongPatternDataset(int docs, long seed) {
        Random random = new Random(seed);
        List<byte[]> patternA = tokenRange(1000, 8);
        List<byte[]> patternB = tokenRange(2000, 7);
        List<byte[]> patternC = tokenRange(3000, 6);
        List<byte[]> shared = tokenRange(900, 2);

        List<DocTerms> rows = new ArrayList<>(docs);
        for (int i = 0; i < docs; i++) {
            List<byte[]> terms = new ArrayList<>();
            terms.addAll(shared);
            int mod = i % 10;
            if (mod <= 5) {
                terms.addAll(patternA);
            } else if (mod <= 8) {
                terms.addAll(patternB);
            } else {
                terms.addAll(patternC);
            }
            for (int n = 0; n < 6; n++) {
                terms.add(token(20000 + random.nextInt(6000)));
            }
            rows.add(ByteArrayTestSupport.doc(i, terms));
        }
        return rows;
    }

    private static List<byte[]> tokenRange(int base, int count) {
        List<byte[]> out = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            out.add(token(base + i));
        }
        return out;
    }

    private static byte[] token(int id) {
        return new byte[] {(byte) ((id >>> 16) & 0xFF), (byte) ((id >>> 8) & 0xFF), (byte) (id & 0xFF)};
    }
}
