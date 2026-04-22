package cn.lxdb.plugins.muqingyu.fptoken.tests.integration;

import static org.junit.jupiter.api.Assertions.assertTrue;

import cn.lxdb.plugins.muqingyu.fptoken.ExclusiveFrequentItemsetSelector;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.DocTerms;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.ExclusiveSelectionResult;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.SelectedGroup;
import cn.lxdb.plugins.muqingyu.fptoken.tests.ByteArrayTestSupport;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.junit.jupiter.api.Test;

/**
 * Legacy root benchmarks emphasized "sampling keeps high-frequency quality".
 * This integration test codifies that expectation into stable assertions.
 */
class SamplingVsFullQualityIntegrationTest {

    @Test
    void samplingMode_shouldKeepMainHighFrequencyPattern_andBoundCoverageDrop() {
        byte[] a = "H0".getBytes(StandardCharsets.UTF_8);
        byte[] b = "H1".getBytes(StandardCharsets.UTF_8);
        byte[] c = "H2".getBytes(StandardCharsets.UTF_8);
        List<DocTerms> rows = buildRows(800, a, b, c);

        boolean oldSampling = true;
        double oldRatio = ExclusiveFrequentItemsetSelector.getSampleRatio();
        int oldMinSample = ExclusiveFrequentItemsetSelector.getMinSampleCount();
        double oldScale = ExclusiveFrequentItemsetSelector.getSamplingSupportScale();
        try {
            ExclusiveFrequentItemsetSelector.setSamplingEnabled(false);
            ExclusiveSelectionResult full = ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(
                    rows, 80, 2, 4, 120_000);

            ExclusiveFrequentItemsetSelector.setSamplingEnabled(true);
            ExclusiveFrequentItemsetSelector.setSampleRatio(0.30d);
            ExclusiveFrequentItemsetSelector.setMinSampleCount(50);
            ExclusiveFrequentItemsetSelector.setSamplingSupportScale(0.0d); // auto-scale

            ExclusiveSelectionResult sampled = ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(
                    rows, 80, 2, 4, 120_000);

            assertTrue(!full.getGroups().isEmpty(), "full path should produce groups");
            assertTrue(!sampled.getGroups().isEmpty(), "sampling path should produce groups");
            assertTrue(ByteArrayTestSupport.anyGroupHasExactlyTerms(full.getGroups(), new byte[][] {a, b, c}),
                    "full path should contain core pattern [H0,H1,H2]");
            assertTrue(ByteArrayTestSupport.anyGroupHasExactlyTerms(sampled.getGroups(), new byte[][] {a, b, c}),
                    "sampling path should retain core pattern [H0,H1,H2]");

            int fullCovered = totalSupport(full.getGroups());
            int sampledCovered = totalSupport(sampled.getGroups());
            double ratio = (double) sampledCovered / (double) Math.max(1, fullCovered);
            assertTrue(ratio >= 0.85d,
                    "sampling total support should stay close to full path, ratio=" + ratio
                            + ", sampled=" + sampledCovered + ", full=" + fullCovered);
        } finally {
            ExclusiveFrequentItemsetSelector.setSamplingEnabled(oldSampling);
            ExclusiveFrequentItemsetSelector.setSampleRatio(oldRatio);
            ExclusiveFrequentItemsetSelector.setMinSampleCount(oldMinSample);
            ExclusiveFrequentItemsetSelector.setSamplingSupportScale(oldScale);
        }
    }

    private static int totalSupport(List<SelectedGroup> groups) {
        int sum = 0;
        for (SelectedGroup group : groups) {
            sum += group.getSupport();
        }
        return sum;
    }

    private static List<DocTerms> buildRows(int count, byte[] a, byte[] b, byte[] c) {
        Random rnd = new Random(42);
        List<DocTerms> rows = new ArrayList<>(count);
        byte[][] noise = new byte[1200][];
        for (int i = 0; i < noise.length; i++) {
            noise[i] = ("N" + i).getBytes(StandardCharsets.UTF_8);
        }

        for (int docId = 0; docId < count; docId++) {
            List<byte[]> terms = new ArrayList<>();
            if (docId < (count * 3 / 4)) {
                terms.add(a);
                terms.add(b);
                terms.add(c);
            }
            if ((docId & 1) == 0) {
                terms.add("M0".getBytes(StandardCharsets.UTF_8));
                terms.add("M1".getBytes(StandardCharsets.UTF_8));
            }
            int target = 16 + rnd.nextInt(8);
            while (terms.size() < target) {
                terms.add(noise[rnd.nextInt(noise.length)]);
            }
            rows.add(new DocTerms(docId, terms));
        }
        return rows;
    }
}
