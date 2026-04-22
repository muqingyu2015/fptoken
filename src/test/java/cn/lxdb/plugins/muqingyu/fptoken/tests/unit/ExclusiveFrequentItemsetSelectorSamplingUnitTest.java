package cn.lxdb.plugins.muqingyu.fptoken.tests.unit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import cn.lxdb.plugins.muqingyu.fptoken.ExclusiveFrequentItemsetSelector;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.config.EngineTuningConfig;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.DocTerms;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.ExclusiveSelectionResult;
import cn.lxdb.plugins.muqingyu.fptoken.tests.ByteArrayTestSupport;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class ExclusiveFrequentItemsetSelectorSamplingUnitTest {

    @Test
    void sampleRatioZero_shouldBypassSamplingPath() {
        List<DocTerms> rows = buildRowsWithStableFrequentPattern(400);

        ExclusiveSelectionResult baseline = runAsFullPath(rows, 120, 2, 4, 100_000);
        try {
            // case under test: sampling enabled but ratio=0 should still behave like full path
            ExclusiveFrequentItemsetSelector.setSamplingEnabled(true);
            ExclusiveFrequentItemsetSelector.setSampleRatio(0.0d);
            ExclusiveFrequentItemsetSelector.setMinSampleCount(50);
            // If sampling path is used, this aggressive scale would likely suppress candidates.
            ExclusiveFrequentItemsetSelector.setSamplingSupportScale(10.0d);

            ExclusiveSelectionResult actual =
                    ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(rows, 120, 2, 4, 100_000);

            assertSameAsBaseline(baseline, actual);
        } finally {
            restoreSamplingDefaults();
        }
    }

    @Test
    void smallDocCount_shouldBypassSamplingPath() {
        List<DocTerms> rows = buildRowsWithStableFrequentPattern(180); // bypass threshold is docCount < 200
        ExclusiveSelectionResult baseline = runAsFullPath(rows, 40, 2, 4, 100_000);
        try {
            ExclusiveFrequentItemsetSelector.setSamplingEnabled(true);
            ExclusiveFrequentItemsetSelector.setSampleRatio(0.30d);
            ExclusiveFrequentItemsetSelector.setMinSampleCount(50);
            ExclusiveFrequentItemsetSelector.setSamplingSupportScale(10.0d);

            ExclusiveSelectionResult actual =
                    ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(rows, 40, 2, 4, 100_000);

            assertSameAsBaseline(baseline, actual);
        } finally {
            restoreSamplingDefaults();
        }
    }

    @Test
    void minSupportOne_shouldBypassSamplingPath() {
        List<DocTerms> rows = buildRowsWithStableFrequentPattern(400);
        ExclusiveSelectionResult baseline = runAsFullPath(rows, 1, 2, 4, 100_000);
        try {
            ExclusiveFrequentItemsetSelector.setSamplingEnabled(true);
            ExclusiveFrequentItemsetSelector.setSampleRatio(0.30d);
            ExclusiveFrequentItemsetSelector.setMinSampleCount(50);
            ExclusiveFrequentItemsetSelector.setSamplingSupportScale(10.0d);

            ExclusiveSelectionResult actual =
                    ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(rows, 1, 2, 4, 100_000);

            assertSameAsBaseline(baseline, actual);
        } finally {
            restoreSamplingDefaults();
        }
    }

    @Test
    void sampleRatioOne_shouldBypassSamplingPath() {
        List<DocTerms> rows = buildRowsWithStableFrequentPattern(400);
        ExclusiveSelectionResult baseline = runAsFullPath(rows, 120, 2, 4, 100_000);
        try {
            ExclusiveFrequentItemsetSelector.setSamplingEnabled(true);
            ExclusiveFrequentItemsetSelector.setSampleRatio(1.0d); // sampleSize >= docCount => bypass
            ExclusiveFrequentItemsetSelector.setMinSampleCount(1);
            ExclusiveFrequentItemsetSelector.setSamplingSupportScale(10.0d);

            ExclusiveSelectionResult actual =
                    ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(rows, 120, 2, 4, 100_000);

            assertSameAsBaseline(baseline, actual);
        } finally {
            restoreSamplingDefaults();
        }
    }

    @Test
    void samplingEnabled_withFixedSeed_shouldBeDeterministicAcrossRuns() {
        List<DocTerms> rows = buildRowsWithStableFrequentPattern(420);
        try {
            ExclusiveFrequentItemsetSelector.setSamplingEnabled(true);
            ExclusiveFrequentItemsetSelector.setSampleRatio(0.30d);
            ExclusiveFrequentItemsetSelector.setMinSampleCount(50);
            ExclusiveFrequentItemsetSelector.setSamplingSupportScale(0.0d);

            ExclusiveSelectionResult r1 =
                    ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(rows, 120, 2, 4, 100_000);
            ExclusiveSelectionResult r2 =
                    ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(rows, 120, 2, 4, 100_000);

            assertEquals(ByteArrayTestSupport.groupsFingerprint(r1.getGroups()),
                    ByteArrayTestSupport.groupsFingerprint(r2.getGroups()));
            assertEquals(r1.getCandidateCount(), r2.getCandidateCount());
            assertEquals(r1.getFrequentTermCount(), r2.getFrequentTermCount());
            assertTrue(!r1.getGroups().isEmpty());
        } finally {
            restoreSamplingDefaults();
        }
    }

    private static ExclusiveSelectionResult runAsFullPath(
            List<DocTerms> rows, int minSupport, int minItemsetSize, int maxItemsetSize, int maxCandidateCount) {
        ExclusiveFrequentItemsetSelector.setSamplingEnabled(false);
        return ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(
                rows, minSupport, minItemsetSize, maxItemsetSize, maxCandidateCount);
    }

    private static void assertSameAsBaseline(ExclusiveSelectionResult baseline, ExclusiveSelectionResult actual) {
        assertEquals(ByteArrayTestSupport.groupsFingerprint(baseline.getGroups()),
                ByteArrayTestSupport.groupsFingerprint(actual.getGroups()));
        assertEquals(baseline.getFrequentTermCount(), actual.getFrequentTermCount());
        assertEquals(baseline.getCandidateCount(), actual.getCandidateCount());
        assertTrue(actual.getGroups().size() > 0);
    }

    private static void restoreSamplingDefaults() {
        // restore defaults to avoid leaking static config to other tests
        ExclusiveFrequentItemsetSelector.setSamplingEnabled(true);
        ExclusiveFrequentItemsetSelector.setSampleRatio(EngineTuningConfig.DEFAULT_SAMPLE_RATIO);
        ExclusiveFrequentItemsetSelector.setMinSampleCount(EngineTuningConfig.DEFAULT_MIN_SAMPLE_COUNT);
        ExclusiveFrequentItemsetSelector.setSamplingSupportScale(EngineTuningConfig.DEFAULT_SAMPLING_SUPPORT_SCALE);
    }

    private static List<DocTerms> buildRowsWithStableFrequentPattern(int count) {
        byte[] a = ByteArrayTestSupport.hex("AA");
        byte[] b = ByteArrayTestSupport.hex("BB");
        byte[] c = ByteArrayTestSupport.hex("CC");
        byte[] noiseX = ByteArrayTestSupport.hex("DD");
        byte[] noiseY = ByteArrayTestSupport.hex("EE");

        List<DocTerms> rows = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            if (i < 300) {
                rows.add(ByteArrayTestSupport.doc(i, a, b, c));
            } else if (i % 2 == 0) {
                rows.add(ByteArrayTestSupport.doc(i, a, noiseX));
            } else {
                rows.add(ByteArrayTestSupport.doc(i, b, noiseY));
            }
        }
        return rows;
    }
}
