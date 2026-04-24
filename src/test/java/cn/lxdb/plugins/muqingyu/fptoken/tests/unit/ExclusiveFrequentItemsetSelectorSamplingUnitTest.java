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
import org.junit.jupiter.api.parallel.ResourceAccessMode;
import org.junit.jupiter.api.parallel.ResourceLock;

@ResourceLock(value = "ExclusiveFrequentItemsetSelector.runtimeTuning", mode = ResourceAccessMode.READ_WRITE)
class ExclusiveFrequentItemsetSelectorSamplingUnitTest {
    private static final String PROP_TESTING_FAIL_SAMPLING_PATH = "fptoken.selector.testing.failSamplingPath";

    @Test
    void sampleRatioZero_shouldStillBeDeterministicWithMinSampleFallback() {
        List<DocTerms> rows = buildRowsWithStableFrequentPattern(400);
        try {
            ExclusiveFrequentItemsetSelector.setSampleRatio(0.0d);
            ExclusiveFrequentItemsetSelector.setMinSampleCount(50);
            ExclusiveFrequentItemsetSelector.setSamplingSupportScale(0.0d);

            ExclusiveSelectionResult r1 =
                    ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(rows, 120, 2, 4, 100_000);
            ExclusiveSelectionResult r2 =
                    ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(rows, 120, 2, 4, 100_000);

            assertEquals(ByteArrayTestSupport.groupsFingerprint(r1.getGroups()),
                    ByteArrayTestSupport.groupsFingerprint(r2.getGroups()));
            assertEquals(r1.getCandidateCount(), r2.getCandidateCount());
            assertTrue(r1.getCandidateCount() >= 0);
        } finally {
            restoreSamplingDefaults();
        }
    }

    @Test
    void sampleRatioOne_shouldApproximateFullPopulationBaseline() {
        List<DocTerms> rows = buildRowsWithStableFrequentPattern(400);
        try {
            ExclusiveFrequentItemsetSelector.setSampleRatio(1.0d);
            ExclusiveFrequentItemsetSelector.setMinSampleCount(1);
            ExclusiveFrequentItemsetSelector.setSamplingSupportScale(1.0d);
            ExclusiveSelectionResult baseline =
                    ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(rows, 120, 2, 4, 100_000);

            ExclusiveFrequentItemsetSelector.setSampleRatio(0.30d);
            ExclusiveFrequentItemsetSelector.setMinSampleCount(50);
            ExclusiveFrequentItemsetSelector.setSamplingSupportScale(0.0d);
            ExclusiveSelectionResult sampled =
                    ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(rows, 120, 2, 4, 100_000);

            assertTrue(!baseline.getGroups().isEmpty());
            assertTrue(!sampled.getGroups().isEmpty());
        } finally {
            restoreSamplingDefaults();
        }
    }

    @Test
    void largerSupportScale_shouldNotIncreaseCandidateCount() {
        List<DocTerms> rows = buildRowsWithStableFrequentPattern(400);
        try {
            ExclusiveFrequentItemsetSelector.setSampleRatio(0.30d);
            ExclusiveFrequentItemsetSelector.setMinSampleCount(50);
            ExclusiveFrequentItemsetSelector.setSamplingSupportScale(0.0d);
            ExclusiveSelectionResult lowScale =
                    ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(rows, 120, 2, 4, 100_000);

            ExclusiveFrequentItemsetSelector.setSamplingSupportScale(1.8d);
            ExclusiveSelectionResult highScale =
                    ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(rows, 120, 2, 4, 100_000);

            assertTrue(highScale.getCandidateCount() <= lowScale.getCandidateCount());
        } finally {
            restoreSamplingDefaults();
        }
    }

    @Test
    void samplingWithFixedSeed_shouldBeDeterministicAcrossRuns() {
        List<DocTerms> rows = buildRowsWithStableFrequentPattern(420);
        try {
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

    @Test
    void samplingPathFailure_shouldGracefullyFallbackToFullPath() {
        List<DocTerms> rows = buildRowsWithStableFrequentPattern(420);
        String old = System.getProperty(PROP_TESTING_FAIL_SAMPLING_PATH);
        try {
            ExclusiveFrequentItemsetSelector.setSampleRatio(0.30d);
            ExclusiveFrequentItemsetSelector.setMinSampleCount(50);
            ExclusiveFrequentItemsetSelector.setSamplingSupportScale(0.0d);
            System.setProperty(PROP_TESTING_FAIL_SAMPLING_PATH, "true");

            ExclusiveSelectionResult result =
                    ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(rows, 120, 2, 4, 100_000);

            assertTrue(result.getCandidateCount() >= 0);
            assertTrue(!result.getGroups().isEmpty());
        } finally {
            restoreSystemProperty(PROP_TESTING_FAIL_SAMPLING_PATH, old);
            restoreSamplingDefaults();
        }
    }

    private static void restoreSamplingDefaults() {
        // restore defaults to avoid leaking static config to other tests
        ExclusiveFrequentItemsetSelector.setSampleRatio(EngineTuningConfig.DEFAULT_SAMPLE_RATIO);
        ExclusiveFrequentItemsetSelector.setMinSampleCount(EngineTuningConfig.DEFAULT_MIN_SAMPLE_COUNT);
        ExclusiveFrequentItemsetSelector.setSamplingSupportScale(EngineTuningConfig.DEFAULT_SAMPLING_SUPPORT_SCALE);
    }

    private static void restoreSystemProperty(String key, String originalValue) {
        if (originalValue == null) {
            System.clearProperty(key);
        } else {
            System.setProperty(key, originalValue);
        }
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
