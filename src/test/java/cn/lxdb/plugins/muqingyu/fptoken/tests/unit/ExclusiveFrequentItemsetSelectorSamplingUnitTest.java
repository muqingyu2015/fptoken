package cn.lxdb.plugins.muqingyu.fptoken.tests.unit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import cn.lxdb.plugins.muqingyu.fptoken.ExclusiveFrequentItemsetSelector;
import cn.lxdb.plugins.muqingyu.fptoken.config.EngineTuningConfig;
import cn.lxdb.plugins.muqingyu.fptoken.model.DocTerms;
import cn.lxdb.plugins.muqingyu.fptoken.model.ExclusiveSelectionResult;
import cn.lxdb.plugins.muqingyu.fptoken.tests.ByteArrayTestSupport;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class ExclusiveFrequentItemsetSelectorSamplingUnitTest {

    @Test
    void sampleRatioZero_shouldBypassSamplingPath() {
        List<DocTerms> rows = buildRowsWithStableFrequentPattern(400);

        // baseline: explicit full path
        ExclusiveFrequentItemsetSelector.setSamplingEnabled(false);
        ExclusiveSelectionResult baseline =
                ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(rows, 120, 2, 4, 100_000);

        // case under test: sampling enabled but ratio=0 should still behave like full path
        ExclusiveFrequentItemsetSelector.setSamplingEnabled(true);
        ExclusiveFrequentItemsetSelector.setSampleRatio(0.0d);
        ExclusiveFrequentItemsetSelector.setMinSampleCount(50);
        // If sampling path is used, this aggressive scale would likely suppress candidates.
        ExclusiveFrequentItemsetSelector.setSamplingSupportScale(10.0d);

        try {
            ExclusiveSelectionResult actual =
                    ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(rows, 120, 2, 4, 100_000);

            assertEquals(ByteArrayTestSupport.groupsFingerprint(baseline.getGroups()),
                    ByteArrayTestSupport.groupsFingerprint(actual.getGroups()));
            assertEquals(baseline.getFrequentTermCount(), actual.getFrequentTermCount());
            assertEquals(baseline.getCandidateCount(), actual.getCandidateCount());
            assertTrue(actual.getGroups().size() > 0);
        } finally {
            // restore defaults to avoid leaking static config to other tests
            ExclusiveFrequentItemsetSelector.setSamplingEnabled(true);
            ExclusiveFrequentItemsetSelector.setSampleRatio(EngineTuningConfig.DEFAULT_SAMPLE_RATIO);
            ExclusiveFrequentItemsetSelector.setMinSampleCount(EngineTuningConfig.DEFAULT_MIN_SAMPLE_COUNT);
            ExclusiveFrequentItemsetSelector.setSamplingSupportScale(EngineTuningConfig.DEFAULT_SAMPLING_SUPPORT_SCALE);
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
