package cn.lxdb.plugins.muqingyu.fptoken.tests.unit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import cn.lxdb.plugins.muqingyu.fptoken.ExclusiveFrequentItemsetSelector;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.DocTerms;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.ExclusiveSelectionResult;
import cn.lxdb.plugins.muqingyu.fptoken.tests.ByteArrayTestSupport;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;

class ExclusiveFrequentItemsetSelectorStatsUnitTest {

    @Test
    void emptyRows_echoesMaxCandidateCountAndZeroStats() {
        int maxCandidateCount = 1234;
        ExclusiveSelectionResult r = ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(
                Collections.emptyList(), 2, 1, 6, maxCandidateCount);

        assertNotNull(r);
        assertTrue(r.getGroups().isEmpty());
        assertEquals(0, r.getFrequentTermCount());
        assertEquals(0, r.getCandidateCount());
        assertEquals(0, r.getIntersectionCount());
        assertEquals(maxCandidateCount, r.getMaxCandidateCount());
        assertFalse(r.isTruncatedByCandidateLimit());
    }

    @Test
    void minedResult_neverViolatesBasicStatsInvariants() {
        List<DocTerms> rows = new ArrayList<>();
        byte[] core = ByteArrayTestSupport.hex("ABCD");
        byte[] alt = ByteArrayTestSupport.hex("1234");
        for (int i = 0; i < 40; i++) {
            rows.add(ByteArrayTestSupport.doc(i, core, alt, new byte[] {(byte) (i & 0x07)}));
        }

        ExclusiveSelectionResult r = ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(
                rows, 8, 2, 6, 5000);

        assertTrue(r.getFrequentTermCount() >= 0);
        assertTrue(r.getCandidateCount() >= 0);
        assertTrue(r.getIntersectionCount() >= 0);
        assertTrue(r.getCandidateCount() <= r.getMaxCandidateCount());
        assertTrue(ByteArrayTestSupport.pairwiseTermsDisjoint(r.getGroups()));
    }

    @Test
    void nullRows_threeArgOverload_returnsEmptyAndZeroStats() {
        ExclusiveSelectionResult r = ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(null, 2, 1);
        assertNotNull(r);
        assertTrue(r.getGroups().isEmpty());
        assertEquals(0, r.getFrequentTermCount());
        assertEquals(0, r.getCandidateCount());
        assertEquals(0, r.getIntersectionCount());
    }

    @Test
    void listOnlyOverload_emptyRows_returnsEmptyList() {
        List<DocTerms> rows = Collections.emptyList();
        List<cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.SelectedGroup> groups =
                ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsets(rows, 2, 1);
        assertTrue(groups.isEmpty());
    }

    @Test
    void invalidArguments_propagateFromSelectorConfig() {
        List<DocTerms> rows = Collections.singletonList(ByteArrayTestSupport.doc(0, new byte[] {1}));
        assertThrows(IllegalArgumentException.class,
                () -> ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(rows, 0, 1, 4, 1000));
        assertThrows(IllegalArgumentException.class,
                () -> ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(rows, 1, 0, 4, 1000));
        assertThrows(IllegalArgumentException.class,
                () -> ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(rows, 1, 3, 2, 1000));
        assertThrows(IllegalArgumentException.class,
                () -> ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(rows, 1, 1, 4, 0));
    }

    @Test
    void tinyCandidateLimit_canTriggerTruncation() {
        List<DocTerms> rows = new ArrayList<>();
        for (int i = 0; i < 30; i++) {
            rows.add(ByteArrayTestSupport.doc(i, new byte[] {1}, new byte[] {2}, new byte[] {3}));
        }
        ExclusiveSelectionResult r = ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(
                rows, 5, 1, 5, 1);
        assertTrue(r.getCandidateCount() <= 1);
        assertTrue(r.isTruncatedByCandidateLimit());
        assertEquals(1, r.getMaxCandidateCount());
    }

    @Test
    void diagnostics_shouldExposeSamplingAndStageTimingMetrics() {
        List<DocTerms> rows = new ArrayList<>();
        byte[] a = ByteArrayTestSupport.hex("AA");
        byte[] b = ByteArrayTestSupport.hex("BB");
        byte[] c = ByteArrayTestSupport.hex("CC");
        for (int i = 0; i < 120; i++) {
            rows.add(ByteArrayTestSupport.doc(i, a, b, c, new byte[] {(byte) (i % 9)}));
        }

        ExclusiveSelectionResult result = ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(
                rows, 10, 2, 4, 20_000);
        ExclusiveSelectionResult.SelectionDiagnostics diagnostics = result.getDiagnostics();

        assertNotNull(diagnostics);
        assertTrue(diagnostics.getTargetSampleSize() >= diagnostics.getActualSampleSize());
        assertTrue(diagnostics.getIndexBuildMs() >= 0);
        assertTrue(diagnostics.getMiningInputBuildMs() >= 0);
        assertTrue(diagnostics.getMiningMs() >= 0);
        assertTrue(diagnostics.getHintMergeMs() >= 0);
        assertTrue(diagnostics.getRecomputeMs() >= 0);
        assertTrue(diagnostics.getPickMs() >= 0);
        assertTrue(diagnostics.getTotalMs() >= 0);
        assertFalse(diagnostics.isSampledInputBuildFallbackToFull());
        assertFalse(diagnostics.isSampledMiningFallbackToFull());
    }

    @Test
    void selectionRequest_shouldCarryExecutionTuningAsImmutableConfigBoundary() {
        List<DocTerms> rows = new ArrayList<>();
        byte[] a = ByteArrayTestSupport.hex("AA");
        byte[] b = ByteArrayTestSupport.hex("BB");
        for (int i = 0; i < 80; i++) {
            rows.add(ByteArrayTestSupport.doc(i, a, b));
        }
        ExclusiveFrequentItemsetSelector.ExecutionTuning tuning =
                new ExclusiveFrequentItemsetSelector.ExecutionTuning(
                        0.2d,
                        8,
                        0.1d,
                        0,
                        1,
                        0,
                        1.0d,
                        cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.picker.TwoPhaseExclusiveItemsetPicker.ScoringWeights.defaults()
                );
        ExclusiveFrequentItemsetSelector.SelectionRequest request =
                ExclusiveFrequentItemsetSelector.SelectionRequest.builder(rows)
                        .minSupport(3)
                        .minItemsetSize(2)
                        .maxItemsetSize(4)
                        .maxCandidateCount(10_000)
                        .executionTuning(tuning)
                        .build();

        ExclusiveSelectionResult result = ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(request);
        assertNotNull(result);
        assertNotNull(result.getDiagnostics());
    }
}
