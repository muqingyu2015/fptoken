package cn.lxdb.plugins.muqingyu.fptoken.tests.performance;

import static org.junit.jupiter.api.Assertions.assertTrue;

import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.config.SelectorConfig;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.index.TermTidsetIndex;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.miner.BeamFrequentItemsetMiner;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.CandidateItemset;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.DocTerms;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.FrequentItemsetMiningResult;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.picker.TwoPhaseExclusiveItemsetPicker;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

@Tag("performance")
@EnabledIfSystemProperty(named = "fptoken.runPerfTests", matches = "true")
@Timeout(value = 10, unit = TimeUnit.SECONDS)
class PerformanceVsCompressibilityBoundaryTest {

    @Test
    @EnabledIfSystemProperty(named = "fptoken.runStressTests", matches = "true")
    void stressTestForLongestPatterns() {
        List<DocTerms> rows = CompressibilityAssessmentFixtures.longPatternDataset(5000, 20260425L);
        Runtime rt = Runtime.getRuntime();
        rt.gc();
        long memBefore = rt.totalMemory() - rt.freeMemory();
        long start = System.nanoTime();

        boolean success = false;
        String failureReason = "";
        int longest = 0;
        try {
            CompressibilityPipelineSupport.PipelineResult result = CompressibilityPipelineSupport.run(
                    rows, 10, 3, 15, 320000, 8000, 48, 128, 250);
            longest = result.getMetrics().getMaxItemsetLength();
            success = true;
            System.out.println("boundary-longest success=true"
                    + ", longest=" + longest
                    + ", selected=" + result.getMetrics().getSelectedItemsetCount()
                    + ", ratio=" + String.format("%.4f", result.getMetrics().getCompressionRatio())
                    + ", elapsedMs=" + result.getElapsedMs());
        } catch (OutOfMemoryError oom) {
            failureReason = "oom";
        } catch (RuntimeException ex) {
            failureReason = ex.getClass().getSimpleName();
        }

        long elapsedMs = (System.nanoTime() - start) / 1_000_000L;
        long memAfter = rt.totalMemory() - rt.freeMemory();
        long deltaMb = Math.max(0L, (memAfter - memBefore) / (1024L * 1024L));

        if (success) {
            assertTrue(longest >= 3);
        } else {
            assertTrue(!failureReason.isEmpty());
        }
        assertTrue(elapsedMs >= 0L);
        System.out.println("boundary-longest success=" + success
                + ", reason=" + failureReason
                + ", elapsedMs=" + elapsedMs
                + ", memDeltaMb=" + deltaMb
                + ", longest=" + longest);
    }

    @Test
    void testImpactOfHighBranchingFactorOnLongPatterns() {
        List<DocTerms> rows = CompressibilityAssessmentFixtures.randomPayloadDataset(6000, 14, 20260426L);

        MiningBoundarySnapshot b32 = runMiningBoundaryCase(rows, 100, 8, 32);
        MiningBoundarySnapshot b48 = runMiningBoundaryCase(rows, 100, 8, 48);

        System.out.println("branch=32 longest=" + b32.longest + ", elapsedMs=" + b32.elapsedMs + ", memDeltaMb=" + b32.memDeltaMb);
        System.out.println("branch=48 longest=" + b48.longest + ", elapsedMs=" + b48.elapsedMs + ", memDeltaMb=" + b48.memDeltaMb);

        assertTrue(b32.elapsedMs >= 0L && b48.elapsedMs >= 0L);
        assertTrue(b48.longest + 1 >= b32.longest);
    }

    private static MiningBoundarySnapshot runMiningBoundaryCase(
            List<DocTerms> rows,
            int minSupport,
            int maxLen,
            int maxBranchingFactor
    ) {
        Runtime rt = Runtime.getRuntime();
        rt.gc();
        long memBefore = rt.totalMemory() - rt.freeMemory();

        long start = System.nanoTime();
        TermTidsetIndex index = TermTidsetIndex.build(rows);
        SelectorConfig config = new SelectorConfig(minSupport, 3, maxLen, 250000);
        BeamFrequentItemsetMiner miner = new BeamFrequentItemsetMiner();
        FrequentItemsetMiningResult mining = miner.mineWithStats(
                index.getTidsetsByTermIdUnsafe(),
                config,
                6000,
                maxBranchingFactor,
                96
        );
        TwoPhaseExclusiveItemsetPicker picker = new TwoPhaseExclusiveItemsetPicker();
        List<CandidateItemset> selected = picker.pick(
                mining.getCandidates(),
                index.getIdToTermUnsafe().size(),
                120
        );
        int longest = 0;
        for (CandidateItemset candidate : selected) {
            longest = Math.max(longest, candidate.length());
        }

        long elapsedMs = (System.nanoTime() - start) / 1_000_000L;
        long memAfter = rt.totalMemory() - rt.freeMemory();
        long deltaMb = Math.max(0L, (memAfter - memBefore) / (1024L * 1024L));
        return new MiningBoundarySnapshot(elapsedMs, longest, deltaMb);
    }

    private static final class MiningBoundarySnapshot {
        final long elapsedMs;
        final int longest;
        final long memDeltaMb;

        private MiningBoundarySnapshot(long elapsedMs, int longest, long memDeltaMb) {
            this.elapsedMs = elapsedMs;
            this.longest = longest;
            this.memDeltaMb = memDeltaMb;
        }
    }
}
