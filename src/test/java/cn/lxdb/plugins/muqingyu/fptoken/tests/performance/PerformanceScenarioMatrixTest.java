package cn.lxdb.plugins.muqingyu.fptoken.tests.performance;

import static org.junit.jupiter.api.Assertions.assertTrue;

import cn.lxdb.plugins.muqingyu.fptoken.ExclusiveFrequentItemsetSelector;
import cn.lxdb.plugins.muqingyu.fptoken.config.SelectorConfig;
import cn.lxdb.plugins.muqingyu.fptoken.index.TermTidsetIndex;
import cn.lxdb.plugins.muqingyu.fptoken.miner.BeamFrequentItemsetMiner;
import cn.lxdb.plugins.muqingyu.fptoken.model.DocTerms;
import cn.lxdb.plugins.muqingyu.fptoken.model.ExclusiveSelectionResult;
import cn.lxdb.plugins.muqingyu.fptoken.model.FrequentItemsetMiningResult;
import cn.lxdb.plugins.muqingyu.fptoken.tests.ByteArrayTestSupport;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

@Tag("performance")
@EnabledIfSystemProperty(named = "fptoken.runPerfTests", matches = "true")
class PerformanceScenarioMatrixTest {

    @Test
    void scenarioA_scalabilityByDataVolume_printsTrend() {
        int[] scales = parseCsvInts(System.getProperty("fptoken.perf.scales", "200,600,1200"));
        int minSupport = Integer.getInteger("fptoken.perf.scale.minSupport", 12);
        int maxCandidateCount = Integer.getInteger("fptoken.perf.scale.maxCandidateCount", 150_000);

        long prevDocs = -1;
        for (int records : scales) {
            List<DocTerms> rows = ByteArrayTestSupport.pcapLikeBatch(records, 256, 32, 16);
            PerfSnapshot p = runFacade(rows, minSupport, 2, 6, maxCandidateCount);
            System.out.println("[perf scale] records=" + records
                    + " docs=" + p.docs
                    + " ms=" + p.elapsedMs
                    + " candidates=" + p.candidateCount
                    + " groups=" + p.groupCount
                    + " memDeltaMb=" + p.heapDeltaMb);
            assertTrue(p.elapsedMs >= 0);
            assertTrue(p.candidateCount >= 0);
            assertTrue(p.docs > prevDocs);
            prevDocs = p.docs;
        }
    }

    @Test
    void scenarioB_parameterImpact_minSupportAndBeamWidth() {
        List<DocTerms> rows = ByteArrayTestSupport.pcapLikeBatch(
                Integer.getInteger("fptoken.perf.param.records", 900), 256, 32, 16);

        PerfSnapshot lowSupport = runFacade(rows, 8, 2, 6, 200_000);
        PerfSnapshot highSupport = runFacade(rows, 40, 2, 6, 200_000);
        System.out.println("[perf params] lowSupport candidates=" + lowSupport.candidateCount
                + ", highSupport candidates=" + highSupport.candidateCount);
        assertTrue(highSupport.candidateCount <= lowSupport.candidateCount);
        assertTrue(highSupport.frequentTermCount <= lowSupport.frequentTermCount);

        TermTidsetIndex index = TermTidsetIndex.build(rows);
        SelectorConfig cfg = new SelectorConfig(12, 2, 6, 250_000);
        BeamFrequentItemsetMiner miner = new BeamFrequentItemsetMiner();
        FrequentItemsetMiningResult narrow = miner.mineWithStats(index.getTidsetsByTermId(), cfg, 1024, 8, 8);
        FrequentItemsetMiningResult wide = miner.mineWithStats(index.getTidsetsByTermId(), cfg, 1024, 32, 64);
        System.out.println("[perf params] narrow candidates=" + narrow.getGeneratedCandidateCount()
                + ", wide candidates=" + wide.getGeneratedCandidateCount());
        assertTrue(wide.getGeneratedCandidateCount() >= narrow.getGeneratedCandidateCount());
    }

    @Test
    void scenarioC_memoryPressure_withHeapBudgetHint() {
        int records = Integer.getInteger("fptoken.perf.mem.records", 1400);
        int heapBudgetMb = Integer.getInteger("fptoken.perf.memBudgetMb", 1024);
        List<DocTerms> rows = ByteArrayTestSupport.pcapLikeBatch(records, 320, 40, 20);
        PerfSnapshot p = runFacade(rows, 18, 2, 6, 220_000);
        System.out.println("[perf memory] docs=" + p.docs
                + " ms=" + p.elapsedMs
                + " memDeltaMb=" + p.heapDeltaMb
                + " budgetMb=" + heapBudgetMb);

        assertTrue(p.heapDeltaMb <= heapBudgetMb, () -> "heap delta " + p.heapDeltaMb + "MB > budget " + heapBudgetMb + "MB");
        assertTrue(p.candidateCount <= 220_000);
    }

    @Test
    void scenarioD_dataDistribution_highVsLowRepetition() {
        List<DocTerms> highRepetition = buildHighRepetitionRows(500);
        List<DocTerms> lowRepetition = buildLowRepetitionRows(500);

        PerfSnapshot high = runFacade(highRepetition, 20, 2, 6, 120_000);
        PerfSnapshot low = runFacade(lowRepetition, 20, 2, 6, 120_000);
        System.out.println("[perf dist] highRep candidates=" + high.candidateCount
                + ", lowRep candidates=" + low.candidateCount
                + ", highMs=" + high.elapsedMs
                + ", lowMs=" + low.elapsedMs);

        assertTrue(high.candidateCount >= low.candidateCount);
        assertTrue(high.groupCount >= 0 && low.groupCount >= 0);
    }

    private static PerfSnapshot runFacade(
            List<DocTerms> rows,
            int minSupport,
            int minItemsetSize,
            int maxItemsetSize,
            int maxCandidateCount
    ) {
        Runtime rt = Runtime.getRuntime();
        rt.gc();
        long memBefore = rt.totalMemory() - rt.freeMemory();
        long t0 = System.nanoTime();
        ExclusiveSelectionResult r = ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(
                rows, minSupport, minItemsetSize, maxItemsetSize, maxCandidateCount);
        long elapsedMs = (System.nanoTime() - t0) / 1_000_000L;
        long memAfter = rt.totalMemory() - rt.freeMemory();
        long heapDeltaMb = Math.max(0L, (memAfter - memBefore) / (1024L * 1024L));
        return new PerfSnapshot(
                rows.size(),
                elapsedMs,
                heapDeltaMb,
                r.getCandidateCount(),
                r.getFrequentTermCount(),
                r.getGroups().size());
    }

    private static int[] parseCsvInts(String csv) {
        String[] parts = csv.split(",");
        int[] out = new int[parts.length];
        for (int i = 0; i < parts.length; i++) {
            out[i] = Integer.parseInt(parts[i].trim());
        }
        Arrays.sort(out);
        return out;
    }

    private static List<DocTerms> buildHighRepetitionRows(int count) {
        byte[] a = {(byte) 0xAA};
        byte[] b = {(byte) 0xBB};
        byte[] c = {(byte) 0xCC};
        List<DocTerms> rows = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            rows.add(ByteArrayTestSupport.doc(i, a, b, c, new byte[] {(byte) (i & 0x03)}));
        }
        return rows;
    }

    private static List<DocTerms> buildLowRepetitionRows(int count) {
        Random random = new Random(20260421L);
        List<DocTerms> rows = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            byte[] x = new byte[4];
            byte[] y = new byte[4];
            random.nextBytes(x);
            random.nextBytes(y);
            rows.add(ByteArrayTestSupport.doc(i, x, y));
        }
        return rows;
    }

    private static final class PerfSnapshot {
        final int docs;
        final long elapsedMs;
        final long heapDeltaMb;
        final int candidateCount;
        final int frequentTermCount;
        final int groupCount;

        PerfSnapshot(
                int docs,
                long elapsedMs,
                long heapDeltaMb,
                int candidateCount,
                int frequentTermCount,
                int groupCount
        ) {
            this.docs = docs;
            this.elapsedMs = elapsedMs;
            this.heapDeltaMb = heapDeltaMb;
            this.candidateCount = candidateCount;
            this.frequentTermCount = frequentTermCount;
            this.groupCount = groupCount;
        }
    }
}
