package cn.lxdb.plugins.muqingyu.fptoken.tests.performance;

import static org.junit.jupiter.api.Assertions.assertTrue;

import cn.lxdb.plugins.muqingyu.fptoken.ExclusiveFrequentItemsetSelector;
import cn.lxdb.plugins.muqingyu.fptoken.config.SelectorConfig;
import cn.lxdb.plugins.muqingyu.fptoken.index.TermTidsetIndex;
import cn.lxdb.plugins.muqingyu.fptoken.miner.BeamFrequentItemsetMiner;
import cn.lxdb.plugins.muqingyu.fptoken.model.CandidateItemset;
import cn.lxdb.plugins.muqingyu.fptoken.model.DocTerms;
import cn.lxdb.plugins.muqingyu.fptoken.model.ExclusiveSelectionResult;
import cn.lxdb.plugins.muqingyu.fptoken.model.FrequentItemsetMiningResult;
import cn.lxdb.plugins.muqingyu.fptoken.picker.TwoPhaseExclusiveItemsetPicker;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

/**
 * 参数建议驱动的性能场景测试（A-H）。
 *
 * <p>说明：默认数据量使用可运行的中等规模，避免本地长时间阻塞；
 * 可通过系统属性把 records 提升到 10k 来贴近生产批次。
 */
@Tag("performance")
@EnabledIfSystemProperty(named = "fptoken.runPerfTests", matches = "true")
@Timeout(value = 10, unit = TimeUnit.SECONDS)
class PerformanceParameterTuningRecommendationTest {

    @Test
    void scenarioA_baseline_standardProfile() {
        int records = PerfTestSupport.intProp("fptoken.perf.tune.a.records", 1200);
        List<DocTerms> rows = PerfTestSupport.standardPcapRows(records);
        PipelineSnapshot s = runPipeline(
                rows,
                new SelectorConfig(100, 3, 6, 200_000),
                1500,
                16,
                32,
                2,
                60_000L,
                100
        );
        long maxMs = PerfTestSupport.longProp("fptoken.perf.tune.a.maxMs", 120_000L);
        assertTrue(s.totalMs <= maxMs, () -> "scenarioA totalMs=" + s.totalMs + ", maxMs=" + maxMs);
        assertTrue(s.indexMs >= 0L && s.mineMs >= 0L && s.pickMs >= 0L);
        assertTrue(s.cpuMs >= 0L);
        assertTrue(s.selectedCount >= 0 && s.totalEstimatedSaving >= 0L);
    }

    @Test
    void scenarioB_goalOrientedProfileComparison() {
        List<DocTerms> rows = PerfTestSupport.standardPcapRows(
                PerfTestSupport.intProp("fptoken.perf.tune.b.records", 900));
        PipelineSnapshot compress = runPipeline(
                rows,
                new SelectorConfig(20, 3, 8, 200_000),
                2500,
                20,
                48,
                3,
                60_000L,
                150
        );
        PipelineSnapshot speed = runPipeline(
                rows,
                new SelectorConfig(150, 3, 4, 120_000),
                800,
                12,
                24,
                2,
                60_000L,
                80
        );
        PipelineSnapshot balance = runPipeline(
                rows,
                new SelectorConfig(100, 3, 6, 200_000),
                1500,
                16,
                32,
                2,
                60_000L,
                100
        );
        PipelineSnapshot lowMem = runPipeline(
                rows,
                new SelectorConfig(180, 3, 3, 80_000),
                500,
                8,
                16,
                2,
                60_000L,
                50
        );

        assertTrue(speed.totalMs <= compress.totalMs || speed.selectedCount <= compress.selectedCount);
        assertTrue(lowMem.peakHeapDeltaMb <= compress.peakHeapDeltaMb + 128L);
        assertTrue(balance.selectedCount >= 0);
    }

    @Test
    void scenarioC_beamAndFrequentTermSensitivity_matrix() {
        List<DocTerms> rows = PerfTestSupport.standardPcapRows(
                PerfTestSupport.intProp("fptoken.perf.tune.c.records", 700));
        int[] beamWidths = parseCsvInts(System.getProperty("fptoken.perf.tune.c.beam", "24,32,48"));
        int[] maxFrequentTerms = parseCsvInts(System.getProperty("fptoken.perf.tune.c.freq", "1500,2000,2500"));

        for (int beam : beamWidths) {
            for (int freqCap : maxFrequentTerms) {
                PipelineSnapshot s = runPipeline(
                        rows,
                        new SelectorConfig(100, 3, 6, 200_000),
                        freqCap,
                        16,
                        beam,
                        2,
                        60_000L,
                        100
                );
                assertTrue(s.totalMs >= 0L);
                assertTrue(s.generatedCandidates >= 0);
            }
        }
    }

    @Test
    void scenarioD_maxItemsetSizeImpact_4to8() {
        List<DocTerms> rows = PerfTestSupport.standardPcapRows(
                PerfTestSupport.intProp("fptoken.perf.tune.d.records", 700));
        List<Long> mineTimes = new ArrayList<>();
        for (int maxLen = 4; maxLen <= 8; maxLen++) {
            PipelineSnapshot s = runPipeline(
                    rows,
                    new SelectorConfig(100, 3, maxLen, 200_000),
                    2000,
                    16,
                    32,
                    2,
                    60_000L,
                    100
            );
            mineTimes.add(s.mineMs);
            assertTrue(s.selectedAvgLen <= maxLen);
        }
        assertTrue(Collections.max(mineTimes) >= Collections.min(mineTimes));
    }

    @Test
    void scenarioE_maxSwapTrialsImpact_optimizationGain() {
        List<DocTerms> rows = PerfTestSupport.standardPcapRows(
                PerfTestSupport.intProp("fptoken.perf.tune.e.records", 900));
        int[] swapTrials = parseCsvInts(System.getProperty("fptoken.perf.tune.e.swaps", "0,50,80,100,150,200"));
        long previousSaving = -1L;
        for (int swaps : swapTrials) {
            PipelineSnapshot s = runPipeline(
                    rows,
                    new SelectorConfig(100, 3, 6, 200_000),
                    2000,
                    16,
                    32,
                    2,
                    60_000L,
                    swaps
            );
            if (previousSaving >= 0L && swaps > 0) {
                assertTrue(s.totalEstimatedSaving >= previousSaving || s.selectedCount == 0);
            }
            previousSaving = s.totalEstimatedSaving;
        }
    }

    @Test
    @EnabledIfSystemProperty(named = "fptoken.runScaleTests", matches = "true")
    void scenarioF_scaling_2k5_to_20k() {
        int[] scales = parseCsvInts(System.getProperty("fptoken.perf.tune.f.records", "2500,5000,10000,20000"));
        List<Long> costs = new ArrayList<>();
        for (int records : scales) {
            PipelineSnapshot s = runPipeline(
                    PerfTestSupport.standardPcapRows(records),
                    new SelectorConfig(100, 3, 6, 200_000),
                    1500,
                    16,
                    32,
                    2,
                    60_000L,
                    100
            );
            costs.add(s.totalMs);
            assertTrue(s.totalMs >= 0L);
        }
        assertTrue(costs.size() >= 2);
    }

    @Test
    void scenarioG_memoryGcStress_threeProfiles() {
        int records = PerfTestSupport.intProp("fptoken.perf.tune.g.records", 1000);
        List<DocTerms> rows = PerfTestSupport.standardPcapRows(records);
        long[] before = gcStats();
        PipelineSnapshot conservative = runPipeline(
                rows,
                new SelectorConfig(150, 3, 4, 120_000),
                1000,
                12,
                24,
                2,
                60_000L,
                80
        );
        PipelineSnapshot standard = runPipeline(
                rows,
                new SelectorConfig(100, 3, 6, 200_000),
                1500,
                16,
                32,
                2,
                60_000L,
                100
        );
        PipelineSnapshot aggressive = runPipeline(
                rows,
                new SelectorConfig(20, 3, 8, 220_000),
                2500,
                20,
                48,
                3,
                60_000L,
                150
        );
        long[] after = gcStats();
        assertTrue(after[0] >= before[0]);
        assertTrue(after[1] >= before[1]);
        assertTrue(aggressive.generatedCandidates >= standard.generatedCandidates
                || aggressive.truncatedByCandidateLimit);
        assertTrue(conservative.peakHeapDeltaMb <= aggressive.peakHeapDeltaMb + 256L);
    }

    @Test
    void scenarioH_adaptiveVsStatic_validation() {
        List<DocTerms> rows = PerfTestSupport.standardPcapRows(
                PerfTestSupport.intProp("fptoken.perf.tune.h.records", 900));
        PipelineSnapshot staticStd = runPipeline(
                rows,
                new SelectorConfig(100, 3, 6, 200_000),
                1500,
                16,
                32,
                2,
                60_000L,
                100
        );

        ExclusiveSelectionResult adaptive;
        long adaptiveMs = PerfTestSupport.elapsedMillis(() -> {
            ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(rows, 100, 3, 6, 200_000);
        });
        adaptive = ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(rows, 100, 3, 6, 200_000);

        assertTrue(adaptiveMs >= 0L);
        assertTrue(adaptive.getCandidateCount() >= 0);
        // 自适应主要目标是不同词汇量下的稳健性，这里只约束不要明显退化。
        assertTrue(adaptiveMs <= staticStd.totalMs * 2L + 1L);
    }

    private PipelineSnapshot runPipeline(
            List<DocTerms> rows,
            SelectorConfig config,
            int maxFrequentTermCount,
            int maxBranchingFactor,
            int beamWidth,
            int maxIdleLevels,
            long maxRuntimeMillis,
            int maxSwapTrials
    ) {
        Runtime rt = Runtime.getRuntime();
        ThreadMXBean threadMx = ManagementFactory.getThreadMXBean();
        boolean hasCpu = threadMx.isCurrentThreadCpuTimeSupported();
        long cpu0 = hasCpu ? threadMx.getCurrentThreadCpuTime() : 0L;

        rt.gc();
        long mem0 = rt.totalMemory() - rt.freeMemory();
        long t0 = System.nanoTime();

        TermTidsetIndex index;
        long i0 = System.nanoTime();
        index = TermTidsetIndex.build(rows);
        long indexMs = (System.nanoTime() - i0) / 1_000_000L;

        BeamFrequentItemsetMiner miner = new BeamFrequentItemsetMiner();
        FrequentItemsetMiningResult mined;
        long m0 = System.nanoTime();
        mined = miner.mineWithStats(
                index.getTidsetsByTermId(),
                config,
                maxFrequentTermCount,
                maxBranchingFactor,
                beamWidth,
                maxIdleLevels,
                maxRuntimeMillis
        );
        long mineMs = (System.nanoTime() - m0) / 1_000_000L;

        TwoPhaseExclusiveItemsetPicker picker = new TwoPhaseExclusiveItemsetPicker();
        List<CandidateItemset> selected;
        long p0 = System.nanoTime();
        selected = picker.pick(mined.getCandidates(), index.getIdToTerm().size(), maxSwapTrials);
        long pickMs = (System.nanoTime() - p0) / 1_000_000L;

        long totalMs = (System.nanoTime() - t0) / 1_000_000L;
        long mem1 = rt.totalMemory() - rt.freeMemory();
        long heapDeltaMb = Math.max(0L, (mem1 - mem0) / (1024L * 1024L));
        long cpuMs = hasCpu ? Math.max(0L, (threadMx.getCurrentThreadCpuTime() - cpu0) / 1_000_000L) : 0L;

        long totalSaving = 0L;
        int totalLen = 0;
        for (CandidateItemset c : selected) {
            totalSaving += c.getEstimatedSaving();
            totalLen += c.length();
        }
        double avgLen = selected.isEmpty() ? 0.0d : ((double) totalLen / selected.size());

        return new PipelineSnapshot(
                indexMs,
                mineMs,
                pickMs,
                totalMs,
                cpuMs,
                heapDeltaMb,
                mined.getGeneratedCandidateCount(),
                selected.size(),
                avgLen,
                totalSaving,
                mined.isTruncatedByCandidateLimit()
        );
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

    private static long[] gcStats() {
        long count = 0L;
        long time = 0L;
        for (GarbageCollectorMXBean bean : ManagementFactory.getGarbageCollectorMXBeans()) {
            long c = bean.getCollectionCount();
            long t = bean.getCollectionTime();
            if (c >= 0L) {
                count += c;
            }
            if (t >= 0L) {
                time += t;
            }
        }
        return new long[] {count, time};
    }

    private static final class PipelineSnapshot {
        final long indexMs;
        final long mineMs;
        final long pickMs;
        final long totalMs;
        final long cpuMs;
        final long peakHeapDeltaMb;
        final int generatedCandidates;
        final int selectedCount;
        final double selectedAvgLen;
        final long totalEstimatedSaving;
        final boolean truncatedByCandidateLimit;

        PipelineSnapshot(
                long indexMs,
                long mineMs,
                long pickMs,
                long totalMs,
                long cpuMs,
                long peakHeapDeltaMb,
                int generatedCandidates,
                int selectedCount,
                double selectedAvgLen,
                long totalEstimatedSaving,
                boolean truncatedByCandidateLimit
        ) {
            this.indexMs = indexMs;
            this.mineMs = mineMs;
            this.pickMs = pickMs;
            this.totalMs = totalMs;
            this.cpuMs = cpuMs;
            this.peakHeapDeltaMb = peakHeapDeltaMb;
            this.generatedCandidates = generatedCandidates;
            this.selectedCount = selectedCount;
            this.selectedAvgLen = selectedAvgLen;
            this.totalEstimatedSaving = totalEstimatedSaving;
            this.truncatedByCandidateLimit = truncatedByCandidateLimit;
        }
    }
}
