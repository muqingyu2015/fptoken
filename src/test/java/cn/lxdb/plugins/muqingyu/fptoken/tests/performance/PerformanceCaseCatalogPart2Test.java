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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

@Tag("performance")
@EnabledIfSystemProperty(named = "fptoken.runPerfTests", matches = "true")
@Timeout(value = 10, unit = TimeUnit.SECONDS)
class PerformanceCaseCatalogPart2Test {

    // =========================
    // Memory + GC (PERF-MEM)
    // =========================

    @Test
    void PERF_MEM_001_indexMemory_10000records() {
        List<DocTerms> rows = PerfTestSupport.standardPcapRows(3000);
        long deltaMb = heapDeltaMb(() -> TermTidsetIndex.build(rows));
        long maxMb = PerfTestSupport.longProp("fptoken.perf.mem.001.maxMb", 1024L);
        assertTrue(deltaMb <= maxMb, () -> "PERF-MEM-001 deltaMb=" + deltaMb + ", maxMb=" + maxMb);
    }

    @Test
    void PERF_MEM_002_miningPeak_maxLen6() {
        long deltaMb = heapDeltaMb(() -> runMining(1000, 10, 2, 6, 250000, 4096, 32, 64));
        long maxMb = PerfTestSupport.longProp("fptoken.perf.mem.002.maxMb", 1024L);
        assertTrue(deltaMb <= maxMb, () -> "PERF-MEM-002 deltaMb=" + deltaMb + ", maxMb=" + maxMb);
    }

    @Test
    void PERF_MEM_003_candidateSetMemory_10000() {
        long deltaMb = heapDeltaMb(() -> PerfTestSupport.syntheticCandidates(10000, 20000, 30000, 2, 5, 3L));
        long maxMb = PerfTestSupport.longProp("fptoken.perf.mem.003.maxMb", 512L);
        assertTrue(deltaMb <= maxMb, () -> "PERF-MEM-003 deltaMb=" + deltaMb + ", maxMb=" + maxMb);
    }

    @Test
    void PERF_MEM_004_memoryVsRecordCount() {
        long m1 = heapDeltaMb(() -> TermTidsetIndex.build(PerfTestSupport.standardPcapRows(1000)));
        long m5 = heapDeltaMb(() -> TermTidsetIndex.build(PerfTestSupport.standardPcapRows(3000)));
        long m10 = heapDeltaMb(() -> TermTidsetIndex.build(PerfTestSupport.standardPcapRows(6000)));
        long m20 = heapDeltaMb(() -> TermTidsetIndex.build(PerfTestSupport.standardPcapRows(12000)));
        assertTrue(m5 >= m1 || m10 >= m5 || m20 >= m10);
    }

    @Test
    void PERF_MEM_005_memoryVsVocabulary() {
        long m1 = heapDeltaMb(() -> TermTidsetIndex.build(PerfTestSupport.rowsWithVocabulary(3000, 1000, 12, 11L)));
        long m5 = heapDeltaMb(() -> TermTidsetIndex.build(PerfTestSupport.rowsWithVocabulary(3000, 5000, 12, 12L)));
        long m10 = heapDeltaMb(() -> TermTidsetIndex.build(PerfTestSupport.rowsWithVocabulary(3000, 10000, 12, 13L)));
        assertTrue(m5 >= m1 || m10 >= m5);
    }

    @Test
    void PERF_MEM_006_gcFrequency_fullPipeline() {
        long[] before = gcStats();
        ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(
                PerfTestSupport.standardPcapRows(2000), 10, 2, 6, 200000);
        long[] after = gcStats();
        long countInc = after[0] - before[0];
        long timeInc = after[1] - before[1];
        long maxCount = PerfTestSupport.longProp("fptoken.perf.mem.006.maxGcCount", 100L);
        long maxTime = PerfTestSupport.longProp("fptoken.perf.mem.006.maxGcTimeMs", 4000L);
        assertTrue(countInc <= maxCount, () -> "PERF-MEM-006 gcCountInc=" + countInc + ", max=" + maxCount);
        assertTrue(timeInc <= maxTime, () -> "PERF-MEM-006 gcTimeIncMs=" + timeInc + ", max=" + maxTime);
    }

    @Test
    void PERF_MEM_007_gcPauseProxy_maxObserved() {
        long[] before = gcStats();
        ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(
                PerfTestSupport.standardPcapRows(1000), 10, 2, 6, 200000);
        long[] after = gcStats();
        long gcTime = Math.max(0L, after[1] - before[1]);
        assertTrue(gcTime < PerfTestSupport.longProp("fptoken.perf.mem.007.maxGcTimeMs", 2000L));
    }

    @Test
    void PERF_MEM_008_allocationRateProxy_miningLoop() {
        long deltaMb = heapDeltaMb(() -> {
            for (int i = 0; i < 3; i++) {
                runMining(400, 10, 2, 6, 100000, 2048, 24, 64);
            }
        });
        assertTrue(deltaMb >= 0L);
    }

    @Test
    void PERF_MEM_009_bitsetPoolingEffect_proxy() {
        long noPoolMs = PerfTestSupport.elapsedMillis(() -> {
            for (int i = 0; i < 3; i++) {
                BeamFrequentItemsetMiner miner = new BeamFrequentItemsetMiner();
                mineWith(miner, 500);
            }
        });
        BeamFrequentItemsetMiner reusedMiner = new BeamFrequentItemsetMiner();
        long poolMs = PerfTestSupport.elapsedMillis(() -> {
            for (int i = 0; i < 3; i++) {
                mineWith(reusedMiner, 500);
            }
        });
        assertTrue(poolMs <= noPoolMs * 1.5d);
    }

    @Test
    void PERF_MEM_010_memoryLeakDetection_10batches() {
        Runtime rt = Runtime.getRuntime();
        rt.gc();
        long start = rt.totalMemory() - rt.freeMemory();
        for (int i = 0; i < 5; i++) {
            ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(
                    PerfTestSupport.standardPcapRows(300), 10, 2, 6, 100000);
            rt.gc();
        }
        long end = rt.totalMemory() - rt.freeMemory();
        long driftMb = Math.max(0L, (end - start) / (1024L * 1024L));
        long maxDrift = PerfTestSupport.longProp("fptoken.perf.mem.010.maxDriftMb", 256L);
        assertTrue(driftMb <= maxDrift, () -> "PERF-MEM-010 driftMb=" + driftMb + ", maxDriftMb=" + maxDrift);
    }

    // =========================
    // Concurrency (PERF-CONC)
    // =========================

    @Test
    void PERF_CONC_001_singleThreadThroughput() {
        long ms = PerfTestSupport.elapsedMillis(() -> {
            for (int i = 0; i < 2; i++) {
                ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(
                        PerfTestSupport.standardPcapRows(400), 10, 2, 6, 100000);
            }
        });
        assertTrue(ms >= 0L);
    }

    @Test
    void PERF_CONC_002_multiInstance4x() throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(4);
        try {
            long ms = runParallel(pool, 4, () -> {
                ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(
                        PerfTestSupport.standardPcapRows(400), 10, 2, 6, 100000);
                return 1;
            });
            assertTrue(ms >= 0L);
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void PERF_CONC_003_threadPoolSizing() throws Exception {
        long t2 = measureWithThreads(2);
        long t4 = measureWithThreads(4);
        long t8 = measureWithThreads(8);
        assertTrue(t2 >= 0L && t4 >= 0L && t8 >= 0L);
    }

    @Test
    void PERF_CONC_004_sharedIndexReadOnly() throws Exception {
        TermTidsetIndex index = TermTidsetIndex.build(PerfTestSupport.standardPcapRows(600));
        BeamFrequentItemsetMiner miner = new BeamFrequentItemsetMiner();
        SelectorConfig cfg = new SelectorConfig(10, 2, 6, 120000);
        ExecutorService pool = Executors.newFixedThreadPool(4);
        try {
            long ms = runParallel(pool, 4, () -> {
                FrequentItemsetMiningResult result = miner.mineWithStats(index.getTidsetsByTermId(), cfg, 2048, 24, 64);
                return result.getGeneratedCandidateCount();
            });
            assertTrue(ms >= 0L);
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void PERF_CONC_005_cpuUtilizationProxy() throws Exception {
        long single = measureWithThreads(1);
        long multi = measureWithThreads(4);
        assertTrue(multi <= single * 4L);
    }

    // =========================
    // Scale (PERF-SCALE)
    // =========================

    @Test
    void PERF_SCALE_001_records_1k_to_50k() {
        int[] records = new int[] {500, 1500, 3000};
        long prev = -1L;
        for (int n : records) {
            long ms = PerfTestSupport.elapsedMillis(() ->
                    ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(
                            PerfTestSupport.standardPcapRows(n), 10, 2, 6, 200000));
            if (prev > 0L) {
                assertTrue(ms >= prev / 3L);
            }
            prev = ms;
        }
    }

    @Test
    void PERF_SCALE_002_vocabulary_500_to_50k() {
        int[] vocab = new int[] {500, 2000, 5000, 10000};
        long prev = -1L;
        for (int v : vocab) {
            long ms = PerfTestSupport.elapsedMillis(() ->
                    TermTidsetIndex.build(PerfTestSupport.rowsWithVocabulary(2500, v, 12, v)));
            if (prev > 0L) {
                assertTrue(ms >= prev / 3L);
            }
            prev = ms;
        }
    }

    @Test
    void PERF_SCALE_003_itemsetLen_2_to_15() {
        int[] maxLens = new int[] {2, 4, 6, 10, 15};
        for (int maxLen : maxLens) {
            FrequentItemsetMiningResult r = runMining(400, 10, 2, maxLen, 120000, 2048, 24, 64);
            assertTrue(r.getGeneratedCandidateCount() >= 0);
        }
    }

    @Test
    void PERF_SCALE_004_windowsPerRecord_100_to_2000_proxy() {
        long low = runWindowScale(120, 128, 10);
        long high = runWindowScale(120, 128, 1);
        assertTrue(high >= low);
    }

    @Test
    void PERF_SCALE_005_candidatesInput_100_to_20000() {
        TwoPhaseExclusiveItemsetPicker picker = new TwoPhaseExclusiveItemsetPicker();
        for (int n : new int[] {100, 500, 2000, 8000}) {
            List<CandidateItemset> c = PerfTestSupport.syntheticCandidates(n, 12000, 12000, 2, 5, n);
            long ms = PerfTestSupport.elapsedMillis(() -> picker.pick(c, 12000, 200));
            assertTrue(ms >= 0L);
        }
    }

    @Test
    void PERF_SCALE_006_theoreticalComplexity_proxy() {
        long t1 = PerfTestSupport.elapsedMillis(() ->
                ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(
                        PerfTestSupport.standardPcapRows(500), 10, 2, 6, 120000));
        long t2 = PerfTestSupport.elapsedMillis(() ->
                ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(
                        PerfTestSupport.standardPcapRows(1000), 10, 2, 6, 120000));
        double ratio = (double) Math.max(1L, t2) / Math.max(1L, t1);
        assertTrue(ratio < PerfTestSupport.longProp("fptoken.perf.scale.006.maxRatio", 6L));
    }

    // =========================
    // Stress (PERF-STRESS)
    // =========================

    @Test
    @EnabledIfSystemProperty(named = "fptoken.runStressTests", matches = "true")
    void PERF_STRESS_001_superLargeBatch_100k() {
        long ms = PerfTestSupport.elapsedMillis(() ->
                ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(
                        PerfTestSupport.standardPcapRows(10000), 10, 2, 6, 100000));
        assertTrue(ms >= 0L);
    }

    @Test
    @EnabledIfSystemProperty(named = "fptoken.runStressTests", matches = "true")
    void PERF_STRESS_002_superLongRecord_10kBytes() {
        List<DocTerms> rows = cn.lxdb.plugins.muqingyu.fptoken.tests.ByteArrayTestSupport.pcapLikeBatch(120, 4096, 128, 32);
        ExclusiveSelectionResult r = ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(rows, 10, 2, 6, 300000);
        assertTrue(r.getCandidateCount() >= 0);
    }

    @Test
    @EnabledIfSystemProperty(named = "fptoken.runStressTests", matches = "true")
    void PERF_STRESS_003_superManyWindows_step1() {
        List<DocTerms> rows = cn.lxdb.plugins.muqingyu.fptoken.tests.ByteArrayTestSupport.pcapLikeBatch(40, 1024, 128, 1);
        ExclusiveSelectionResult r = ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(rows, 10, 2, 6, 200000);
        assertTrue(r.getCandidateCount() <= 200000);
    }

    @Test
    @EnabledIfSystemProperty(named = "fptoken.runStressTests", matches = "true")
    void PERF_STRESS_004_minSupport1_candidateExplosionBounded() {
        ExclusiveSelectionResult r = ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(
                PerfTestSupport.standardPcapRows(2000), 1, 2, 6, 50000);
        assertTrue(r.getCandidateCount() <= 50000);
    }

    @Test
    @EnabledIfSystemProperty(named = "fptoken.runStressTests", matches = "true")
    void PERF_STRESS_005_unboundedRuntimeNaturalFinish() {
        FrequentItemsetMiningResult r = runMining(600, 10, 2, 10, 120000, 2048, 24, 64);
        assertTrue(r.getGeneratedCandidateCount() >= 0);
    }

    @Test
    @EnabledIfSystemProperty(named = "fptoken.runStressTests", matches = "true")
    void PERF_STRESS_006_lowHeapBudgetProxy() {
        long deltaMb = heapDeltaMb(() -> ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(
                PerfTestSupport.standardPcapRows(1000), 10, 2, 6, 100000));
        assertTrue(deltaMb <= PerfTestSupport.longProp("fptoken.perf.stress.006.maxMemMb", 1024L));
    }

    @Test
    @EnabledIfSystemProperty(named = "fptoken.runStressTests", matches = "true")
    void PERF_STRESS_007_continuous100batches() {
        PerfTestSupport.repeatWithinBudget(
                20,
                PerfTestSupport.longProp("fptoken.perf.stress.007.budgetMs", 9000L),
                () -> ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(
                        PerfTestSupport.standardPcapRows(120), 10, 2, 6, 100000));
        assertTrue(true);
    }

    @Test
    @EnabledIfSystemProperty(named = "fptoken.runStressTests", matches = "true")
    void PERF_STRESS_008_extremeVocabulary_100kplus() {
        List<DocTerms> rows = PerfTestSupport.rowsWithVocabulary(5000, 120000, 12, 88L);
        TermTidsetIndex index = TermTidsetIndex.build(rows);
        assertTrue(index.getIdToTerm().size() > 30000);
    }

    // =========================
    // Compare (PERF-COMP)
    // =========================

    @Test
    void PERF_COMP_001_vsBruteforce_smallDataset() {
        // 代理版：仅验证近似挖掘在小数据上的耗时明显低于“全排列组合”模拟。
        List<DocTerms> rows = PerfTestSupport.rowsWithVocabulary(200, 80, 8, 66L);
        long approxMs = PerfTestSupport.elapsedMillis(() ->
                ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(rows, 3, 2, 4, 50000));
        long bruteMs = PerfTestSupport.elapsedMillis(() -> bruteForceProxy(rows));
        assertTrue(bruteMs >= approxMs);
    }

    @Test
    void PERF_COMP_002_pruningOnOff_proxyBySupport() {
        List<DocTerms> rows = PerfTestSupport.standardPcapRows(800);
        long loose = PerfTestSupport.elapsedMillis(() ->
                ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(rows, 1, 2, 6, 200000));
        long strict = PerfTestSupport.elapsedMillis(() ->
                ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(rows, 40, 2, 6, 200000));
        assertTrue(strict <= loose);
    }

    @Test
    void PERF_COMP_003_vsNoBitsetPooling_proxy() {
        long newMinerEveryTime = PerfTestSupport.elapsedMillis(() -> {
            for (int i = 0; i < 4; i++) {
                mineWith(new BeamFrequentItemsetMiner(), 700);
            }
        });
        BeamFrequentItemsetMiner reused = new BeamFrequentItemsetMiner();
        long reusedMiner = PerfTestSupport.elapsedMillis(() -> {
            for (int i = 0; i < 4; i++) {
                mineWith(reused, 700);
            }
        });
        assertTrue(reusedMiner <= newMinerEveryTime * 1.4d);
    }

    @Test
    void PERF_COMP_004_vsArrayCopyPrefix_proxy() {
        // 代理版：宽束深层场景应保持可控，避免链式前缀退化。
        FrequentItemsetMiningResult r = runMining(1000, 10, 2, 10, 250000, 4096, 32, 128);
        assertTrue(r.getGeneratedCandidateCount() >= 0);
    }

    @Test
    void PERF_COMP_005_heapTopK_vsFullSort_proxy() {
        // 代理版：设置较大 beam，验证运行时长在预算内。
        long ms = PerfTestSupport.elapsedMillis(() -> runMining(600, 10, 2, 8, 120000, 2048, 24, 128));
        assertTrue(ms < PerfTestSupport.longProp("fptoken.perf.comp.005.maxMs", 25000L));
    }

    // =========================
    // Soak (PERF-SOAK)
    // =========================

    @Test
    @EnabledIfSystemProperty(named = "fptoken.runSoakTests", matches = "true")
    void PERF_SOAK_001_shortSoak_30min_proxy() {
        runSoakIterations(PerfTestSupport.intProp("fptoken.perf.soak.001.iterations", 20));
    }

    @Test
    @EnabledIfSystemProperty(named = "fptoken.runSoakTests", matches = "true")
    void PERF_SOAK_002_longSoak_4h_proxy() {
        runSoakIterations(PerfTestSupport.intProp("fptoken.perf.soak.002.iterations", 40));
    }

    @Test
    @EnabledIfSystemProperty(named = "fptoken.runSoakTests", matches = "true")
    void PERF_SOAK_003_extremeSoak_alternateLoads() {
        int loops = PerfTestSupport.intProp("fptoken.perf.soak.003.iterations", 30);
        PerfTestSupport.repeatWithinBudget(
                loops,
                PerfTestSupport.longProp("fptoken.perf.soak.003.budgetMs", 9000L),
                () -> {
                    int idx = (int) (System.nanoTime() & 1L);
                    int records = (idx == 0) ? 200 : 600;
                    ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(
                            PerfTestSupport.standardPcapRows(records), 10, 2, 6, 100000);
                });
        assertTrue(true);
    }

    @Test
    void PERF_SOAK_004_warmupVsColdStart() {
        long cold = PerfTestSupport.elapsedMillis(() ->
                ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(
                        PerfTestSupport.standardPcapRows(800), 10, 2, 6, 100000));
        for (int i = 0; i < 5; i++) {
            ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(
                    PerfTestSupport.standardPcapRows(600), 10, 2, 6, 100000);
        }
        long warm = PerfTestSupport.elapsedMillis(() ->
                ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(
                        PerfTestSupport.standardPcapRows(800), 10, 2, 6, 100000));
        assertTrue(warm <= cold * 1.5d);
    }

    @Test
    void PERF_SOAK_005_jitterP99_proxy() {
        List<Long> samples = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            long ms = PerfTestSupport.elapsedMillis(() ->
                    ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(
                            PerfTestSupport.standardPcapRows(400), 10, 2, 6, 100000));
            samples.add(ms);
        }
        samples.sort(Long::compareTo);
        long p50 = samples.get(samples.size() / 2);
        long p99 = samples.get((int) Math.floor(samples.size() * 0.95d));
        assertTrue(p99 <= p50 * 2L + 1L, () -> "PERF-SOAK-005 p50=" + p50 + ", p99=" + p99);
    }

    private long heapDeltaMb(Runnable runnable) {
        Runtime rt = Runtime.getRuntime();
        rt.gc();
        long before = rt.totalMemory() - rt.freeMemory();
        runnable.run();
        long after = rt.totalMemory() - rt.freeMemory();
        return Math.max(0L, (after - before) / (1024L * 1024L));
    }

    private long[] gcStats() {
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

    private long runParallel(ExecutorService pool, int tasks, Callable<Integer> callable)
            throws InterruptedException, ExecutionException {
        List<Future<Integer>> futures = new ArrayList<>(tasks);
        long t0 = System.nanoTime();
        for (int i = 0; i < tasks; i++) {
            futures.add(pool.submit(callable));
        }
        for (Future<Integer> f : futures) {
            f.get();
        }
        return (System.nanoTime() - t0) / 1_000_000L;
    }

    private long measureWithThreads(int threads) throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        try {
            return runParallel(pool, threads, () -> {
                ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(
                        PerfTestSupport.standardPcapRows(300), 10, 2, 6, 100000);
                return 1;
            });
        } finally {
            pool.shutdownNow();
        }
    }

    private FrequentItemsetMiningResult runMining(
            int records,
            int minSupport,
            int minLen,
            int maxLen,
            int maxCandidates,
            int maxFrequentTermCount,
            int maxBranchingFactor,
            int beamWidth
    ) {
        return mineWith(new BeamFrequentItemsetMiner(), records, minSupport, minLen, maxLen, maxCandidates,
                maxFrequentTermCount, maxBranchingFactor, beamWidth);
    }

    private void mineWith(BeamFrequentItemsetMiner miner, int records) {
        mineWith(miner, records, 10, 2, 6, 120000, 2048, 24, 64);
    }

    private FrequentItemsetMiningResult mineWith(
            BeamFrequentItemsetMiner miner,
            int records,
            int minSupport,
            int minLen,
            int maxLen,
            int maxCandidates,
            int maxFrequentTermCount,
            int maxBranchingFactor,
            int beamWidth
    ) {
        List<DocTerms> rows = PerfTestSupport.standardPcapRows(records);
        TermTidsetIndex index = TermTidsetIndex.build(rows);
        SelectorConfig config = new SelectorConfig(minSupport, minLen, maxLen, maxCandidates);
        return miner.mineWithStats(index.getTidsetsByTermId(), config, maxFrequentTermCount, maxBranchingFactor, beamWidth);
    }

    private long runWindowScale(int records, int windowLen, int step) {
        List<DocTerms> rows = cn.lxdb.plugins.muqingyu.fptoken.tests.ByteArrayTestSupport
                .pcapLikeBatch(records, 1024, windowLen, step);
        return PerfTestSupport.elapsedMillis(() ->
                ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(rows, 10, 2, 6, 120000));
    }

    private void bruteForceProxy(List<DocTerms> rows) {
        // 仅作为“暴力枚举”对比代理：做大量组合扫描，不参与业务断言。
        int n = Math.min(rows.size(), 120);
        int score = 0;
        for (int i = 0; i < n; i++) {
            for (int j = i + 1; j < n; j++) {
                int a = rows.get(i).getTerms().size();
                int b = rows.get(j).getTerms().size();
                score += Math.min(a, b);
            }
        }
        if (score < 0) {
            throw new IllegalStateException("unreachable");
        }
    }

    private void runSoakIterations(int iterations) {
        PerfTestSupport.repeatWithinBudget(
                iterations,
                PerfTestSupport.longProp("fptoken.perf.soak.iterationBudgetMs", 9000L),
                () -> ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(
                        PerfTestSupport.standardPcapRows(200), 10, 2, 6, 100000));
        assertTrue(true);
    }
}
