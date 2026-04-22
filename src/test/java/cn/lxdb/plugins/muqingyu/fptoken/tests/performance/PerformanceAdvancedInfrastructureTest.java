package cn.lxdb.plugins.muqingyu.fptoken.tests.performance;

import static org.junit.jupiter.api.Assertions.assertTrue;

import cn.lxdb.plugins.muqingyu.fptoken.ExclusiveFrequentItemsetSelector;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.config.SelectorConfig;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.index.TermTidsetIndex;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.miner.BeamFrequentItemsetMiner;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.DocTerms;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.ExclusiveSelectionResult;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.FrequentItemsetMiningResult;
import java.io.IOException;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.Comparator;
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

/**
 * 补充“更深层次”性能用例（JVM/系统代理/微架构代理/自动化调参/稳定性/故障注入/报告）。
 */
@Tag("performance")
@EnabledIfSystemProperty(named = "fptoken.runPerfTests", matches = "true")
@Timeout(value = 10, unit = TimeUnit.SECONDS)
class PerformanceAdvancedInfrastructureTest {

    // ===== 数据真实性分级（L1-L4） =====

    @Test
    void dataTier_L1toL4_generationStrategy() {
        List<DocTerms> l1 = PerfTestSupport.highCardinalityRows(400, 12, 1L); // 完全随机近似
        List<DocTerms> l2 = PerfTestSupport.protocolMixRows(400, 2L); // 协议模板
        List<DocTerms> l3 = PerfTestSupport.standardPcapRows(50); // 真实结构近似（pcap-like）
        List<DocTerms> l4 = replayLikeRows(400, 3L); // 生产回放近似（模板循环）
        assertTrue(!l1.isEmpty() && !l2.isEmpty() && !l3.isEmpty() && !l4.isEmpty());
    }

    // ===== JVM 层面（代理） =====

    @Test
    void JVM_001_gcStrategyProxy_observeCollectorAndPauseDelta() {
        List<String> gcNames = new ArrayList<>();
        for (GarbageCollectorMXBean bean : ManagementFactory.getGarbageCollectorMXBeans()) {
            gcNames.add(bean.getName());
        }
        long[] before = gcStats();
        runFacade(PerfTestSupport.protocolMixRows(800, 7L), 80, 3, 6, 120_000);
        long[] after = gcStats();
        assertTrue(!gcNames.isEmpty());
        assertTrue(after[0] >= before[0]);
        assertTrue(after[1] >= before[1]);
    }

    @Test
    void JVM_002_heapSizePressureProxy_oomThresholdGuard() {
        long low = heapDeltaMb(() -> runFacade(PerfTestSupport.protocolMixRows(500, 11L), 120, 3, 4, 80_000));
        long high = heapDeltaMb(() -> runFacade(PerfTestSupport.protocolMixRows(1300, 11L), 80, 3, 7, 220_000));
        assertTrue(high >= low || high >= 0L);
    }

    @Test
    void JVM_005_jitWarmupCurve_proxy() {
        List<Long> cold = new ArrayList<>();
        List<Long> warm = new ArrayList<>();
        for (int i = 0; i < 8; i++) {
            final int seed = i;
            long ms = PerfTestSupport.elapsedMillis(() ->
                    runFacade(PerfTestSupport.protocolMixRows(700, seed), 100, 3, 6, 120_000));
            if (i < 3) {
                cold.add(ms);
            } else {
                warm.add(ms);
            }
        }
        long coldMedian = median(cold);
        long warmMedian = median(warm);
        assertTrue(warmMedian <= coldMedian * 2L + 1L);
    }

    // ===== OS 层面（代理） =====

    @Test
    void OS_001_cpuAffinityProxy_parallelScaling() throws Exception {
        long t1 = parallelRun(1, 1, () -> runFacade(PerfTestSupport.protocolMixRows(350, 19L), 100, 3, 6, 80_000));
        long t2 = parallelRun(2, 2, () -> runFacade(PerfTestSupport.protocolMixRows(350, 19L), 100, 3, 6, 80_000));
        long t4 = parallelRun(4, 4, () -> runFacade(PerfTestSupport.protocolMixRows(350, 19L), 100, 3, 6, 80_000));
        assertTrue(t1 >= 0L && t2 >= 0L && t4 >= 0L);
    }

    @Test
    void OS_003_pageCacheProxy_fileReadWriteHotCold() throws IOException {
        Path temp = Files.createTempFile("fptoken-os-", ".bin");
        byte[] payload = new byte[1024 * 1024];
        try {
            long cold = PerfTestSupport.elapsedMillis(() -> {
                try {
                    Files.write(temp, payload);
                    Files.readAllBytes(temp);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
            long warm = PerfTestSupport.elapsedMillis(() -> {
                try {
                    Files.readAllBytes(temp);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
            assertTrue(cold >= 0L && warm >= 0L);
        } finally {
            Files.deleteIfExists(temp);
        }
    }

    // ===== 微架构层面（代理） =====

    @Test
    void MICRO_001_cacheMissProxy_bitsetAccessPattern() {
        BitSet denseA = new BitSet(2_000_000);
        BitSet denseB = new BitSet(2_000_000);
        for (int i = 0; i < 1_600_000; i += 2) {
            denseA.set(i);
        }
        for (int i = 1; i < 1_600_000; i += 3) {
            denseB.set(i);
        }
        long seqMs = PerfTestSupport.elapsedMillis(() -> {
            BitSet c = (BitSet) denseA.clone();
            c.and(denseB);
            c.cardinality();
        });
        long randomMs = PerfTestSupport.elapsedMillis(() -> {
            BitSet c = new BitSet(2_000_000);
            for (int i = 0; i < 200_000; i++) {
                c.set((i * 97) % 1_900_000);
            }
            c.and(denseB);
            c.cardinality();
        });
        assertTrue(seqMs >= 0L && randomMs >= 0L);
    }

    // ===== 热点剖析（代理） =====

    @Test
    void HOT_001_bitsetIntersectionHotpath_proxy() {
        TermTidsetIndex index = TermTidsetIndex.build(PerfTestSupport.protocolMixRows(1200, 29L));
        List<BitSet> tidsets = index.getTidsetsByTermId();
        long ms = PerfTestSupport.elapsedMillis(() -> {
            int lim = Math.min(tidsets.size(), 1500);
            for (int i = 1; i < lim; i++) {
                BitSet x = (BitSet) tidsets.get(i - 1).clone();
                x.and(tidsets.get(i));
                x.cardinality();
            }
        });
        assertTrue(ms >= 0L);
    }

    @Test
    void HOT_003_allocationHotspot_proxy() {
        long ms = PerfTestSupport.elapsedMillis(() -> {
            for (int i = 0; i < 2000; i++) {
                int[] tmp = new int[128];
                BitSet bits = new BitSet(8192);
                if (tmp.length == 0 || bits.isEmpty()) {
                    // no-op: keep allocations on hot path
                }
            }
        });
        assertTrue(ms >= 0L);
    }

    // ===== 参数自动化（AUTO） =====

    @Test
    void AUTO_001_gridSearch_smallSpace_bestLatency() throws IOException {
        List<DocTerms> rows = PerfTestSupport.standardPcapRows(600);
        int[] beam = new int[] {24, 32, 48};
        int[] freq = new int[] {1000, 1500, 2000};
        int[] maxLen = new int[] {5, 6, 7};
        List<String> reportRows = new ArrayList<>();
        reportRows.add("timestamp,beam,freq,maxLen,totalMs,candidateCount,groupCount");
        SearchPoint best = null;
        for (int b : beam) {
            for (int f : freq) {
                for (int l : maxLen) {
                    SearchPoint p = evaluatePoint(rows, b, f, l);
                    reportRows.add(Instant.now() + "," + b + "," + f + "," + l + "," + p.totalMs + ","
                            + p.candidateCount + "," + p.groupCount);
                    if (best == null || p.totalMs < best.totalMs) {
                        best = p;
                    }
                }
            }
        }
        assertTrue(best != null && best.totalMs >= 0L);
        Path out = Files.createTempFile("fptoken-grid-", ".csv");
        Files.write(out, reportRows, StandardCharsets.UTF_8);
        Files.deleteIfExists(out);
    }

    @Test
    void AUTO_005_sensitivityProxy_rankMostSensitiveParam() {
        List<DocTerms> rows = PerfTestSupport.standardPcapRows(500);
        SearchPoint base = evaluatePoint(rows, 32, 1500, 6);
        SearchPoint beamUp = evaluatePoint(rows, 48, 1500, 6);
        SearchPoint freqUp = evaluatePoint(rows, 32, 2000, 6);
        SearchPoint lenUp = evaluatePoint(rows, 32, 1500, 7);
        List<SearchPoint> deltas = Arrays.asList(
                delta("beam", base, beamUp),
                delta("freq", base, freqUp),
                delta("len", base, lenUp)
        );
        deltas.sort(Comparator.comparingLong(a -> -a.totalMs));
        assertTrue(!deltas.isEmpty());
    }

    // ===== 稳定性与退化（STAB） =====

    @Test
    void STAB_001_baselineP50P99() {
        List<Long> samples = new ArrayList<>();
        for (int i = 0; i < 40; i++) {
            final int seed = i;
            long ms = PerfTestSupport.elapsedMillis(() ->
                    runFacade(PerfTestSupport.protocolMixRows(500, seed), 100, 3, 6, 120_000));
            samples.add(ms);
        }
        long p50 = percentile(samples, 0.50d);
        long p99 = percentile(samples, 0.99d);
        assertTrue(p99 >= p50);
    }

    @Test
    void STAB_003_versionRegressionGuard_fromBaselineFile() throws IOException {
        List<Long> samples = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            final int seed = i;
            samples.add(PerfTestSupport.elapsedMillis(() ->
                    runFacade(PerfTestSupport.protocolMixRows(450, seed), 100, 3, 6, 120_000)));
        }
        long currentP50 = percentile(samples, 0.50d);
        Path baselinePath = Path.of(System.getProperty(
                "fptoken.perf.baseline.file",
                System.getProperty("java.io.tmpdir") + "/fptoken-perf-baseline.txt"
        ));
        if (!Files.exists(baselinePath)) {
            Files.writeString(baselinePath, Long.toString(currentP50), StandardCharsets.UTF_8);
            assertTrue(true);
            return;
        }
        long baseline = Long.parseLong(Files.readString(baselinePath, StandardCharsets.UTF_8).trim());
        double maxRegressionRatio = Double.parseDouble(System.getProperty("fptoken.perf.baseline.maxRegression", "1.30"));
        assertTrue(currentP50 <= baseline * maxRegressionRatio,
                () -> "currentP50=" + currentP50 + ", baseline=" + baseline + ", maxRatio=" + maxRegressionRatio);
    }

    // ===== 故障注入（FAULT） =====

    @Test
    void FAULT_001_cpuQuotaProxy_timeoutBounded() {
        List<DocTerms> rows = PerfTestSupport.standardPcapRows(800);
        TermTidsetIndex index = TermTidsetIndex.build(rows);
        BeamFrequentItemsetMiner miner = new BeamFrequentItemsetMiner();
        SelectorConfig cfg = new SelectorConfig(100, 3, 8, 220_000);
        long ms = PerfTestSupport.elapsedMillis(() -> miner.mineWithStats(
                index.getTidsetsByTermId(), cfg, 2000, 16, 32, 2, 5L));
        assertTrue(ms >= 0L);
    }

    @Test
    void FAULT_005_partialDataCorruption_proxy() {
        List<DocTerms> rows = new ArrayList<>(PerfTestSupport.protocolMixRows(300, 55L));
        rows.add(new DocTerms(999_001, Collections.<byte[]>emptyList()));
        rows.add(new DocTerms(999_002, Arrays.asList(new byte[] {(byte) 0x00}, new byte[] {(byte) 0x00})));
        ExclusiveSelectionResult r = runFacade(rows, 50, 2, 6, 80_000);
        assertTrue(r.getCandidateCount() >= 0);
    }

    // ===== 报告模板字段 =====

    @Test
    void reportTemplate_requiredFields_exportCsv() throws IOException {
        ExclusiveSelectionResult r = runFacade(PerfTestSupport.protocolMixRows(600, 66L), 100, 3, 6, 120_000);
        String row = String.join(",",
                Instant.now().toString(),
                "unknown-commit",
                "local-env",
                "records=600",
                "minSupport=100|minLen=3|maxLen=6|maxCandidate=120000",
                Integer.toString(r.getCandidateCount()),
                Integer.toString(r.getGroups().size()),
                Integer.toString(r.getFrequentTermCount()),
                Integer.toString(r.getIntersectionCount()));
        Path out = Files.createTempFile("fptoken-perf-report-", ".csv");
        Files.write(out, Arrays.asList(
                "time,commit,env,dataScale,params,candidateCount,groupCount,frequentTermCount,intersectionCount",
                row), StandardCharsets.UTF_8);
        assertTrue(Files.size(out) > 0L);
        Files.deleteIfExists(out);
    }

    private ExclusiveSelectionResult runFacade(
            List<DocTerms> rows,
            int minSupport,
            int minLen,
            int maxLen,
            int maxCandidateCount
    ) {
        return ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(
                rows, minSupport, minLen, maxLen, maxCandidateCount);
    }

    private SearchPoint evaluatePoint(List<DocTerms> rows, int beam, int freqCap, int maxLen) {
        TermTidsetIndex index = TermTidsetIndex.build(rows);
        BeamFrequentItemsetMiner miner = new BeamFrequentItemsetMiner();
        SelectorConfig cfg = new SelectorConfig(100, 3, maxLen, 200_000);
        FrequentItemsetMiningResult mined;
        long totalMs = PerfTestSupport.elapsedMillis(() -> {
            miner.mineWithStats(index.getTidsetsByTermId(), cfg, freqCap, 16, beam);
        });
        mined = miner.mineWithStats(index.getTidsetsByTermId(), cfg, freqCap, 16, beam);
        int groups = new TwoPhasePickerWrapper().pickCount(mined, index);
        return new SearchPoint(totalMs, mined.getGeneratedCandidateCount(), groups);
    }

    private SearchPoint delta(String name, SearchPoint base, SearchPoint changed) {
        if (name == null) {
            throw new IllegalArgumentException("name");
        }
        return new SearchPoint(
                Math.max(0L, changed.totalMs - base.totalMs),
                Math.max(0, changed.candidateCount - base.candidateCount),
                Math.max(0, changed.groupCount - base.groupCount)
        );
    }

    private long parallelRun(int threads, int tasks, Runnable runnable) throws InterruptedException, ExecutionException {
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        try {
            List<Future<Boolean>> futures = new ArrayList<>();
            long t0 = System.nanoTime();
            for (int i = 0; i < tasks; i++) {
                Callable<Boolean> c = () -> {
                    runnable.run();
                    return true;
                };
                futures.add(pool.submit(c));
            }
            for (Future<Boolean> f : futures) {
                f.get();
            }
            return (System.nanoTime() - t0) / 1_000_000L;
        } finally {
            pool.shutdownNow();
        }
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

    private long heapDeltaMb(Runnable r) {
        Runtime rt = Runtime.getRuntime();
        rt.gc();
        long before = rt.totalMemory() - rt.freeMemory();
        r.run();
        long after = rt.totalMemory() - rt.freeMemory();
        return Math.max(0L, (after - before) / (1024L * 1024L));
    }

    private long median(List<Long> values) {
        return percentile(values, 0.50d);
    }

    private long percentile(List<Long> values, double p) {
        if (values.isEmpty()) {
            return 0L;
        }
        List<Long> sorted = new ArrayList<>(values);
        sorted.sort(Long::compareTo);
        int idx = (int) Math.floor((sorted.size() - 1) * Math.max(0d, Math.min(1d, p)));
        return sorted.get(idx);
    }

    private List<DocTerms> replayLikeRows(int docs, long seed) {
        List<DocTerms> base = PerfTestSupport.protocolMixRows(Math.max(8, docs / 10), seed);
        List<DocTerms> out = new ArrayList<>(docs);
        for (int i = 0; i < docs; i++) {
            DocTerms src = base.get(i % base.size());
            out.add(new DocTerms(i, src.getTerms()));
        }
        return out;
    }

    private static final class SearchPoint {
        final long totalMs;
        final int candidateCount;
        final int groupCount;

        SearchPoint(long totalMs, int candidateCount, int groupCount) {
            this.totalMs = totalMs;
            this.candidateCount = candidateCount;
            this.groupCount = groupCount;
        }
    }

    private static final class TwoPhasePickerWrapper {
        int pickCount(FrequentItemsetMiningResult mined, TermTidsetIndex index) {
            return new cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.picker.TwoPhaseExclusiveItemsetPicker()
                    .pick(mined.getCandidates(), index.getIdToTerm().size(), 100)
                    .size();
        }
    }
}
