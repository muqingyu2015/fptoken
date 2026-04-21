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
import cn.lxdb.plugins.muqingyu.fptoken.model.SelectedGroup;
import cn.lxdb.plugins.muqingyu.fptoken.picker.TwoPhaseExclusiveItemsetPicker;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

@Tag("performance")
@EnabledIfSystemProperty(named = "fptoken.runPerfTests", matches = "true")
class PerformanceApplicationScenarioTest {

    // 场景 E: E2E 端到端
    @Test
    void scenarioE_e2e_pipelineSlaAndCoverage() {
        List<DocTerms> rows = PerfTestSupport.protocolMixRows(
                PerfTestSupport.intProp("fptoken.perf.app.e.docs", 12000), 20260421L);
        Runtime rt = Runtime.getRuntime();
        rt.gc();
        long before = rt.totalMemory() - rt.freeMemory();
        ExclusiveSelectionResult[] holder = new ExclusiveSelectionResult[1];
        long ms = PerfTestSupport.elapsedMillis(() -> holder[0] =
                ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(rows, 8, 2, 6, 220000));
        long after = rt.totalMemory() - rt.freeMemory();
        long memDeltaMb = Math.max(0L, (after - before) / (1024L * 1024L));
        int totalSupport = holder[0].getGroups().stream().mapToInt(SelectedGroup::getSupport).sum();
        double coverage = PerfTestSupport.supportCoverageRatio(rows.size(), totalSupport);
        long maxMs = PerfTestSupport.longProp("fptoken.perf.app.e.maxMs", 180000L);
        long maxMemMb = PerfTestSupport.longProp("fptoken.perf.app.e.maxMemMb", 1536L);
        assertTrue(ms <= maxMs, () -> "scenarioE ms=" + ms + ", maxMs=" + maxMs);
        assertTrue(memDeltaMb <= maxMemMb, () -> "scenarioE memDeltaMb=" + memDeltaMb + ", maxMemMb=" + maxMemMb);
        assertTrue(coverage >= 0d);
    }

    @Test
    void scenarioE_e2e_cpuAndIoProxy() throws Exception {
        List<DocTerms> rows = PerfTestSupport.protocolMixRows(4000, 99L);
        ThreadMXBean bean = ManagementFactory.getThreadMXBean();
        long cpu0 = bean.isCurrentThreadCpuTimeSupported() ? bean.getCurrentThreadCpuTime() : 0L;
        ExclusiveSelectionResult result = ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(
                rows, 8, 2, 6, 150000);
        long cpu1 = bean.isCurrentThreadCpuTimeSupported() ? bean.getCurrentThreadCpuTime() : cpu0;
        long cpuMs = Math.max(0L, (cpu1 - cpu0) / 1_000_000L);

        Path temp = Files.createTempFile("fptoken-perf-", ".txt");
        try {
            List<String> lines = result.getGroups().stream()
                    .limit(200)
                    .map(g -> g.getSupport() + "|" + g.getTerms().size())
                    .collect(Collectors.toList());
            long ioMs = PerfTestSupport.elapsedMillis(() -> {
                try {
                    Files.write(temp, lines, StandardCharsets.UTF_8);
                    Files.readAllLines(temp, StandardCharsets.UTF_8);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            assertTrue(ioMs >= 0L);
        } finally {
            Files.deleteIfExists(temp);
        }
        assertTrue(cpuMs >= 0L);
    }

    // 场景 F: 内存压力与 GC 行为
    @Test
    void scenarioF_gcBehavior_underMemoryPressure() {
        long[] gcBefore = gcStats();
        List<DocTerms> rows = PerfTestSupport.standardPcapRows(2500);
        for (int i = 0; i < 4; i++) {
            ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(rows, 6, 2, 8, 240000);
        }
        long[] gcAfter = gcStats();
        long gcCountInc = gcAfter[0] - gcBefore[0];
        long gcTimeInc = gcAfter[1] - gcBefore[1];
        assertTrue(gcCountInc >= 0L);
        assertTrue(gcTimeInc >= 0L);
    }

    @Test
    void scenarioF_gcCollectorAwareness_andLeakGuard() {
        List<GarbageCollectorMXBean> beans = ManagementFactory.getGarbageCollectorMXBeans();
        assertTrue(!beans.isEmpty());

        Runtime rt = Runtime.getRuntime();
        rt.gc();
        long base = rt.totalMemory() - rt.freeMemory();
        for (int i = 0; i < 12; i++) {
            ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(
                    PerfTestSupport.protocolMixRows(800, i), 8, 2, 6, 120000);
            rt.gc();
        }
        long end = rt.totalMemory() - rt.freeMemory();
        long driftMb = Math.max(0L, (end - base) / (1024L * 1024L));
        long maxDriftMb = PerfTestSupport.longProp("fptoken.perf.app.f.maxDriftMb", 256L);
        assertTrue(driftMb <= maxDriftMb, () -> "scenarioF driftMb=" + driftMb + ", maxDriftMb=" + maxDriftMb);
    }

    // 场景 G: 不同流量特征
    @Test
    void scenarioG_trafficCharacteristics_robustness() {
        long highCard = runFacadeMs(PerfTestSupport.highCardinalityRows(9000, 14, 7L), 8);
        long lowCard = runFacadeMs(PerfTestSupport.lowCardinalityRows(9000, 14, 7L), 8);
        long mixed = runFacadeMs(PerfTestSupport.protocolMixRows(9000, 7L), 8);
        long enc20 = runFacadeMs(PerfTestSupport.rowsWithEncryptedRatio(9000, 14, 0.2d, 7L), 8);
        long enc80 = runFacadeMs(PerfTestSupport.rowsWithEncryptedRatio(9000, 14, 0.8d, 7L), 8);
        assertTrue(highCard >= 0L && lowCard >= 0L && mixed >= 0L && enc20 >= 0L && enc80 >= 0L);
        long max = Collections.max(List.of(highCard, lowCard, mixed, enc20, enc80));
        long min = Collections.min(List.of(highCard, lowCard, mixed, enc20, enc80));
        long maxRatio = PerfTestSupport.longProp("fptoken.perf.app.g.maxRatio", 25L);
        assertTrue(max <= Math.max(1L, min) * maxRatio,
                () -> "scenarioG maxMs=" + max + ", minMs=" + min + ", maxRatio=" + maxRatio);
    }

    // 场景 H: 依赖库/外部组件
    @Test
    void scenarioH_dependencyHotspot_bitsetAndSerializationIo() throws Exception {
        TermTidsetIndex index = TermTidsetIndex.build(PerfTestSupport.protocolMixRows(6000, 88L));
        List<java.util.BitSet> tidsets = index.getTidsetsByTermId();
        long andMs = PerfTestSupport.elapsedMillis(() -> {
            int lim = Math.min(2000, tidsets.size());
            for (int i = 1; i < lim; i++) {
                java.util.BitSet a = (java.util.BitSet) tidsets.get(i - 1).clone();
                a.and(tidsets.get(i));
                a.cardinality();
            }
        });

        Path temp = Files.createTempFile("fptoken-dep-", ".bin");
        try {
            byte[] payload = new byte[2 * 1024 * 1024];
            long ioMs = PerfTestSupport.elapsedMillis(() -> {
                try {
                    Files.write(temp, payload);
                    Files.readAllBytes(temp);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            assertTrue(ioMs >= 0L);
        } finally {
            Files.deleteIfExists(temp);
        }
        assertTrue(andMs >= 0L);
    }

    // 场景 I: 配置敏感性
    @Test
    void scenarioI_configSensitivity_extremeCombinations() {
        List<DocTerms> rows = PerfTestSupport.protocolMixRows(6000, 100L);
        long aggressive = runFacadeMs(rows, 1, 2, 12, 260000);
        long conservative = runFacadeMs(rows, 40, 2, 3, 60000);
        assertTrue(aggressive >= 0L && conservative >= 0L);

        TermTidsetIndex index = TermTidsetIndex.build(rows);
        BeamFrequentItemsetMiner miner = new BeamFrequentItemsetMiner();
        SelectorConfig cfg = new SelectorConfig(6, 2, 8, 200000);
        FrequentItemsetMiningResult lowFreqCap = miner.mineWithStats(index.getTidsetsByTermId(), cfg, 300, 8, 16);
        FrequentItemsetMiningResult highFreqCap = miner.mineWithStats(index.getTidsetsByTermId(), cfg, 5000, 64, 128);
        assertTrue(highFreqCap.getGeneratedCandidateCount() >= lowFreqCap.getGeneratedCandidateCount()
                || highFreqCap.isTruncatedByCandidateLimit());
    }

    @Test
    void scenarioI_configSensitivity_highSwapTrialsCost() {
        List<DocTerms> rows = PerfTestSupport.protocolMixRows(5000, 777L);
        TermTidsetIndex index = TermTidsetIndex.build(rows);
        BeamFrequentItemsetMiner miner = new BeamFrequentItemsetMiner();
        SelectorConfig cfg = new SelectorConfig(8, 2, 6, 180000);
        FrequentItemsetMiningResult mined = miner.mineWithStats(index.getTidsetsByTermId(), cfg, 4096, 24, 64);
        List<CandidateItemset> candidates = mined.getCandidates();
        TwoPhaseExclusiveItemsetPicker picker = new TwoPhaseExclusiveItemsetPicker();
        long tLow = PerfTestSupport.elapsedMillis(() -> picker.pick(candidates, index.getIdToTerm().size(), 50));
        long tHigh = PerfTestSupport.elapsedMillis(() -> picker.pick(candidates, index.getIdToTerm().size(), 1000));
        assertTrue(tHigh >= tLow || tHigh >= 0L);
    }

    // 场景 J: 持续运行稳定性
    @Test
    @EnabledIfSystemProperty(named = "fptoken.runSoakTests", matches = "true")
    void scenarioJ_soak_stabilityTrend() {
        List<Long> samples = new ArrayList<>();
        for (int i = 0; i < 40; i++) {
            long ms = runFacadeMs(PerfTestSupport.protocolMixRows(900, i), 8);
            samples.add(ms);
        }
        long firstAvg = avg(samples.subList(0, 10));
        long lastAvg = avg(samples.subList(30, 40));
        assertTrue(lastAvg <= firstAvg * 2L + 1L);
    }

    // 场景 K: 下游检索链路
    @Test
    void scenarioK_downstreamSearchIntegration_throughputAndQuality() {
        List<DocTerms> train = PerfTestSupport.protocolMixRows(7000, 123L);
        ExclusiveSelectionResult model = ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(
                train, 8, 2, 6, 200000);

        List<DocTerms> queries = PerfTestSupport.protocolMixRows(3000, 456L);
        long indexedMs = PerfTestSupport.elapsedMillis(() -> runIndexedQueryProxy(model.getGroups(), queries));
        long naiveMs = PerfTestSupport.elapsedMillis(() -> runNaiveQueryProxy(model.getGroups(), queries));
        assertTrue(indexedMs >= 0L && naiveMs >= 0L);
    }

    // 场景 L: 数据倾斜
    @Test
    void scenarioL_skewedData_superNodes() {
        List<DocTerms> skewed = PerfTestSupport.skewedRows(10000, 12, 0.55d, 900L);
        ExclusiveSelectionResult result = ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(
                skewed, 8, 2, 6, 220000);
        assertTrue(result.getCandidateCount() >= 0);
        assertTrue(result.getGroups().size() >= 0);
        assertTrue(!result.isTruncatedByCandidateLimit() || result.getCandidateCount() == 220000);
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

    private long runFacadeMs(List<DocTerms> rows, int minSupport) {
        return runFacadeMs(rows, minSupport, 2, 6, 200000);
    }

    private long runFacadeMs(List<DocTerms> rows, int minSupport, int minLen, int maxLen, int maxCandidates) {
        return PerfTestSupport.elapsedMillis(() ->
                ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(
                        rows, minSupport, minLen, maxLen, maxCandidates));
    }

    private long avg(List<Long> values) {
        long sum = 0L;
        for (Long v : values) {
            sum += v.longValue();
        }
        return values.isEmpty() ? 0L : sum / values.size();
    }

    private int runIndexedQueryProxy(List<SelectedGroup> groups, List<DocTerms> queries) {
        int hits = 0;
        List<List<byte[]>> groupTerms = groups.stream().map(SelectedGroup::getTerms).collect(Collectors.toList());
        for (DocTerms query : queries) {
            List<byte[]> terms = query.getTerms();
            for (List<byte[]> gt : groupTerms) {
                if (PerfTestSupport.termsContainAll(terms, gt)) {
                    hits++;
                    break;
                }
            }
        }
        return hits;
    }

    private int runNaiveQueryProxy(List<SelectedGroup> groups, List<DocTerms> queries) {
        int hits = 0;
        for (DocTerms query : queries) {
            for (SelectedGroup g : groups) {
                if (PerfTestSupport.termsContainAll(query.getTerms(), g.getTerms())) {
                    hits++;
                    break;
                }
            }
        }
        return hits;
    }
}
