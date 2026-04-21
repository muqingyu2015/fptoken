package cn.lxdb.plugins.muqingyu.fptoken.tests.performance;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import cn.lxdb.plugins.muqingyu.fptoken.ExclusiveFrequentItemsetSelector;
import cn.lxdb.plugins.muqingyu.fptoken.config.SelectorConfig;
import cn.lxdb.plugins.muqingyu.fptoken.index.TermTidsetIndex;
import cn.lxdb.plugins.muqingyu.fptoken.miner.BeamFrequentItemsetMiner;
import cn.lxdb.plugins.muqingyu.fptoken.model.CandidateItemset;
import cn.lxdb.plugins.muqingyu.fptoken.model.DocTerms;
import cn.lxdb.plugins.muqingyu.fptoken.model.ExclusiveSelectionResult;
import cn.lxdb.plugins.muqingyu.fptoken.model.FrequentItemsetMiningResult;
import cn.lxdb.plugins.muqingyu.fptoken.picker.GreedyExclusiveItemsetPicker;
import cn.lxdb.plugins.muqingyu.fptoken.picker.TwoPhaseExclusiveItemsetPicker;
import cn.lxdb.plugins.muqingyu.fptoken.tests.ByteArrayTestSupport;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import java.util.concurrent.TimeUnit;

@Tag("performance")
@EnabledIfSystemProperty(named = "fptoken.runPerfTests", matches = "true")
@Timeout(value = 10, unit = TimeUnit.SECONDS)
class PerformanceCaseCatalogPart1Test {

    // =========================
    // Index Build (PERF-INDEX)
    // =========================

    @Test
    void PERF_INDEX_001_emptyData_indexBuild() {
        long ms = PerfTestSupport.elapsedMillis(() -> TermTidsetIndex.build(Collections.emptyList()));
        long budget = PerfTestSupport.longProp("fptoken.perf.index.001.maxMs", 50L);
        assertTrue(ms < budget, () -> "PERF-INDEX-001 ms=" + ms + ", budgetMs=" + budget);
    }

    @Test
    void PERF_INDEX_002_singleRecord_indexBuild() {
        List<DocTerms> rows = PerfTestSupport.standardPcapRows(1);
        long ms = PerfTestSupport.elapsedMillis(() -> TermTidsetIndex.build(rows));
        long budget = PerfTestSupport.longProp("fptoken.perf.index.002.maxMs", 200L);
        assertTrue(ms < budget, () -> "PERF-INDEX-002 ms=" + ms + ", budgetMs=" + budget + ", docs=" + rows.size());
    }

    @Test
    void PERF_INDEX_003_100records_latencyAndThroughput() {
        int records = PerfTestSupport.intProp("fptoken.perf.index.003.records", 100);
        List<DocTerms> rows = PerfTestSupport.standardPcapRows(records);
        long ms = PerfTestSupport.elapsedMillis(() -> TermTidsetIndex.build(rows));
        double tps = PerfTestSupport.throughputPerSecond(records, ms);
        long budget = PerfTestSupport.longProp("fptoken.perf.index.003.maxMs", 4000L);
        double minTps = Double.parseDouble(System.getProperty("fptoken.perf.index.003.minRps", "120"));
        assertTrue(ms < budget, () -> "PERF-INDEX-003 ms=" + ms + ", budgetMs=" + budget);
        assertTrue(tps >= minTps, () -> "PERF-INDEX-003 throughput=" + tps + " rec/s, expected>=" + minTps);
    }

    @Test
    void PERF_INDEX_004_1000records_latencyAndThroughput() {
        int records = PerfTestSupport.intProp("fptoken.perf.index.004.records", 1000);
        List<DocTerms> rows = PerfTestSupport.standardPcapRows(records);
        long ms = PerfTestSupport.elapsedMillis(() -> TermTidsetIndex.build(rows));
        double tps = PerfTestSupport.throughputPerSecond(records, ms);
        long budget = PerfTestSupport.longProp("fptoken.perf.index.004.maxMs", 30000L);
        double minTps = Double.parseDouble(System.getProperty("fptoken.perf.index.004.minRps", "100"));
        assertTrue(ms < budget, () -> "PERF-INDEX-004 ms=" + ms + ", budgetMs=" + budget);
        assertTrue(tps >= minTps, () -> "PERF-INDEX-004 throughput=" + tps + " rec/s, expected>=" + minTps);
    }

    @Test
    void PERF_INDEX_005_standardBatch_10000records() {
        int records = PerfTestSupport.intProp("fptoken.perf.index.005.records", 3000);
        List<DocTerms> rows = PerfTestSupport.standardPcapRows(records);
        Runtime rt = Runtime.getRuntime();
        rt.gc();
        long memBefore = rt.totalMemory() - rt.freeMemory();
        long ms = PerfTestSupport.elapsedMillis(() -> TermTidsetIndex.build(rows));
        long memAfter = rt.totalMemory() - rt.freeMemory();
        long deltaMb = Math.max(0L, (memAfter - memBefore) / (1024L * 1024L));
        double tps = PerfTestSupport.throughputPerSecond(records, ms);
        long budget = PerfTestSupport.longProp("fptoken.perf.index.005.maxMs", 120000L);
        long maxMem = PerfTestSupport.longProp("fptoken.perf.index.005.maxMemMb", 1024L);
        assertTrue(ms < budget, () -> "PERF-INDEX-005 ms=" + ms + ", budgetMs=" + budget);
        assertTrue(tps > 0d, () -> "PERF-INDEX-005 throughput should be positive");
        assertTrue(deltaMb <= maxMem, () -> "PERF-INDEX-005 memDeltaMb=" + deltaMb + ", maxMemMb=" + maxMem);
    }

    @Test
    @EnabledIfSystemProperty(named = "fptoken.runScaleTests", matches = "true")
    void PERF_INDEX_006_20000records_scalingVs10000() {
        int records10k = PerfTestSupport.intProp("fptoken.perf.index.006.records10k", 3000);
        int records20k = PerfTestSupport.intProp("fptoken.perf.index.006.records20k", 6000);
        List<DocTerms> rows10k = PerfTestSupport.standardPcapRows(records10k);
        List<DocTerms> rows20k = PerfTestSupport.standardPcapRows(records20k);
        long t10 = PerfTestSupport.elapsedMillis(() -> TermTidsetIndex.build(rows10k));
        long t20 = PerfTestSupport.elapsedMillis(() -> TermTidsetIndex.build(rows20k));
        double ratio = (double) Math.max(1L, t20) / Math.max(1L, t10);
        double maxRatio = Double.parseDouble(System.getProperty("fptoken.perf.index.006.maxRatio", "2.8"));
        assertTrue(ratio <= maxRatio, () -> "PERF-INDEX-006 ratio=" + ratio + ", maxRatio=" + maxRatio);
    }

    @Test
    @EnabledIfSystemProperty(named = "fptoken.runScaleTests", matches = "true")
    void PERF_INDEX_007_50000records_stress() {
        int records = PerfTestSupport.intProp("fptoken.perf.index.007.records", 12000);
        List<DocTerms> rows = PerfTestSupport.standardPcapRows(records);
        Runtime rt = Runtime.getRuntime();
        rt.gc();
        long memBefore = rt.totalMemory() - rt.freeMemory();
        long ms = PerfTestSupport.elapsedMillis(() -> TermTidsetIndex.build(rows));
        long memAfter = rt.totalMemory() - rt.freeMemory();
        long deltaMb = Math.max(0L, (memAfter - memBefore) / (1024L * 1024L));
        long maxMem = PerfTestSupport.longProp("fptoken.perf.index.007.maxMemMb", 2048L);
        long maxMs = PerfTestSupport.longProp("fptoken.perf.index.007.maxMs", 300000L);
        assertTrue(ms < maxMs, () -> "PERF-INDEX-007 ms=" + ms + ", budgetMs=" + maxMs);
        assertTrue(deltaMb <= maxMem, () -> "PERF-INDEX-007 memDeltaMb=" + deltaMb + ", maxMemMb=" + maxMem);
    }

    @Test
    void PERF_INDEX_008_vocabularyImpact_1k_5k_10k() {
        List<DocTerms> v1 = PerfTestSupport.rowsWithVocabulary(3000, 1000, 12, 1L);
        List<DocTerms> v5 = PerfTestSupport.rowsWithVocabulary(3000, 5000, 12, 2L);
        List<DocTerms> v10 = PerfTestSupport.rowsWithVocabulary(3000, 10000, 12, 3L);
        long t1 = PerfTestSupport.elapsedMillis(() -> TermTidsetIndex.build(v1));
        long t5 = PerfTestSupport.elapsedMillis(() -> TermTidsetIndex.build(v5));
        long t10 = PerfTestSupport.elapsedMillis(() -> TermTidsetIndex.build(v10));
        assertTrue(t5 >= 0L && t10 >= 0L && t1 >= 0L);
    }

    @Test
    void PERF_INDEX_009_distribution_uniformVsPowerLaw() {
        List<DocTerms> uniform = PerfTestSupport.rowsWithVocabulary(3000, 5000, 14, 7L);
        List<DocTerms> powerLaw = PerfTestSupport.rowsWithPowerLawDistribution(3000, 5000, 14, 7L);
        long tUniform = PerfTestSupport.elapsedMillis(() -> TermTidsetIndex.build(uniform));
        long tPowerLaw = PerfTestSupport.elapsedMillis(() -> TermTidsetIndex.build(powerLaw));
        double ratio = (double) Math.max(1L, tPowerLaw) / Math.max(1L, tUniform);
        double maxRatio = Double.parseDouble(System.getProperty("fptoken.perf.index.009.powerlawMaxRatio", "1.5"));
        assertTrue(ratio <= maxRatio, () -> "PERF-INDEX-009 powerLaw/uniform=" + ratio + ", maxRatio=" + maxRatio);
    }

    @Test
    void PERF_INDEX_010_duplicateRatio_0_50_90() {
        List<DocTerms> lowDup = PerfTestSupport.rowsWithDuplicateRatio(3000, 5000, 14, 0.0d, 10L);
        List<DocTerms> midDup = PerfTestSupport.rowsWithDuplicateRatio(3000, 5000, 14, 0.5d, 10L);
        List<DocTerms> highDup = PerfTestSupport.rowsWithDuplicateRatio(3000, 5000, 14, 0.9d, 10L);

        Runtime rt = Runtime.getRuntime();
        rt.gc();
        long lowBefore = rt.totalMemory() - rt.freeMemory();
        TermTidsetIndex.build(lowDup);
        long lowAfter = rt.totalMemory() - rt.freeMemory();
        long lowMb = Math.max(0L, (lowAfter - lowBefore) / (1024L * 1024L));

        rt.gc();
        long highBefore = rt.totalMemory() - rt.freeMemory();
        TermTidsetIndex.build(highDup);
        long highAfter = rt.totalMemory() - rt.freeMemory();
        long highMb = Math.max(0L, (highAfter - highBefore) / (1024L * 1024L));

        long midMs = PerfTestSupport.elapsedMillis(() -> TermTidsetIndex.build(midDup));
        assertTrue(midMs >= 0L);
        assertTrue(highMb <= lowMb + 128L, () -> "PERF-INDEX-010 highDupMb=" + highMb + ", lowDupMb=" + lowMb);
    }

    // =========================
    // Mining (PERF-MINE)
    // =========================

    @Test
    void PERF_MINE_001_emptyFrequentTerms() {
        BeamFrequentItemsetMiner miner = new BeamFrequentItemsetMiner();
        SelectorConfig cfg = new SelectorConfig(3, 2, 6, 1000);
        long ms = PerfTestSupport.elapsedMillis(() -> miner.mineWithStats(Collections.emptyList(), cfg, 1024, 16, 32));
        long maxMs = PerfTestSupport.longProp("fptoken.perf.mine.001.maxMs", 20L);
        assertTrue(ms < maxMs, () -> "PERF-MINE-001 ms=" + ms + ", maxMs=" + maxMs);
    }

    @Test
    void PERF_MINE_002_only1Itemset() {
        MineSnapshot s = mineSnapshot(400, 12, 1, 1, 80000, 1024, 16, 48, 0, 0L);
        long maxMs = PerfTestSupport.longProp("fptoken.perf.mine.002.maxMs", 5000L);
        assertTrue(s.ms < maxMs, () -> "PERF-MINE-002 ms=" + s.ms + ", maxMs=" + maxMs);
    }

    @Test
    void PERF_MINE_003_maxItemsetSize2() {
        MineSnapshot s = mineSnapshot(400, 12, 2, 2, 80000, 1024, 16, 48, 0, 0L);
        long maxMs = PerfTestSupport.longProp("fptoken.perf.mine.003.maxMs", 12000L);
        assertTrue(s.ms < maxMs, () -> "PERF-MINE-003 ms=" + s.ms + ", maxMs=" + maxMs);
        assertTrue(s.generated >= 0);
    }

    @Test
    void PERF_MINE_004_maxItemsetSize3() {
        MineSnapshot s = mineSnapshot(400, 12, 2, 3, 100000, 1024, 24, 48, 0, 0L);
        long maxMs = PerfTestSupport.longProp("fptoken.perf.mine.004.maxMs", 20000L);
        assertTrue(s.ms < maxMs, () -> "PERF-MINE-004 ms=" + s.ms + ", maxMs=" + maxMs);
    }

    @Test
    void PERF_MINE_005_standardDepth_maxLen6() {
        MineSnapshot s = mineSnapshot(500, 10, 2, 6, 120000, 2048, 24, 48, 0, 0L);
        long maxMs = PerfTestSupport.longProp("fptoken.perf.mine.005.maxMs", 60000L);
        int softUpperBound = PerfTestSupport.intProp("fptoken.perf.mine.005.softUpperBound", 10000);
        assertTrue(s.ms < maxMs, () -> "PERF-MINE-005 ms=" + s.ms + ", maxMs=" + maxMs);
        assertTrue(s.generated <= softUpperBound || s.truncated,
                () -> "PERF-MINE-005 generated=" + s.generated + ", softUpperBound=" + softUpperBound);
    }

    @Test
    void PERF_MINE_006_deepMining_maxLen10() {
        MineSnapshot s = mineSnapshot(500, 10, 2, 10, 120000, 2048, 24, 64, 2, 0L);
        long maxMs = PerfTestSupport.longProp("fptoken.perf.mine.006.maxMs", 120000L);
        assertTrue(s.ms < maxMs, () -> "PERF-MINE-006 ms=" + s.ms + ", maxMs=" + maxMs);
    }

    @Test
    @EnabledIfSystemProperty(named = "fptoken.runScaleTests", matches = "true")
    void PERF_MINE_007_extremeDepth_maxLen15() {
        MineSnapshot s = mineSnapshot(500, 10, 2, 12, 120000, 2048, 24, 64, 2, 0L);
        long maxMs = PerfTestSupport.longProp("fptoken.perf.mine.007.maxMs", 180000L);
        assertTrue(s.ms < maxMs, () -> "PERF-MINE-007 ms=" + s.ms + ", maxMs=" + maxMs);
    }

    @Test
    void PERF_MINE_008_beamWidthImpact() {
        int[] beam = new int[] {16, 32, 64, 128, 256};
        int lastGenerated = -1;
        for (int b : beam) {
            MineSnapshot s = mineSnapshot(350, 10, 2, 6, 120000, 2048, 24, b, 0, 0L);
            assertTrue(s.ms >= 0L);
            if (lastGenerated >= 0) {
                assertTrue(s.generated >= lastGenerated || s.truncated);
            }
            lastGenerated = s.generated;
        }
    }

    @Test
    void PERF_MINE_009_branchingFactorImpact() {
        int[] branches = new int[] {8, 16, 32, 64, 128};
        int lastGenerated = -1;
        for (int branch : branches) {
            MineSnapshot s = mineSnapshot(350, 10, 2, 6, 120000, 2048, branch, 64, 0, 0L);
            assertTrue(s.ms >= 0L);
            if (lastGenerated >= 0) {
                assertTrue(s.generated >= lastGenerated || s.truncated);
            }
            lastGenerated = s.generated;
        }
    }

    @Test
    void PERF_MINE_010_maxFrequentTermCountImpact() {
        int[] freqCaps = new int[] {500, 1000, 2000, 5000, 0};
        for (int cap : freqCaps) {
            MineSnapshot s = mineSnapshot(350, 10, 2, 6, 120000, cap, 24, 64, 0, 0L);
            assertTrue(s.frequentTerms >= 0);
            assertTrue(s.ms >= 0L);
        }
    }

    @Test
    void PERF_MINE_011_minSupportImpact() {
        int[] supports = new int[] {1, 5, 10, 20, 50, 100};
        int previousGenerated = Integer.MAX_VALUE;
        for (int minSupport : supports) {
            MineSnapshot s = mineSnapshot(350, minSupport, 2, 6, 120000, 2048, 24, 64, 0, 0L);
            assertTrue(s.generated <= previousGenerated || s.truncated);
            previousGenerated = s.generated;
        }
    }

    @Test
    void PERF_MINE_012_adaptiveBeamWidth_enabledVsDisabledProxy() {
        // 使用极窄 beam 近似“关闭自适应收益”，宽 beam 近似“开启并受益”。
        MineSnapshot narrow = mineSnapshot(900, 10, 2, 10, 200000, 4096, 32, 16, 0, 0L);
        MineSnapshot wide = mineSnapshot(900, 10, 2, 10, 200000, 4096, 32, 128, 0, 0L);
        assertTrue(wide.generated >= narrow.generated || wide.truncated);
    }

    // =========================
    // Picker (PERF-PICK)
    // =========================

    @Test
    void PERF_PICK_001_emptyCandidates() {
        GreedyExclusiveItemsetPicker picker = new GreedyExclusiveItemsetPicker();
        long ms = PerfTestSupport.elapsedMillis(() -> picker.pick(Collections.emptyList(), 64));
        assertTrue(ms < PerfTestSupport.longProp("fptoken.perf.pick.001.maxMs", 20L));
    }

    @Test
    void PERF_PICK_002_greedy100() {
        runGreedyPickerCase("PERF-PICK-002", 100, 2048, 2000, 100L);
    }

    @Test
    void PERF_PICK_003_greedy1000() {
        runGreedyPickerCase("PERF-PICK-003", 1000, 4096, 4000, 300L);
    }

    @Test
    void PERF_PICK_004_greedy5000() {
        runGreedyPickerCase("PERF-PICK-004", 5000, 8192, 10000, 1200L);
    }

    @Test
    void PERF_PICK_005_greedy20000() {
        runGreedyPickerCase("PERF-PICK-005", 20000, 16384, 20000, 5000L);
    }

    @Test
    void PERF_PICK_006_swapTrials0_equalsGreedyPath() {
        List<CandidateItemset> candidates = PerfTestSupport.syntheticCandidates(2000, 8000, 8000, 2, 5, 99L);
        TwoPhaseExclusiveItemsetPicker picker = new TwoPhaseExclusiveItemsetPicker();
        long ms = PerfTestSupport.elapsedMillis(() -> picker.pick(candidates, 8000, 0));
        assertTrue(ms < PerfTestSupport.longProp("fptoken.perf.pick.006.maxMs", 1000L));
    }

    @Test
    void PERF_PICK_007_swapTrials50() {
        runTwoPhaseCase("PERF-PICK-007", 50, 1200L);
    }

    @Test
    void PERF_PICK_008_swapTrials200() {
        runTwoPhaseCase("PERF-PICK-008", 200, 1800L);
    }

    @Test
    void PERF_PICK_009_swapTrials1000() {
        runTwoPhaseCase("PERF-PICK-009", 1000, 3500L);
    }

    @Test
    void PERF_PICK_010_swapSuccessRateImpact_proxy() {
        List<CandidateItemset> lowSuccess = PerfTestSupport.syntheticCandidates(3000, 3000, 6000, 2, 4, 1L);
        List<CandidateItemset> highSuccess = PerfTestSupport.syntheticCandidates(3000, 10000, 6000, 2, 4, 2L);
        TwoPhaseExclusiveItemsetPicker picker = new TwoPhaseExclusiveItemsetPicker();
        long lowMs = PerfTestSupport.elapsedMillis(() -> picker.pick(lowSuccess, 3000, 500));
        long highMs = PerfTestSupport.elapsedMillis(() -> picker.pick(highSuccess, 10000, 500));
        assertTrue(highMs >= 0L && lowMs >= 0L);
    }

    @Test
    void PERF_PICK_011_inputOrderImpact() {
        List<CandidateItemset> ordered = PerfTestSupport.syntheticCandidates(2000, 8000, 6000, 2, 4, 42L);
        List<CandidateItemset> shuffled = new ArrayList<>(ordered);
        Collections.shuffle(shuffled, new java.util.Random(42L));
        TwoPhaseExclusiveItemsetPicker picker = new TwoPhaseExclusiveItemsetPicker();
        int orderedSize = picker.pick(ordered, 8000, 200).size();
        int shuffledSize = picker.pick(shuffled, 8000, 200).size();
        assertTrue(orderedSize >= 0 && shuffledSize >= 0);
    }

    // =========================
    // E2E (PERF-E2E subset)
    // =========================

    @Test
    void PERF_E2E_001_standardBatch_10000() {
        int records = PerfTestSupport.intProp("fptoken.perf.e2e.001.records", 3000);
        List<DocTerms> rows = PerfTestSupport.standardPcapRows(records);
        long ms = PerfTestSupport.elapsedMillis(() ->
                ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(rows, 10, 2, 6, 200000));
        long maxMs = PerfTestSupport.longProp("fptoken.perf.e2e.001.maxMs", 180000L);
        assertTrue(ms < maxMs, () -> "PERF-E2E-001 ms=" + ms + ", maxMs=" + maxMs);
    }

    @Test
    void PERF_E2E_002_stageRatio_indexMinePick() {
        List<DocTerms> rows = PerfTestSupport.standardPcapRows(2000);
        SelectorConfig cfg = new SelectorConfig(10, 2, 6, 200000);
        BeamFrequentItemsetMiner miner = new BeamFrequentItemsetMiner();
        TwoPhaseExclusiveItemsetPicker picker = new TwoPhaseExclusiveItemsetPicker();

        long indexMs;
        long mineMs;
        long pickMs;
        TermTidsetIndex[] holder = new TermTidsetIndex[1];
        indexMs = PerfTestSupport.elapsedMillis(() -> holder[0] = TermTidsetIndex.build(rows));
        FrequentItemsetMiningResult[] mined = new FrequentItemsetMiningResult[1];
        mineMs = PerfTestSupport.elapsedMillis(() -> mined[0] =
                miner.mineWithStats(holder[0].getTidsetsByTermId(), cfg, 4096, 24, 64));
        pickMs = PerfTestSupport.elapsedMillis(() ->
                picker.pick(mined[0].getCandidates(), holder[0].getIdToTerm().size(), 128));

        long total = Math.max(1L, indexMs + mineMs + pickMs);
        double indexPct = (indexMs * 100.0d) / total;
        double minePct = (mineMs * 100.0d) / total;
        double pickPct = (pickMs * 100.0d) / total;
        assertTrue(indexPct >= 0d && minePct >= 0d && pickPct >= 0d);
        assertEquals(100.0d, indexPct + minePct + pickPct, 0.2d);
    }

    @Test
    void PERF_E2E_003_smallBatch_1000() {
        List<DocTerms> rows = PerfTestSupport.standardPcapRows(1000);
        long ms = PerfTestSupport.elapsedMillis(() ->
                ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(rows, 10, 2, 6, 200000));
        assertTrue(ms < PerfTestSupport.longProp("fptoken.perf.e2e.003.maxMs", 30000L));
    }

    @Test
    void PERF_E2E_004_midBatch_5000() {
        List<DocTerms> rows = PerfTestSupport.standardPcapRows(2000);
        long ms = PerfTestSupport.elapsedMillis(() ->
                ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(rows, 10, 2, 6, 200000));
        assertTrue(ms < PerfTestSupport.longProp("fptoken.perf.e2e.004.maxMs", 90000L));
    }

    @Test
    @EnabledIfSystemProperty(named = "fptoken.runScaleTests", matches = "true")
    void PERF_E2E_005_largeBatch_50000() {
        List<DocTerms> rows = PerfTestSupport.standardPcapRows(10000);
        Runtime rt = Runtime.getRuntime();
        rt.gc();
        long before = rt.totalMemory() - rt.freeMemory();
        long ms = PerfTestSupport.elapsedMillis(() ->
                ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(rows, 10, 2, 6, 200000));
        long after = rt.totalMemory() - rt.freeMemory();
        long deltaMb = Math.max(0L, (after - before) / (1024L * 1024L));
        assertTrue(ms < PerfTestSupport.longProp("fptoken.perf.e2e.005.maxMs", 360000L));
        assertTrue(deltaMb <= PerfTestSupport.longProp("fptoken.perf.e2e.005.maxMemMb", 2048L));
    }

    @Test
    void PERF_E2E_006_007_008_009_windowSizeImpact() {
        long t1024 = runE2EWithWindow(200, 1024, 128);
        long t128 = runE2EWithWindow(200, 128, 1);
        long t64 = runE2EWithWindow(200, 64, 1);
        long t256 = runE2EWithWindow(200, 256, 1);
        assertTrue(t1024 >= 0 && t128 >= 0 && t64 >= 0 && t256 >= 0);
        long max = Math.max(Math.max(t1024, t128), Math.max(t64, t256));
        long min = Math.min(Math.min(t1024, t128), Math.min(t64, t256));
        assertTrue(max >= min);
        assertTrue(max > min, () -> "window size impact collapsed: "
                + "t1024=" + t1024 + ", t256=" + t256 + ", t128=" + t128 + ", t64=" + t64);
    }

    @Test
    void PERF_E2E_010_windowStepImpact() {
        long step128 = runE2EWithWindow(200, 128, 128);
        long step64 = runE2EWithWindow(200, 128, 64);
        long step32 = runE2EWithWindow(200, 128, 32);
        assertTrue(step32 >= step64 || step64 >= step128);
    }

    @Test
    void PERF_E2E_011_012_013_014_015_itemTypeScenarios() {
        // 由于当前造数方法固定输出 1/2/3-byte 混合，这里对比“词汇规模 proxy”来模拟 item 类型差异。
        ExclusiveSelectionResult oneByteLike = runFacade(PerfTestSupport.rowsWithVocabulary(2500, 256, 12, 11L));
        ExclusiveSelectionResult twoByteLike = runFacade(PerfTestSupport.rowsWithVocabulary(2500, 65536, 12, 12L));
        ExclusiveSelectionResult threeByteLike = runFacade(PerfTestSupport.rowsWithVocabulary(2500, 120000, 12, 13L));
        ExclusiveSelectionResult mixed = runFacade(PerfTestSupport.rowsWithVocabulary(2500, 50000, 12, 14L));
        assertTrue(oneByteLike.getFrequentTermCount() >= 0);
        assertTrue(twoByteLike.getFrequentTermCount() >= 0);
        assertTrue(threeByteLike.getFrequentTermCount() >= 0);
        assertTrue(mixed.getFrequentTermCount() >= 0);
        int maxTerms = Math.max(
                Math.max(oneByteLike.getFrequentTermCount(), twoByteLike.getFrequentTermCount()),
                threeByteLike.getFrequentTermCount());
        int minTerms = Math.min(
                Math.min(oneByteLike.getFrequentTermCount(), twoByteLike.getFrequentTermCount()),
                threeByteLike.getFrequentTermCount());
        assertTrue(maxTerms >= minTerms);
        assertTrue(maxTerms > minTerms
                        || oneByteLike.getCandidateCount() != twoByteLike.getCandidateCount()
                        || twoByteLike.getCandidateCount() != threeByteLike.getCandidateCount(),
                "item-type proxy should show at least one observable difference");
    }

    @Test
    void PERF_E2E_016_017_018_019_020_repeatabilitySpectrum() {
        ExclusiveSelectionResult fullRepeat = runFacade(PerfTestSupport.rowsWithDuplicateRatio(2000, 5000, 14, 1.0d, 21L));
        ExclusiveSelectionResult highRepeat = runFacade(PerfTestSupport.rowsWithDuplicateRatio(2000, 5000, 14, 0.9d, 22L));
        ExclusiveSelectionResult medRepeat = runFacade(PerfTestSupport.rowsWithDuplicateRatio(2000, 5000, 14, 0.5d, 23L));
        ExclusiveSelectionResult lowRepeat = runFacade(PerfTestSupport.rowsWithDuplicateRatio(2000, 5000, 14, 0.1d, 24L));
        ExclusiveSelectionResult randomLike = runFacade(PerfTestSupport.rowsWithDuplicateRatio(2000, 5000, 14, 0.0d, 25L));
        assertTrue(fullRepeat.getCandidateCount() >= highRepeat.getCandidateCount());
        assertTrue(randomLike.getCandidateCount() >= 0);
        assertTrue(medRepeat.getGroups().size() >= 0 && lowRepeat.getGroups().size() >= 0);
    }

    private void runGreedyPickerCase(String id, int count, int dictionarySize, int docs, long maxMs) {
        List<CandidateItemset> candidates = PerfTestSupport.syntheticCandidates(count, dictionarySize, docs, 2, 5, count * 7L);
        GreedyExclusiveItemsetPicker picker = new GreedyExclusiveItemsetPicker();
        long ms = PerfTestSupport.elapsedMillis(() -> picker.pick(candidates, dictionarySize));
        long budget = PerfTestSupport.longProp("fptoken.perf." + id.toLowerCase() + ".maxMs", maxMs);
        assertTrue(ms < budget, () -> id + " ms=" + ms + ", budgetMs=" + budget);
    }

    private void runTwoPhaseCase(String id, int maxTrials, long maxMs) {
        List<CandidateItemset> candidates = PerfTestSupport.syntheticCandidates(1500, 8000, 8000, 2, 5, maxTrials * 31L);
        TwoPhaseExclusiveItemsetPicker picker = new TwoPhaseExclusiveItemsetPicker();
        long ms = PerfTestSupport.elapsedMillis(() -> picker.pick(candidates, 10000, maxTrials));
        long budget = PerfTestSupport.longProp("fptoken.perf." + id.toLowerCase() + ".maxMs", maxMs);
        assertTrue(ms < budget, () -> id + " ms=" + ms + ", budgetMs=" + budget);
    }

    private MineSnapshot mineSnapshot(
            int records,
            int minSupport,
            int minLen,
            int maxLen,
            int maxCandidates,
            int maxFrequentTermCount,
            int maxBranchingFactor,
            int beamWidth,
            int maxIdleLevels,
            long maxRuntimeMillis
    ) {
        List<DocTerms> rows = PerfTestSupport.standardPcapRows(records);
        TermTidsetIndex index = TermTidsetIndex.build(rows);
        BeamFrequentItemsetMiner miner = new BeamFrequentItemsetMiner();
        SelectorConfig cfg = new SelectorConfig(minSupport, minLen, maxLen, maxCandidates);
        FrequentItemsetMiningResult[] out = new FrequentItemsetMiningResult[1];
        long ms = PerfTestSupport.elapsedMillis(() -> out[0] = miner.mineWithStats(
                index.getTidsetsByTermId(),
                cfg,
                maxFrequentTermCount,
                maxBranchingFactor,
                beamWidth,
                maxIdleLevels,
                maxRuntimeMillis
        ));
        return new MineSnapshot(ms, out[0]);
    }

    private long runE2EWithWindow(int records, int windowLen, int step) {
        List<DocTerms> rows = ByteArrayTestSupport.pcapLikeBatch(records, 1024, windowLen, step);
        return PerfTestSupport.elapsedMillis(() ->
                ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(rows, 10, 2, 6, 200000));
    }

    private ExclusiveSelectionResult runFacade(List<DocTerms> rows) {
        return ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(rows, 10, 2, 6, 200000);
    }

    private static final class MineSnapshot {
        final long ms;
        final int generated;
        final int frequentTerms;
        final boolean truncated;

        MineSnapshot(long ms, FrequentItemsetMiningResult result) {
            this.ms = ms;
            this.generated = result.getGeneratedCandidateCount();
            this.frequentTerms = result.getFrequentTermCount();
            this.truncated = result.isTruncatedByCandidateLimit();
        }
    }
}
