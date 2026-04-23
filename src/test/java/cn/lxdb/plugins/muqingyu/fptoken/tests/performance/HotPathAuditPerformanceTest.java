package cn.lxdb.plugins.muqingyu.fptoken.tests.performance;

import static org.junit.jupiter.api.Assertions.assertTrue;

import cn.lxdb.plugins.muqingyu.fptoken.api.ExclusiveFpRowsProcessingApi;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.config.EngineTuningConfig;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.config.SelectorConfig;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.index.TermTidsetIndex;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.miner.BeamFrequentItemsetMiner;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.CandidateItemset;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.DocTerms;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.ExclusiveSelectionResult;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.FrequentItemsetMiningResult;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.SelectedGroup;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.picker.TwoPhaseExclusiveItemsetPicker;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.util.ByteArrayUtils;
import cn.lxdb.plugins.muqingyu.fptoken.runner.result.LineFileProcessingResult;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

/**
 * 高热点路径审计：通过阶段计时识别高频调用类的成本占比。
 */
@Tag("performance")
@EnabledIfSystemProperty(named = "fptoken.runPerfTests", matches = "true")
@Timeout(value = 20, unit = TimeUnit.SECONDS)
class HotPathAuditPerformanceTest {

    private static final int STAGE_INDEX = 0;
    private static final int STAGE_MINE = 1;
    private static final int STAGE_PICK = 2;
    private static final int STAGE_DERIVED = 3;
    private static final int STAGE_FINAL_INDEX = 4;
    private static final int STAGE_TOTAL = 5;

    private static final String[] STAGE_NAMES = {
            "TermTidsetIndex.buildWithSupportBounds",
            "BeamFrequentItemsetMiner.mineWithStats",
            "TwoPhaseExclusiveItemsetPicker.pick",
            "ExclusiveFpRowsProcessingApi.buildDerivedData",
            "LineFileProcessingResult(FinalIndexData build)",
            "Total"
    };

    private static final int INDEX_SUB_FIRST_PASS = 0;
    private static final int INDEX_SUB_FILTER_PREPARE = 1;
    private static final int INDEX_SUB_SECOND_PASS = 2;
    private static final int INDEX_SUB_TOTAL = 3;
    private static final String[] INDEX_SUB_STAGE_NAMES = {
            "Index.firstPass(freq scan)",
            "Index.filterPrepare(map+allocate)",
            "Index.secondPass(set bitset)",
            "Index.total"
    };

    @Test
    void hotPathAudit_shouldPrintStageCostRanking() {
        List<Scenario> scenarios = new ArrayList<Scenario>();
        scenarios.add(new Scenario("protocol-mix", PerfTestSupport.protocolMixRows(12000, 20260422L), 8));
        scenarios.add(new Scenario("vocab-heavy", PerfTestSupport.rowsWithVocabulary(12000, 2500, 18, 20260422L), 3));
        scenarios.add(new Scenario("power-law", PerfTestSupport.rowsWithPowerLawDistribution(12000, 2500, 18, 20260422L), 3));

        long[] aggregate = new long[STAGE_NAMES.length];
        long[] aggregateIndexSub = new long[INDEX_SUB_STAGE_NAMES.length];
        int measuredRuns = 0;
        for (Scenario scenario : scenarios) {
            // warmup
            runOnce(scenario.rows, scenario.minSupport);

            long[] scenarioTotals = new long[STAGE_NAMES.length];
            long[] scenarioIndexSubTotals = new long[INDEX_SUB_STAGE_NAMES.length];
            int reps = 3;
            for (int i = 0; i < reps; i++) {
                long[] run = runOnce(scenario.rows, scenario.minSupport);
                for (int s = 0; s < run.length; s++) {
                    scenarioTotals[s] += run[s];
                    aggregate[s] += run[s];
                }
                long[] runIndexSub = runIndexSubTimings(scenario.rows, scenario.minSupport);
                for (int s = 0; s < runIndexSub.length; s++) {
                    scenarioIndexSubTotals[s] += runIndexSub[s];
                    aggregateIndexSub[s] += runIndexSub[s];
                }
                measuredRuns++;
            }
            printStageBreakdown("scenario=" + scenario.name, scenarioTotals, reps);
            printIndexSubStageBreakdown("scenario=" + scenario.name, scenarioIndexSubTotals, reps);
        }

        printStageBreakdown("aggregate", aggregate, measuredRuns);
        printIndexSubStageBreakdown("aggregate", aggregateIndexSub, measuredRuns);
        int hottestStage = hottestStage(aggregate);
        System.out.println("[hotpath-audit] hottestStage=" + STAGE_NAMES[hottestStage]
                + ", avgMs=" + (aggregate[hottestStage] / Math.max(1, measuredRuns)));

        assertTrue(aggregate[STAGE_TOTAL] > 0L);
        assertTrue(aggregate[hottestStage] > 0L);
    }

    private static long[] runOnce(List<DocTerms> rows, int minSupport) {
        long[] out = new long[STAGE_NAMES.length];
        final TermTidsetIndex[] indexHolder = new TermTidsetIndex[1];

        out[STAGE_INDEX] = PerfTestSupport.elapsedMillis(() ->
                indexHolder[0] = TermTidsetIndex.buildWithSupportBounds(
                        rows, minSupport, EngineTuningConfig.DEFAULT_MAX_DOC_COVERAGE_RATIO));
        TermTidsetIndex index = indexHolder[0];

        BeamFrequentItemsetMiner miner = new BeamFrequentItemsetMiner();
        SelectorConfig selectorConfig = new SelectorConfig(
                minSupport,
                2,
                EngineTuningConfig.DEFAULT_MAX_ITEMSET_SIZE,
                EngineTuningConfig.DEFAULT_MAX_CANDIDATE_COUNT
        );
        final FrequentItemsetMiningResult[] miningHolder = new FrequentItemsetMiningResult[1];
        out[STAGE_MINE] = PerfTestSupport.elapsedMillis(() ->
                miningHolder[0] = miner.mineWithStats(
                        index.getTidsetsByTermIdUnsafe(),
                        selectorConfig,
                        EngineTuningConfig.DEFAULT_MAX_FREQUENT_TERM_COUNT,
                        EngineTuningConfig.DEFAULT_MAX_BRANCHING_FACTOR,
                        EngineTuningConfig.FACADE_DEFAULT_BEAM_WIDTH,
                        2,
                        3000L));
        FrequentItemsetMiningResult mining = miningHolder[0];

        TwoPhaseExclusiveItemsetPicker picker = new TwoPhaseExclusiveItemsetPicker();
        int dictionarySize = Math.max(1, index.getIdToTermUnsafe().size());
        final List<CandidateItemset>[] selectedCandidateHolder = new List[1];
        out[STAGE_PICK] = PerfTestSupport.elapsedMillis(() ->
                selectedCandidateHolder[0] = picker.pick(
                        mining.getCandidates(),
                        dictionarySize,
                        EngineTuningConfig.DEFAULT_MAX_SWAP_TRIALS,
                        EngineTuningConfig.DEFAULT_MIN_NET_GAIN));
        List<CandidateItemset> selectedCandidates = selectedCandidateHolder[0];
        List<SelectedGroup> selectedGroups = toSelectedGroups(selectedCandidates, index.getIdToTermUnsafe());

        ExclusiveSelectionResult selectionResult = new ExclusiveSelectionResult(
                selectedGroups,
                mining.getFrequentTermCount(),
                mining.getGeneratedCandidateCount(),
                mining.getIntersectionCount(),
                selectorConfig.getMaxCandidateCount(),
                mining.isTruncatedByCandidateLimit()
        );

        final LineFileProcessingResult.DerivedData[] derivedHolder =
                new LineFileProcessingResult.DerivedData[1];
        out[STAGE_DERIVED] = PerfTestSupport.elapsedMillis(() ->
                derivedHolder[0] = ExclusiveFpRowsProcessingApi.buildDerivedData(
                        rows,
                        selectionResult,
                        EngineTuningConfig.DEFAULT_HOT_TERM_THRESHOLD_EXCLUSIVE));

        out[STAGE_FINAL_INDEX] = PerfTestSupport.elapsedMillis(() ->
                new LineFileProcessingResult(
                        rows,
                        selectionResult,
                        derivedHolder[0],
                        EngineTuningConfig.DEFAULT_SKIP_HASH_MIN_GRAM,
                        EngineTuningConfig.DEFAULT_SKIP_HASH_MAX_GRAM));

        out[STAGE_TOTAL] = out[STAGE_INDEX] + out[STAGE_MINE] + out[STAGE_PICK]
                + out[STAGE_DERIVED] + out[STAGE_FINAL_INDEX];
        return out;
    }

    private static long[] runIndexSubTimings(List<DocTerms> rows, int minSupport) {
        long[] out = new long[INDEX_SUB_STAGE_NAMES.length];
        TermTidsetIndex.BuildWithSupportBoundsTimings timings =
                new TermTidsetIndex.BuildWithSupportBoundsTimings();
        TermTidsetIndex.buildWithSupportBounds(
                rows,
                minSupport,
                EngineTuningConfig.DEFAULT_MAX_DOC_COVERAGE_RATIO,
                timings
        );
        out[INDEX_SUB_FIRST_PASS] = timings.getFirstPassMillis();
        out[INDEX_SUB_FILTER_PREPARE] = timings.getFilterPrepareMillis();
        out[INDEX_SUB_SECOND_PASS] = timings.getSecondPassMillis();
        out[INDEX_SUB_TOTAL] = timings.getTotalMillis();
        return out;
    }

    private static List<SelectedGroup> toSelectedGroups(
            List<CandidateItemset> candidates,
            List<byte[]> idToTerm
    ) {
        List<SelectedGroup> out = new ArrayList<SelectedGroup>(candidates.size());
        for (int i = 0; i < candidates.size(); i++) {
            CandidateItemset candidate = candidates.get(i);
            int[] termIds = candidate.getTermIdsUnsafe();
            List<byte[]> terms = new ArrayList<byte[]>(termIds.length);
            for (int t = 0; t < termIds.length; t++) {
                terms.add(ByteArrayUtils.copy(idToTerm.get(termIds[t])));
            }
            out.add(new SelectedGroup(
                    terms,
                    bitSetToDocIds(candidate.getDocBitsUnsafe()),
                    candidate.getSupport(),
                    candidate.getEstimatedSaving()
            ));
        }
        return out;
    }

    private static List<Integer> bitSetToDocIds(BitSet bits) {
        List<Integer> out = new ArrayList<Integer>(bits.cardinality());
        for (int i = bits.nextSetBit(0); i >= 0; i = bits.nextSetBit(i + 1)) {
            out.add(i);
        }
        return out;
    }

    private static void printStageBreakdown(String title, long[] totals, int runs) {
        long safeRuns = Math.max(1, runs);
        long totalMs = Math.max(1L, totals[STAGE_TOTAL] / safeRuns);
        StringBuilder sb = new StringBuilder();
        sb.append("[hotpath-audit] ").append(title).append(" runs=").append(runs);
        for (int i = 0; i < STAGE_NAMES.length - 1; i++) {
            long avg = totals[i] / safeRuns;
            long share = (avg * 100L) / totalMs;
            sb.append(" | ").append(STAGE_NAMES[i]).append("=").append(avg).append("ms")
                    .append(" (").append(share).append("%)");
        }
        sb.append(" | total=").append(totalMs).append("ms");
        System.out.println(sb.toString());
    }

    private static void printIndexSubStageBreakdown(String title, long[] totals, int runs) {
        long safeRuns = Math.max(1, runs);
        long totalMs = Math.max(1L, totals[INDEX_SUB_TOTAL] / safeRuns);
        StringBuilder sb = new StringBuilder();
        sb.append("[hotpath-audit:index] ").append(title).append(" runs=").append(runs);
        for (int i = 0; i < INDEX_SUB_STAGE_NAMES.length - 1; i++) {
            long avg = totals[i] / safeRuns;
            long share = (avg * 100L) / totalMs;
            sb.append(" | ").append(INDEX_SUB_STAGE_NAMES[i]).append("=").append(avg).append("ms")
                    .append(" (").append(share).append("%)");
        }
        sb.append(" | total=").append(totalMs).append("ms");
        System.out.println(sb.toString());
    }

    private static int hottestStage(long[] totals) {
        int best = 0;
        for (int i = 1; i < STAGE_NAMES.length - 1; i++) {
            if (totals[i] > totals[best]) {
                best = i;
            }
        }
        return best;
    }

    private static final class Scenario {
        private final String name;
        private final List<DocTerms> rows;
        private final int minSupport;

        private Scenario(String name, List<DocTerms> rows, int minSupport) {
            this.name = name;
            this.rows = rows;
            this.minSupport = minSupport;
        }
    }
}
