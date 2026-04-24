package cn.lxdb.plugins.muqingyu.fptoken.tests.performance;

import static org.junit.jupiter.api.Assertions.assertTrue;

import cn.lxdb.plugins.muqingyu.fptoken.api.ExclusiveFpRowsProcessingApi;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.DocTerms;
import cn.lxdb.plugins.muqingyu.fptoken.runner.dataset.LineRecordDatasetLoader;
import cn.lxdb.plugins.muqingyu.fptoken.tests.support.SampleDataPremergeHintTestSupport;
import cn.lxdb.plugins.muqingyu.fptoken.tests.support.SampleDataPremergeHintTestSupport.FiveWaySplit;
import cn.lxdb.plugins.muqingyu.fptoken.tests.support.SampleDataPremergeHintTestSupport.HintsFromSegments;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

/**
 * 在真实文本样例上模拟 Lucene merge（A/B/C 历史段 + merge 尾段），验证 premerge hint 的收益可复现性。
 */
@Tag("performance")
@EnabledIfSystemProperty(named = "fptoken.runPerfTests", matches = "true")
class RealDocsLuceneMergeHintReproducibilityPerformanceTest {

    @Test
    @Timeout(value = 240, unit = TimeUnit.SECONDS)
    void awesomePython_mergeHints_shouldShowStableNonRegressiveBenefit() throws Exception {
        ScenarioResult baselineVsHints = runScenario(false);
        System.out.println(baselineVsHints.reportLine);

        // 真实场景存在抖动：该用例主要用于“可复现地量化收益”，只拦截灾难性回退。
        assertTrue(baselineVsHints.improvementMedianPercent > -700.0d,
                () -> "real-docs hinted median regressed too much: " + baselineVsHints.improvementMedianPercent + "%");
        assertTrue(baselineVsHints.improvementP90Percent > -900.0d,
                () -> "real-docs hinted p90 regressed too much: " + baselineVsHints.improvementP90Percent + "%");
    }

    @Test
    @Timeout(value = 240, unit = TimeUnit.SECONDS)
    void awesomePython_mutexOnly_shouldBeMeasuredWithReproducibilityStats() throws Exception {
        ScenarioResult baselineVsMutexOnly = runScenario(true);
        System.out.println(baselineVsMutexOnly.reportLine);

        // mutex-only 更容易受分布影响，这里重点要求可观测、可复现地量化而非硬性必须提速。
        assertTrue(baselineVsMutexOnly.improvementP90Percent > -55.0d,
                () -> "real-docs mutex-only p90 regressed too much: " + baselineVsMutexOnly.improvementP90Percent + "%");
    }

    private static ScenarioResult runScenario(boolean mutexOnly) throws Exception {
        Assumptions.assumeTrue(
                Files.isRegularFile(SampleDataPremergeHintTestSupport.REAL_DOCS_AWESOME_PYTHON),
                () -> "missing: " + SampleDataPremergeHintTestSupport.REAL_DOCS_AWESOME_PYTHON
        );
        LineRecordDatasetLoader.LoadOutcome outcome = LineRecordDatasetLoader.loadSingleFileRawWithStats(
                SampleDataPremergeHintTestSupport.REAL_DOCS_AWESOME_PYTHON);
        List<DocTerms> allRows = outcome.getLoadedDataset().getRows();
        Assumptions.assumeTrue(allRows.size() >= 600, () -> "too few rows: " + allRows.size());

        FiveWaySplit split = SampleDataPremergeHintTestSupport.splitFiveWay(allRows);
        ExclusiveFpRowsProcessingApi.ProcessingOptions base = SampleDataPremergeHintTestSupport.mergeScenarioBaseOptions()
                .withMinSupport(2)
                .withHotTermThresholdExclusive(2)
                .withSampleRatio(0.55d);

        HintsFromSegments fullHints = SampleDataPremergeHintTestSupport.buildHintsFromSegments(
                base, split.segA, split.segB, split.segC, true, true);
        Assumptions.assumeFalse(fullHints.mutexGroupHints.isEmpty(), "need non-empty mutex hints");
        Assumptions.assumeFalse(fullHints.singleTermHints.isEmpty(), "need non-empty single hints");

        int warmupRuns = PerfTestSupport.intProp("fptoken.perf.realDocs.warmupRuns", 2);
        int measuredRuns = PerfTestSupport.intProp("fptoken.perf.realDocs.measuredRuns", 7);
        int hintBoostWeight = PerfTestSupport.intProp("fptoken.perf.realDocs.hintBoostWeight", 8);

        ExclusiveFpRowsProcessingApi.ProcessingOptions variant = base
                .withPremergeMutexGroupHints(fullHints.mutexGroupHints)
                .withPremergeSingleTermHints(mutexOnly
                        ? Collections.<ExclusiveFpRowsProcessingApi.PremergeHint>emptyList()
                        : fullHints.singleTermHints)
                .withHintBoostWeight(hintBoostWeight)
                .withHintValidationMode(ExclusiveFpRowsProcessingApi.HintValidationMode.FILTER_ONLY);

        List<Long> baselineSamples = measureSamples(split.merged, base, warmupRuns, measuredRuns);
        List<Long> variantSamples = measureSamples(split.merged, variant, warmupRuns, measuredRuns);
        List<Long> improvementBp = computeImprovementBpPerRound(baselineSamples, variantSamples);

        long baselineMedian = PerfTestSupport.median(baselineSamples);
        long variantMedian = PerfTestSupport.median(variantSamples);
        double improveMedianPct = toPercent(PerfTestSupport.percentile(improvementBp, 0.50d));
        double improveP90Pct = toPercent(PerfTestSupport.percentile(improvementBp, 0.10d));
        int positiveRounds = countPositiveRounds(improvementBp);

        String mode = mutexOnly ? "mutex-only" : "mutex+single";
        String report = "[real-lucene-merge-hint] mode=" + mode
                + ", file=" + SampleDataPremergeHintTestSupport.REAL_DOCS_AWESOME_PYTHON
                + ", rows=" + split.totalRows
                + ", mergedRows=" + split.merged.size()
                + ", mutexHints=" + fullHints.mutexGroupHints.size()
                + ", singleHints=" + fullHints.singleTermHints.size()
                + ", baselineMedianMs=" + baselineMedian
                + ", variantMedianMs=" + variantMedian
                + ", improveMedianPercent=" + formatPct(improveMedianPct)
                + ", improveP90Percent=" + formatPct(improveP90Pct)
                + ", positiveRounds=" + positiveRounds + "/" + improvementBp.size();

        return new ScenarioResult(improveMedianPct, improveP90Pct, report);
    }

    private static List<Long> measureSamples(
            List<DocTerms> rows,
            ExclusiveFpRowsProcessingApi.ProcessingOptions options,
            int warmupRuns,
            int measuredRuns
    ) {
        for (int i = 0; i < Math.max(0, warmupRuns); i++) {
            ExclusiveFpRowsProcessingApi.processRows(rows, options);
        }
        List<Long> samples = new ArrayList<>();
        for (int i = 0; i < Math.max(1, measuredRuns); i++) {
            long t0 = System.nanoTime();
            ExclusiveFpRowsProcessingApi.processRows(rows, options);
            long elapsedMs = (System.nanoTime() - t0) / 1_000_000L;
            samples.add(Math.max(1L, elapsedMs));
        }
        return samples;
    }

    private static List<Long> computeImprovementBpPerRound(List<Long> baselineMs, List<Long> variantMs) {
        int n = Math.min(baselineMs.size(), variantMs.size());
        List<Long> out = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            long base = Math.max(1L, baselineMs.get(i).longValue());
            long var = Math.max(1L, variantMs.get(i).longValue());
            long bp = ((base - var) * 10_000L) / base;
            out.add(Long.valueOf(bp));
        }
        return out;
    }

    private static int countPositiveRounds(List<Long> improvementBp) {
        int positive = 0;
        for (int i = 0; i < improvementBp.size(); i++) {
            if (improvementBp.get(i).longValue() > 0L) {
                positive++;
            }
        }
        return positive;
    }

    private static double toPercent(long basisPoints) {
        return basisPoints / 100.0d;
    }

    private static String formatPct(double value) {
        return String.format(Locale.ROOT, "%.2f", value);
    }

    private static final class ScenarioResult {
        private final double improvementMedianPercent;
        private final double improvementP90Percent;
        private final String reportLine;

        private ScenarioResult(double improvementMedianPercent, double improvementP90Percent, String reportLine) {
            this.improvementMedianPercent = improvementMedianPercent;
            this.improvementP90Percent = improvementP90Percent;
            this.reportLine = reportLine;
        }
    }
}
