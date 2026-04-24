package cn.lxdb.plugins.muqingyu.fptoken.tests.performance;

import static org.junit.jupiter.api.Assertions.assertTrue;

import cn.lxdb.plugins.muqingyu.fptoken.api.ExclusiveFpRowsProcessingApi;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.ByteRef;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.DocTerms;
import cn.lxdb.plugins.muqingyu.fptoken.runner.dataset.LineRecordDatasetLoader;
import cn.lxdb.plugins.muqingyu.fptoken.runner.result.LineFileProcessingResult;
import cn.lxdb.plugins.muqingyu.fptoken.tests.support.SampleDataPremergeHintTestSupport;
import cn.lxdb.plugins.muqingyu.fptoken.tests.support.SampleDataPremergeHintTestSupport.FiveWaySplit;
import cn.lxdb.plugins.muqingyu.fptoken.tests.support.SampleDataPremergeHintTestSupport.HintsFromSegments;
import java.nio.file.Files;
import java.nio.file.Path;
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
 * 使用 {@code sample-data/line-records} 样例行数据，对比 merge 尾段上预合并提示对耗时的影响。
 *
 * <p>包含两类用例：</p>
 * <ul>
 *   <li>baseline vs 同时启用 {@code premergeMutexGroupHints} + {@code premergeSingleTermHints}</li>
 *   <li>四向对比：不用 / 仅用 mutex 组 / 仅用 single term / 两者同开（完整评估两类 hint 各自贡献）</li>
 * </ul>
 *
 * <p>{@code records_004_limit32000.txt}：行数最多、行长触顶(64B)档，主压力用例。</p>
 * <p>{@code records_002_medium.txt}：中等规模快速回归。</p>
 *
 * <p>跑法：{@code -Dfptoken.runPerfTests=true}；可选 {@code -Dfptoken.perf.sampleData.warmupRuns=} /
 * {@code -Dfptoken.perf.sampleData.measuredRuns=}。</p>
 */
@Tag("performance")
@EnabledIfSystemProperty(named = "fptoken.runPerfTests", matches = "true")
class SampleDataLineRecordsPremergeHintPerformanceTest {

    @Test
    @Timeout(value = 300, unit = TimeUnit.SECONDS)
    void records004_rawNgram_mergeTail_withAndWithoutPremergeHints() throws Exception {
        runRawMergePerf(SampleDataPremergeHintTestSupport.LINE_RECORDS_004, 4_000);
    }

    @Test
    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    void records002_rawNgram_mergeTail_withAndWithoutPremergeHints() throws Exception {
        runRawMergePerf(SampleDataPremergeHintTestSupport.LINE_RECORDS_002, 800);
    }

    @Test
    @Timeout(value = 360, unit = TimeUnit.SECONDS)
    void records004_rawNgram_mergeTail_mutexOnlySingleOnlyBoth_benchmark() throws Exception {
        runFourWayHintVariantBenchmark(SampleDataPremergeHintTestSupport.LINE_RECORDS_004, 4_000);
    }

    @Test
    @Timeout(value = 180, unit = TimeUnit.SECONDS)
    void records002_rawNgram_mergeTail_mutexOnlySingleOnlyBoth_benchmark() throws Exception {
        runFourWayHintVariantBenchmark(SampleDataPremergeHintTestSupport.LINE_RECORDS_002, 800);
    }

    private static void runRawMergePerf(Path sampleFile, int minRows) throws Exception {
        Assumptions.assumeTrue(Files.isRegularFile(sampleFile), () -> "missing: " + sampleFile);

        LineRecordDatasetLoader.LoadOutcome outcome =
                LineRecordDatasetLoader.loadSingleFileRawWithStats(sampleFile);
        List<DocTerms> allRows = outcome.getLoadedDataset().getRows();
        Assumptions.assumeTrue(allRows.size() >= minRows, () -> "too few rows: " + allRows.size());

        FiveWaySplit split = SampleDataPremergeHintTestSupport.splitFiveWay(allRows);
        int n = split.totalRows;

        int warmupRuns = PerfTestSupport.intProp("fptoken.perf.sampleData.warmupRuns", 1);
        int measuredRuns = PerfTestSupport.intProp("fptoken.perf.sampleData.measuredRuns", 3);

        ExclusiveFpRowsProcessingApi.ProcessingOptions baseOptions =
                SampleDataPremergeHintTestSupport.mergeScenarioBaseOptions();

        HintsFromSegments hints = SampleDataPremergeHintTestSupport.buildHintsFromSegments(
                baseOptions, split.segA, split.segB, split.segC, true, true);
        List<ExclusiveFpRowsProcessingApi.PremergeHint> coverageHints = new ArrayList<>();
        coverageHints.addAll(hints.mutexGroupHints);
        coverageHints.addAll(hints.singleTermHints);
        List<DocTerms> mergedForCoverage = ExclusiveFpRowsProcessingApi.IntermediateSteps.tokenizeRowsForMining(
                ExclusiveFpRowsProcessingApi.IntermediateSteps.copyRows(split.merged),
                baseOptions.getNgramStart(),
                baseOptions.getNgramEnd());
        double hintCoverage = estimateHintCoverage(mergedForCoverage, coverageHints);

        ExclusiveFpRowsProcessingApi.ProcessingOptions hintedOptions = baseOptions
                .withPremergeMutexGroupHints(hints.mutexGroupHints)
                .withPremergeSingleTermHints(hints.singleTermHints)
                .withHintBoostWeight(8)
                .withHintValidationMode(ExclusiveFpRowsProcessingApi.HintValidationMode.FILTER_ONLY);

        RunMetrics baseline = measure(split.merged, baseOptions, warmupRuns, measuredRuns);
        RunMetrics hinted = measure(split.merged, hintedOptions, warmupRuns, measuredRuns);

        double improvementPercent = baseline.medianMs <= 0L
                ? 0.0d
                : ((baseline.medianMs - hinted.medianMs) * 100.0d) / baseline.medianMs;

        System.out.println(
                "[sample-data premerge-hint] file=" + sampleFile
                        + ", totalRows=" + n
                        + ", loaderTotalLines=" + outcome.getStats().getTotalLines()
                        + ", segSize=" + split.segSize
                        + ", mergedDocs=" + split.merged.size()
                        + ", mutexHints=" + hints.mutexGroupHints.size()
                        + ", singleHints=" + hints.singleTermHints.size()
                        + ", baselineMedianMs=" + baseline.medianMs
                        + ", hintedMedianMs=" + hinted.medianMs
                        + ", improvementPercent=" + improvementPercent
                        + ", baselineCandidates=" + baseline.candidateCount
                        + ", hintedCandidates=" + hinted.candidateCount
                        + ", baselineGroups=" + baseline.groupCount
                        + ", hintedGroups=" + hinted.groupCount
                        + ", hintCoveragePercent=" + hintCoverage
        );

        long baselineFloorMs = Math.max(20L, baseline.medianMs);
        assertTrue(hinted.medianMs <= baselineFloorMs * 1.50d,
                () -> "hint path regressed too much, baseline=" + baseline.medianMs + "ms, hinted=" + hinted.medianMs + "ms");
    }

    /**
     * 在同一份 A/B/C 上只生成一次 full hints，再拆成 mutex-only / single-only / both，
     * 测 merge 尾段上四种配置的 {@link ExclusiveFpRowsProcessingApi#processRows} 中位耗时。
     */
    private static void runFourWayHintVariantBenchmark(Path sampleFile, int minRows) throws Exception {
        Assumptions.assumeTrue(Files.isRegularFile(sampleFile), () -> "missing: " + sampleFile);

        LineRecordDatasetLoader.LoadOutcome outcome =
                LineRecordDatasetLoader.loadSingleFileRawWithStats(sampleFile);
        List<DocTerms> allRows = outcome.getLoadedDataset().getRows();
        Assumptions.assumeTrue(allRows.size() >= minRows, () -> "too few rows: " + allRows.size());

        FiveWaySplit split = SampleDataPremergeHintTestSupport.splitFiveWay(allRows);
        int warmupRuns = PerfTestSupport.intProp("fptoken.perf.sampleData.warmupRuns", 1);
        int measuredRuns = PerfTestSupport.intProp("fptoken.perf.sampleData.measuredRuns", 3);

        ExclusiveFpRowsProcessingApi.ProcessingOptions base =
                SampleDataPremergeHintTestSupport.mergeScenarioBaseOptions();
        HintsFromSegments fullHints = SampleDataPremergeHintTestSupport.buildHintsFromSegments(
                base, split.segA, split.segB, split.segC, true, true);

        Assumptions.assumeFalse(fullHints.mutexGroupHints.isEmpty(), "need non-empty mutex hints for 4-way benchmark");
        Assumptions.assumeFalse(fullHints.singleTermHints.isEmpty(), "need non-empty single-term hints for 4-way benchmark");

        List<DocTerms> mergedForCoverage = ExclusiveFpRowsProcessingApi.IntermediateSteps.tokenizeRowsForMining(
                ExclusiveFpRowsProcessingApi.IntermediateSteps.copyRows(split.merged),
                base.getNgramStart(),
                base.getNgramEnd());
        List<ExclusiveFpRowsProcessingApi.PremergeHint> allHintsForCoverage = new ArrayList<>();
        allHintsForCoverage.addAll(fullHints.mutexGroupHints);
        allHintsForCoverage.addAll(fullHints.singleTermHints);
        double coverageAll = estimateHintCoverage(mergedForCoverage, allHintsForCoverage);
        double coverageMutex = estimateHintCoverage(mergedForCoverage, fullHints.mutexGroupHints);
        double coverageSingle = estimateHintCoverage(mergedForCoverage, fullHints.singleTermHints);

        int boost = PerfTestSupport.intProp("fptoken.perf.sampleData.hintBoostWeight", 8);
        ExclusiveFpRowsProcessingApi.ProcessingOptions mutexOnly = base
                .withPremergeMutexGroupHints(fullHints.mutexGroupHints)
                .withPremergeSingleTermHints(Collections.<ExclusiveFpRowsProcessingApi.PremergeHint>emptyList())
                .withHintBoostWeight(boost)
                .withHintValidationMode(ExclusiveFpRowsProcessingApi.HintValidationMode.FILTER_ONLY);
        ExclusiveFpRowsProcessingApi.ProcessingOptions singleOnly = base
                .withPremergeMutexGroupHints(Collections.<ExclusiveFpRowsProcessingApi.PremergeHint>emptyList())
                .withPremergeSingleTermHints(fullHints.singleTermHints)
                .withHintBoostWeight(boost)
                .withHintValidationMode(ExclusiveFpRowsProcessingApi.HintValidationMode.FILTER_ONLY);
        ExclusiveFpRowsProcessingApi.ProcessingOptions bothHints = base
                .withPremergeMutexGroupHints(fullHints.mutexGroupHints)
                .withPremergeSingleTermHints(fullHints.singleTermHints)
                .withHintBoostWeight(boost)
                .withHintValidationMode(ExclusiveFpRowsProcessingApi.HintValidationMode.FILTER_ONLY);

        RunMetrics baseline = measure(split.merged, base, warmupRuns, measuredRuns);
        RunMetrics mutexM = measure(split.merged, mutexOnly, warmupRuns, measuredRuns);
        RunMetrics singleM = measure(split.merged, singleOnly, warmupRuns, measuredRuns);
        RunMetrics bothM = measure(split.merged, bothHints, warmupRuns, measuredRuns);

        System.out.println(
                "[sample-data premerge-hint 4way] file=" + sampleFile
                        + ", totalRows=" + split.totalRows
                        + ", mergedDocs=" + split.merged.size()
                        + ", mutexHintCount=" + fullHints.mutexGroupHints.size()
                        + ", singleHintCount=" + fullHints.singleTermHints.size()
                        + ", hintBoostWeight=" + boost
        );
        System.out.println(
                "[sample-data premerge-hint 4way] medianMs: baseline=" + baseline.medianMs
                        + ", mutexOnly=" + mutexM.medianMs
                        + ", singleOnly=" + singleM.medianMs
                        + ", both=" + bothM.medianMs
        );
        System.out.println(
                "[sample-data premerge-hint 4way] vsBaselineMsImprove%: mutexOnly="
                        + formatImprovePercent(baseline.medianMs, mutexM.medianMs)
                        + ", singleOnly=" + formatImprovePercent(baseline.medianMs, singleM.medianMs)
                        + ", both=" + formatImprovePercent(baseline.medianMs, bothM.medianMs)
                        + " (positive => faster than baseline)"
        );
        System.out.println(
                "[sample-data premerge-hint 4way] candidates: baseline=" + baseline.candidateCount
                        + ", mutexOnly=" + mutexM.candidateCount
                        + ", singleOnly=" + singleM.candidateCount
                        + ", both=" + bothM.candidateCount
        );
        System.out.println(
                "[sample-data premerge-hint 4way] hintCoverageOnMergeTokenized%: all=" + coverageAll
                        + ", mutexSubset=" + coverageMutex
                        + ", singleSubset=" + coverageSingle
        );

        long baselineFloorMs = Math.max(20L, baseline.medianMs);
        assertTrue(mutexM.medianMs <= baselineFloorMs * 1.80d,
                () -> "mutex-only regressed too much vs baseline, baseline=" + baseline.medianMs + ", mutexOnly=" + mutexM.medianMs);
        assertTrue(singleM.medianMs <= baselineFloorMs * 1.80d,
                () -> "single-only regressed too much vs baseline, baseline=" + baseline.medianMs + ", singleOnly=" + singleM.medianMs);
        assertTrue(bothM.medianMs <= baselineFloorMs * 1.50d,
                () -> "both-hints regressed too much vs baseline, baseline=" + baseline.medianMs + ", both=" + bothM.medianMs);
    }

    private static String formatImprovePercent(long baselineMs, long variantMs) {
        if (baselineMs <= 0L) {
            return "n/a";
        }
        double pct = ((baselineMs - variantMs) * 100.0d) / baselineMs;
        return String.format(Locale.ROOT, "%.2f", pct);
    }

    private static RunMetrics measure(
            List<DocTerms> rows,
            ExclusiveFpRowsProcessingApi.ProcessingOptions options,
            int warmupRuns,
            int measuredRuns
    ) {
        for (int i = 0; i < Math.max(0, warmupRuns); i++) {
            ExclusiveFpRowsProcessingApi.processRows(rows, options);
        }
        List<Long> samples = new ArrayList<>();
        LineFileProcessingResult last = null;
        for (int i = 0; i < Math.max(1, measuredRuns); i++) {
            long t0 = System.nanoTime();
            last = ExclusiveFpRowsProcessingApi.processRows(rows, options);
            samples.add((System.nanoTime() - t0) / 1_000_000L);
        }
        long medianMs = PerfTestSupport.median(samples);
        return new RunMetrics(
                medianMs,
                last.getSelectionResult().getCandidateCount(),
                last.getFinalIndexData().getHighFreqMutexGroupPostings().size()
        );
    }

    private static double estimateHintCoverage(
            List<DocTerms> rows,
            List<ExclusiveFpRowsProcessingApi.PremergeHint> hints
    ) {
        if (hints.isEmpty()) {
            return 0.0d;
        }
        int matched = 0;
        for (int i = 0; i < hints.size(); i++) {
            if (hintAppearsInRows(hints.get(i), rows)) {
                matched++;
            }
        }
        return (matched * 100.0d) / hints.size();
    }

    private static boolean hintAppearsInRows(
            ExclusiveFpRowsProcessingApi.PremergeHint hint,
            List<DocTerms> rows
    ) {
        List<ByteRef> hintTerms = hint.getTermRefs();
        for (int i = 0; i < rows.size(); i++) {
            List<ByteRef> rowTerms = rows.get(i).getTermRefsUnsafe();
            int found = 0;
            for (int h = 0; h < hintTerms.size(); h++) {
                if (containsRef(rowTerms, hintTerms.get(h))) {
                    found++;
                }
            }
            if (found == hintTerms.size()) {
                return true;
            }
        }
        return false;
    }

    private static boolean containsRef(List<ByteRef> haystack, ByteRef needle) {
        for (int i = 0; i < haystack.size(); i++) {
            ByteRef h = haystack.get(i);
            if (h.getLength() != needle.getLength()) {
                continue;
            }
            byte[] hs = h.getSourceUnsafe();
            byte[] ns = needle.getSourceUnsafe();
            int ho = h.getOffset();
            int no = needle.getOffset();
            boolean same = true;
            for (int p = 0; p < h.getLength(); p++) {
                if (hs[ho + p] != ns[no + p]) {
                    same = false;
                    break;
                }
            }
            if (same) {
                return true;
            }
        }
        return false;
    }

    private static final class RunMetrics {
        private final long medianMs;
        private final int candidateCount;
        private final int groupCount;

        private RunMetrics(long medianMs, int candidateCount, int groupCount) {
            this.medianMs = medianMs;
            this.candidateCount = candidateCount;
            this.groupCount = groupCount;
        }
    }
}
