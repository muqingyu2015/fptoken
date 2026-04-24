package cn.lxdb.plugins.muqingyu.fptoken.tests.performance;

import static org.junit.jupiter.api.Assertions.assertTrue;

import cn.lxdb.plugins.muqingyu.fptoken.api.ExclusiveFpRowsProcessingApi;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.ByteRef;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.DocTerms;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.SelectedGroup;
import cn.lxdb.plugins.muqingyu.fptoken.runner.result.LineFileProcessingResult;
import java.nio.charset.StandardCharsets;
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
 * merge 场景性能对比：有/无 premerge hints。
 *
 * <p>真实模拟 A/B/C 历史段先处理，再对合并段 D 做处理；D 使用 A/B/C 的高频结果 term 作为 hints。
 * 若提升不明显，会打印原因分析指标（hint 覆盖率、候选规模变化、互斥组变化）。</p>
 */
@Tag("performance")
@EnabledIfSystemProperty(named = "fptoken.runPerfTests", matches = "true")
@Timeout(value = 40, unit = TimeUnit.SECONDS)
class MergePremergeHintAccelerationPerformanceTest {

    @Test
    void mergeHints_shouldImproveOrExplainOnMergeLikeWorkload() {
        int segmentDocs = PerfTestSupport.intProp("fptoken.perf.mergeHint.segmentDocs", 900);
        int mergedDocs = PerfTestSupport.intProp("fptoken.perf.mergeHint.mergedDocs", 2400);
        int warmupRuns = PerfTestSupport.intProp("fptoken.perf.mergeHint.warmupRuns", 2);
        int measuredRuns = PerfTestSupport.intProp("fptoken.perf.mergeHint.measuredRuns", 5);

        List<DocTerms> segA = buildMergeLikeRows(0, segmentDocs, 11);
        List<DocTerms> segB = buildMergeLikeRows(segmentDocs, segmentDocs, 23);
        List<DocTerms> segC = buildMergeLikeRows(segmentDocs * 2, segmentDocs, 37);
        List<DocTerms> merged = buildMergeLikeRows(10_000, mergedDocs, 91);

        ExclusiveFpRowsProcessingApi.ProcessingOptions baseOptions = ExclusiveFpRowsProcessingApi.defaultOptions()
                .withMinSupport(3)
                .withMinItemsetSize(2)
                .withMaxItemsetSize(6)
                .withMaxCandidateCount(220_000)
                .withHotTermThresholdExclusive(4)
                .withSampleRatio(0.45d)
                .withHintBoostWeight(0);

        SegmentPremergeHints hints =
                buildHintsFromSegments(baseOptions, segA, segB, segC, true, true);
        List<ExclusiveFpRowsProcessingApi.PremergeHint> coverageHints = new ArrayList<>();
        coverageHints.addAll(hints.mutexGroupHints);
        coverageHints.addAll(hints.singleTermHints);
        double hintCoverage = estimateHintCoverage(merged, coverageHints);

        ExclusiveFpRowsProcessingApi.ProcessingOptions hintedOptions = baseOptions
                .withPremergeMutexGroupHints(hints.mutexGroupHints)
                .withPremergeSingleTermHints(hints.singleTermHints)
                .withHintBoostWeight(8)
                .withHintValidationMode(ExclusiveFpRowsProcessingApi.HintValidationMode.FILTER_ONLY);

        RunMetrics baseline = measure(merged, baseOptions, warmupRuns, measuredRuns);
        RunMetrics hinted = measure(merged, hintedOptions, warmupRuns, measuredRuns);

        double improvementPercent = baseline.medianMs <= 0L
                ? 0.0d
                : ((baseline.medianMs - hinted.medianMs) * 100.0d) / baseline.medianMs;
        String reason = diagnoseNoImprovement(baseline, hinted, hintCoverage);
        System.out.println(
                "[merge-hint-perf] baselineMedianMs=" + baseline.medianMs
                        + ", hintedMedianMs=" + hinted.medianMs
                        + ", improvementPercent=" + improvementPercent
                        + ", baselineCandidates=" + baseline.candidateCount
                        + ", hintedCandidates=" + hinted.candidateCount
                        + ", baselineGroups=" + baseline.groupCount
                        + ", hintedGroups=" + hinted.groupCount
                        + ", hintCoveragePercent=" + hintCoverage
                        + ", reason=" + reason
        );

        // 真实性能验证：至少不应显著回退。小样本下 baseline 可能很小（个位数 ms），这里做地板保护避免抖动误判。
        long baselineFloorMs = Math.max(20L, baseline.medianMs);
        assertTrue(hinted.medianMs <= baselineFloorMs * 1.50d,
                () -> "hint path regressed too much, baseline=" + baseline.medianMs + "ms, hinted=" + hinted.medianMs + "ms");
        assertTrue(reason.length() > 10);
    }

    @Test
    void mergeHints_mutexOnlySingleOnlyAndBoth_shouldBenchmarkAgainstNoHint() {
        int segmentDocs = PerfTestSupport.intProp("fptoken.perf.mergeHint.segmentDocs", 700);
        int mergedDocs = PerfTestSupport.intProp("fptoken.perf.mergeHint.mergedDocs", 1800);
        int warmupRuns = PerfTestSupport.intProp("fptoken.perf.mergeHint.warmupRuns", 1);
        int measuredRuns = PerfTestSupport.intProp("fptoken.perf.mergeHint.measuredRuns", 3);

        List<DocTerms> segA = buildMergeLikeRows(0, segmentDocs, 7);
        List<DocTerms> segB = buildMergeLikeRows(segmentDocs, segmentDocs, 19);
        List<DocTerms> segC = buildMergeLikeRows(segmentDocs * 2, segmentDocs, 31);
        List<DocTerms> merged = buildMergeLikeRows(20_000, mergedDocs, 67);

        ExclusiveFpRowsProcessingApi.ProcessingOptions baseOptions = ExclusiveFpRowsProcessingApi.defaultOptions()
                .withMinSupport(3)
                .withMinItemsetSize(2)
                .withMaxItemsetSize(6)
                .withMaxCandidateCount(220_000)
                .withHotTermThresholdExclusive(4)
                .withSampleRatio(0.45d)
                .withHintBoostWeight(0);

        SegmentPremergeHints mutexOnlyHints =
                buildHintsFromSegments(baseOptions, segA, segB, segC, true, false);
        SegmentPremergeHints singleOnlyHints =
                buildHintsFromSegments(baseOptions, segA, segB, segC, false, true);
        SegmentPremergeHints bothHints =
                buildHintsFromSegments(baseOptions, segA, segB, segC, true, true);
        assertTrue(!mutexOnlyHints.mutexGroupHints.isEmpty(), "mutex hints should not be empty");
        assertTrue(!singleOnlyHints.singleTermHints.isEmpty(), "single-term hints should not be empty");
        assertTrue(!bothHints.mutexGroupHints.isEmpty() || !bothHints.singleTermHints.isEmpty(),
                "combined hints should not be empty");

        RunMetrics baseline = measure(merged, baseOptions, warmupRuns, measuredRuns);
        RunMetrics mutexOnly = measure(
                merged,
                baseOptions
                        .withPremergeMutexGroupHints(mutexOnlyHints.mutexGroupHints)
                        .withPremergeSingleTermHints(mutexOnlyHints.singleTermHints)
                        .withHintBoostWeight(8),
                warmupRuns,
                measuredRuns
        );
        RunMetrics singleOnly = measure(
                merged,
                baseOptions
                        .withPremergeMutexGroupHints(singleOnlyHints.mutexGroupHints)
                        .withPremergeSingleTermHints(singleOnlyHints.singleTermHints)
                        .withHintBoostWeight(8),
                warmupRuns,
                measuredRuns
        );
        RunMetrics both = measure(
                merged,
                baseOptions
                        .withPremergeMutexGroupHints(bothHints.mutexGroupHints)
                        .withPremergeSingleTermHints(bothHints.singleTermHints)
                        .withHintBoostWeight(8),
                warmupRuns,
                measuredRuns
        );

        System.out.println(
                "[merge-hint-perf-4way] baseline=" + baseline.medianMs
                        + "ms, mutexOnly=" + mutexOnly.medianMs
                        + "ms, singleOnly=" + singleOnly.medianMs
                        + "ms, both=" + both.medianMs
        );

        long baselineFloorMs = Math.max(20L, baseline.medianMs);
        assertTrue(mutexOnly.medianMs <= baselineFloorMs * 1.80d,
                () -> "mutex-only hints regressed too much, baseline=" + baseline.medianMs + ", mutexOnly=" + mutexOnly.medianMs);
        assertTrue(singleOnly.medianMs <= baselineFloorMs * 1.80d,
                () -> "single-only hints regressed too much, baseline=" + baseline.medianMs + ", singleOnly=" + singleOnly.medianMs);
        assertTrue(both.medianMs <= baselineFloorMs * 1.50d,
                () -> "combined hints regressed too much, baseline=" + baseline.medianMs + ", both=" + both.medianMs);
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

    private static final class SegmentPremergeHints {
        private final List<ExclusiveFpRowsProcessingApi.PremergeHint> mutexGroupHints;
        private final List<ExclusiveFpRowsProcessingApi.PremergeHint> singleTermHints;

        private SegmentPremergeHints(
                List<ExclusiveFpRowsProcessingApi.PremergeHint> mutexGroupHints,
                List<ExclusiveFpRowsProcessingApi.PremergeHint> singleTermHints
        ) {
            this.mutexGroupHints = mutexGroupHints;
            this.singleTermHints = singleTermHints;
        }
    }

    private static SegmentPremergeHints buildHintsFromSegments(
            ExclusiveFpRowsProcessingApi.ProcessingOptions options,
            List<DocTerms> segA,
            List<DocTerms> segB,
            List<DocTerms> segC,
            boolean includeMutexGroupPostings,
            boolean includeSingleTermPostings
    ) {
        List<ExclusiveFpRowsProcessingApi.PremergeHint> mutexOut = new ArrayList<>();
        List<ExclusiveFpRowsProcessingApi.PremergeHint> singleOut = new ArrayList<>();
        List<LineFileProcessingResult> results = Arrays.asList(
                ExclusiveFpRowsProcessingApi.processRows(segA, options),
                ExclusiveFpRowsProcessingApi.processRows(segB, options),
                ExclusiveFpRowsProcessingApi.processRows(segC, options)
        );
        for (int i = 0; i < results.size(); i++) {
            LineFileProcessingResult.FinalIndexData idx = results.get(i).getFinalIndexData();
            if (includeMutexGroupPostings) {
                for (SelectedGroup g : idx.getHighFreqMutexGroupPostings()) {
                    List<ByteRef> refs = new ArrayList<>();
                    for (byte[] t : g.getTerms()) {
                        refs.add(ByteRef.wrap(Arrays.copyOf(t, t.length)));
                    }
                    // 模拟重复提示：同一条 mutex hint 加入两次。
                    mutexOut.add(new ExclusiveFpRowsProcessingApi.PremergeHint(refs));
                    if (i == 0) {
                        mutexOut.add(new ExclusiveFpRowsProcessingApi.PremergeHint(refs));
                    }
                }
            }
            if (includeSingleTermPostings) {
                for (LineFileProcessingResult.HotTermDocList hot : idx.getHighFreqSingleTermPostings()) {
                    byte[] term = hot.getTerm();
                    singleOut.add(new ExclusiveFpRowsProcessingApi.PremergeHint(
                            Collections.singletonList(ByteRef.wrap(Arrays.copyOf(term, term.length)))));
                }
            }
        }
        return new SegmentPremergeHints(mutexOut, singleOut);
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

    private static String diagnoseNoImprovement(RunMetrics baseline, RunMetrics hinted, double hintCoverage) {
        if (hinted.medianMs < baseline.medianMs) {
            return "improved: premerge hints reduced search/candidate cost";
        }
        if (hintCoverage < 15.0d) {
            return "no clear gain: hint coverage on merged segment is low, most hints are stale";
        }
        if (hinted.candidateCount > baseline.candidateCount * 1.15d) {
            return "no clear gain: hints expanded candidate pool, recheck cost offsets speedup";
        }
        if (hinted.groupCount == baseline.groupCount) {
            return "no clear gain: final chosen pattern set is almost unchanged";
        }
        return "no clear gain: dataset entropy is high and hint prior is weak";
    }

    private static List<DocTerms> buildMergeLikeRows(int docIdStart, int count, int seed) {
        List<DocTerms> out = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            int docId = docIdStart + i;
            List<ByteRef> terms = new ArrayList<>();
            // 高频组合，模拟可复用的 merge 先验
            terms.add(ref("K" + (i % 20)));
            terms.add(ref("V" + ((i / 2 + seed) % 10)));
            if (i % 3 == 0) {
                terms.add(ref("ANCHOR_A"));
                terms.add(ref("ANCHOR_B"));
            }
            // 中频噪声
            terms.add(ref("N" + ((i + seed) % 37)));
            if (i % 11 == 0) {
                terms.add(ref("TAIL_X"));
            }
            out.add(new DocTerms(docId, terms));
        }
        return out;
    }

    private static ByteRef ref(String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        return new ByteRef(bytes, 0, bytes.length);
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
