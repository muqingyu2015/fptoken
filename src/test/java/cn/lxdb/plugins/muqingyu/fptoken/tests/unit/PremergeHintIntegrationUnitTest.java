package cn.lxdb.plugins.muqingyu.fptoken.tests.unit;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import cn.lxdb.plugins.muqingyu.fptoken.ExclusiveFrequentItemsetSelector;
import cn.lxdb.plugins.muqingyu.fptoken.api.ExclusiveFpRowsProcessingApi;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.ByteRef;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.util.ByteArrayKey;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.DocTerms;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.ExclusiveSelectionResult;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.SelectedGroup;
import cn.lxdb.plugins.muqingyu.fptoken.runner.result.LineFileProcessingResult;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.lang.reflect.Method;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.Test;

class PremergeHintIntegrationUnitTest {

    @Test
    void selector_withHintBoost_shouldPreferHintedConflictCandidate() {
        List<DocTerms> rows = new ArrayList<>();
        rows.add(doc(0, "a", "b", "c"));
        rows.add(doc(1, "a", "b", "c"));
        rows.add(doc(2, "a", "b", "c"));

        List<ByteRef> hintedTerms = Arrays.asList(ref("a"), ref("c"));
        List<ExclusiveFrequentItemsetSelector.PremergeHintCandidate> hints =
                Collections.singletonList(new ExclusiveFrequentItemsetSelector.PremergeHintCandidate(hintedTerms));

        ExclusiveSelectionResult result = ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(
                rows, 2, 2, 2, 1000, hints, 10);

        assertTrue(containsGroup(result.getGroups(), "a", "c"));
    }

    @Test
    void processRows_strictHintValidation_shouldRejectEmptyTermRefs() {
        List<DocTerms> rows = new ArrayList<>();
        rows.add(doc(0, "a", "b"));
        rows.add(doc(1, "a", "c"));

        ExclusiveFpRowsProcessingApi.PremergeHint hint =
                new ExclusiveFpRowsProcessingApi.PremergeHint(Collections.emptyList());
        ExclusiveFpRowsProcessingApi.ProcessingOptions options =
                ExclusiveFpRowsProcessingApi.defaultOptions()
                        .withMinSupport(1)
                        .withMinItemsetSize(2)
                        .withPremergeMutexGroupHints(Collections.singletonList(hint))
                        .withHintValidationMode(ExclusiveFpRowsProcessingApi.HintValidationMode.STRICT);

        assertThrows(IllegalArgumentException.class, () ->
                ExclusiveFpRowsProcessingApi.processRows(rows, options));
    }

    @Test
    void processRows_withAndWithoutPremergePostingsHints_shouldBothKeepTermCoverage() {
        ExclusiveFpRowsProcessingApi.ProcessingOptions options = ExclusiveFpRowsProcessingApi.defaultOptions()
                .withMinSupport(2)
                .withMinItemsetSize(2)
                .withMaxItemsetSize(5)
                .withMaxCandidateCount(100_000)
                .withHotTermThresholdExclusive(2)
                .withSampleRatio(0.5d)
                .withHintBoostWeight(0);

        List<DocTerms> history = new ArrayList<>();
        for (int i = 0; i < 120; i++) {
            history.add(doc(i, "A", "B", "K" + (i % 5), "N" + (i % 7)));
        }
        LineFileProcessingResult historyResult = ExclusiveFpRowsProcessingApi.processRows(history, options);
        PremergeHintSplit hints = buildPremergeHintSplit(historyResult.getFinalIndexData());
        assertFalse(hints.mutexGroupHints.isEmpty() || hints.singleTermHints.isEmpty());

        List<DocTerms> merged = new ArrayList<>();
        for (int i = 0; i < 180; i++) {
            merged.add(doc(10_000 + i, "A", "B", "K" + (i % 5), "M" + (i % 9)));
        }

        LineFileProcessingResult noHint = ExclusiveFpRowsProcessingApi.processRows(merged, options);
        LineFileProcessingResult withHint = ExclusiveFpRowsProcessingApi.processRows(
                merged,
                options
                        .withPremergeMutexGroupHints(hints.mutexGroupHints)
                        .withPremergeSingleTermHints(hints.singleTermHints)
                        .withHintBoostWeight(8)
                        .withHintValidationMode(ExclusiveFpRowsProcessingApi.HintValidationMode.FILTER_ONLY)
        );

        assertTermCoverageInvariant(merged, noHint.getFinalIndexData());
        assertTermCoverageInvariant(merged, withHint.getFinalIndexData());
        assertTrue(hasHintTermsInHighFreqLayers(withHint.getFinalIndexData(), hints));
    }

    @Test
    void processRows_shardedHints_shouldStillSurfaceHighFreqTerms() {
        List<DocTerms> rows = new ArrayList<>();
        for (int i = 0; i < 160; i++) {
            rows.add(doc(
                    i,
                    "A",
                    "B",
                    "CORE" + (i % 4),
                    "NOISE" + (i % 17)
            ));
        }

        // 模拟上游乱序 + 分片提示：A/B 拆成多条 premerge hints
        List<ExclusiveFpRowsProcessingApi.PremergeHint> mutexHints = Collections.singletonList(
                new ExclusiveFpRowsProcessingApi.PremergeHint(Arrays.asList(ref("B"), ref("A"))));
        List<ExclusiveFpRowsProcessingApi.PremergeHint> singleHints = Arrays.asList(
                new ExclusiveFpRowsProcessingApi.PremergeHint(Collections.singletonList(ref("A"))),
                new ExclusiveFpRowsProcessingApi.PremergeHint(Collections.singletonList(ref("B"))));

        ExclusiveFpRowsProcessingApi.ProcessingOptions options = ExclusiveFpRowsProcessingApi.defaultOptions()
                .withMinSupport(3)
                .withMinItemsetSize(2)
                .withMaxItemsetSize(5)
                .withMaxCandidateCount(120_000)
                .withSampleRatio(0.45d)
                .withPremergeMutexGroupHints(mutexHints)
                .withPremergeSingleTermHints(singleHints)
                .withHintBoostWeight(8)
                .withHintValidationMode(ExclusiveFpRowsProcessingApi.HintValidationMode.FILTER_ONLY);

        LineFileProcessingResult result = ExclusiveFpRowsProcessingApi.processRows(rows, options);
        assertTrue(hasHighFreqTerm(result.getFinalIndexData(), "A"));
        assertTrue(hasHighFreqTerm(result.getFinalIndexData(), "B"));
    }

    @Test
    void processRows_staleOneOffHint_shouldBeFilteredByCurrentSegmentSupportGate() {
        List<DocTerms> rows = new ArrayList<>();
        rows.add(doc(0, "A", "B", "D"));
        rows.add(doc(1, "A", "B", "D"));
        rows.add(doc(2, "A", "D"));
        rows.add(doc(3, "A", "D"));

        ExclusiveFpRowsProcessingApi.PremergeHint staleHint =
                new ExclusiveFpRowsProcessingApi.PremergeHint(Arrays.asList(ref("A"), ref("B")));
        ExclusiveFpRowsProcessingApi.ProcessingOptions options = ExclusiveFpRowsProcessingApi.defaultOptions()
                .withMinSupport(2)
                .withMinItemsetSize(2)
                .withMaxItemsetSize(2)
                .withMaxCandidateCount(1000)
                .withPremergeMutexGroupHints(Collections.singletonList(staleHint))
                .withHintBoostWeight(20)
                .withHintValidationMode(ExclusiveFpRowsProcessingApi.HintValidationMode.FILTER_ONLY);

        LineFileProcessingResult result = ExclusiveFpRowsProcessingApi.processRows(rows, options);
        assertTrue(hasHighFreqTerm(result.getFinalIndexData(), "D"));
        assertFalse(hasMutexGroup(result.getFinalIndexData(), "A", "B"));
    }

    @Test
    void buildSelectorPremergeHints_conflictedMutexHints_shouldResolveByQualityNotUnion() throws Exception {
        List<DocTerms> rows = new ArrayList<>();
        rows.add(doc(0, "A", "B"));
        rows.add(doc(1, "A", "B"));
        rows.add(doc(2, "A", "B"));
        rows.add(doc(3, "A", "B"));
        rows.add(doc(4, "A", "C"));
        rows.add(doc(5, "A", "C"));

        ExclusiveFpRowsProcessingApi.ProcessingOptions options = ExclusiveFpRowsProcessingApi.defaultOptions()
                .withMinSupport(1)
                .withHintValidationMode(ExclusiveFpRowsProcessingApi.HintValidationMode.FILTER_ONLY)
                .withPremergeMutexGroupHints(Arrays.asList(
                        new ExclusiveFpRowsProcessingApi.PremergeHint(Arrays.asList(ref("A"), ref("B"))),
                        new ExclusiveFpRowsProcessingApi.PremergeHint(Arrays.asList(ref("A"), ref("B"))),
                        new ExclusiveFpRowsProcessingApi.PremergeHint(Arrays.asList(ref("A"), ref("C")))
                ));

        Method m = ExclusiveFpRowsProcessingApi.class.getDeclaredMethod(
                "buildSelectorPremergeHints",
                ExclusiveFpRowsProcessingApi.ProcessingOptions.class,
                List.class,
                int.class
        );
        m.setAccessible(true);
        @SuppressWarnings("unchecked")
        List<ExclusiveFrequentItemsetSelector.PremergeHintCandidate> resolved =
                (List<ExclusiveFrequentItemsetSelector.PremergeHintCandidate>) m.invoke(null, options, rows, 1);

        assertEquals(1, resolved.size());
        assertTrue(hasTerms(resolved.get(0), "A", "B"));
        assertEquals(5, resolved.get(0).getQualityScore());
    }

    @Test
    void buildSelectorPremergeHints_whenMutexExists_shouldTightenSingleHintAdmission() throws Exception {
        List<DocTerms> rows = new ArrayList<>();
        rows.add(doc(0, "A", "B", "S"));
        rows.add(doc(1, "A", "B", "S"));
        rows.add(doc(2, "A", "B"));
        rows.add(doc(3, "A", "B"));

        ExclusiveFpRowsProcessingApi.ProcessingOptions options = ExclusiveFpRowsProcessingApi.defaultOptions()
                .withMinSupport(2)
                .withHintValidationMode(ExclusiveFpRowsProcessingApi.HintValidationMode.FILTER_ONLY)
                .withPremergeMutexGroupHints(Collections.singletonList(
                        new ExclusiveFpRowsProcessingApi.PremergeHint(Arrays.asList(ref("A"), ref("B")))))
                .withPremergeSingleTermHints(Collections.singletonList(
                        new ExclusiveFpRowsProcessingApi.PremergeHint(Collections.singletonList(ref("S")))));

        Method m = ExclusiveFpRowsProcessingApi.class.getDeclaredMethod(
                "buildSelectorPremergeHints",
                ExclusiveFpRowsProcessingApi.ProcessingOptions.class,
                List.class,
                int.class
        );
        m.setAccessible(true);
        @SuppressWarnings("unchecked")
        List<ExclusiveFrequentItemsetSelector.PremergeHintCandidate> resolved =
                (List<ExclusiveFrequentItemsetSelector.PremergeHintCandidate>) m.invoke(null, options, rows, 2);

        assertEquals(1, resolved.size());
        assertTrue(hasTerms(resolved.get(0), "A", "B"));
    }

    @Test
    void processRows_concurrentSharedHints_shouldHitLockFreeAggregateCache() throws Exception {
        ExclusiveFpRowsProcessingApi.IntermediateSteps.clearPremergeHintAggregateCache();
        List<DocTerms> rows = new ArrayList<>();
        for (int i = 0; i < 120; i++) {
            rows.add(doc(i, "A", "B", "S" + (i % 5)));
        }
        List<ExclusiveFpRowsProcessingApi.PremergeHint> mutexHints = Arrays.asList(
                new ExclusiveFpRowsProcessingApi.PremergeHint(Arrays.asList(ref("A"), ref("B"))),
                new ExclusiveFpRowsProcessingApi.PremergeHint(Arrays.asList(ref("A"), ref("S1")))
        );
        List<ExclusiveFpRowsProcessingApi.PremergeHint> singleHints = Arrays.asList(
                new ExclusiveFpRowsProcessingApi.PremergeHint(Collections.singletonList(ref("A"))),
                new ExclusiveFpRowsProcessingApi.PremergeHint(Collections.singletonList(ref("B")))
        );
        ExclusiveFpRowsProcessingApi.ProcessingOptions options = ExclusiveFpRowsProcessingApi.defaultOptions()
                .withMinSupport(2)
                .withMinItemsetSize(2)
                .withPremergeMutexGroupHints(mutexHints)
                .withPremergeSingleTermHints(singleHints)
                .withHintBoostWeight(8)
                .withHintValidationMode(ExclusiveFpRowsProcessingApi.HintValidationMode.FILTER_ONLY);

        int workers = 8;
        ExecutorService pool = Executors.newFixedThreadPool(workers);
        try {
            List<Callable<LineFileProcessingResult>> tasks = new ArrayList<>();
            for (int i = 0; i < workers * 3; i++) {
                tasks.add(() -> ExclusiveFpRowsProcessingApi.processRows(rows, options));
            }
            List<Future<LineFileProcessingResult>> futures = pool.invokeAll(tasks);
            for (int i = 0; i < futures.size(); i++) {
                assertTrue(futures.get(i).get().getSelectionResult().getCandidateCount() >= 0);
            }
        } finally {
            pool.shutdownNow();
        }
        long hits = ExclusiveFpRowsProcessingApi.IntermediateSteps.premergeHintAggregateCacheHitCount();
        long misses = ExclusiveFpRowsProcessingApi.IntermediateSteps.premergeHintAggregateCacheMissCount();
        assertTrue(misses >= 1L);
        assertTrue(hits >= 1L, "concurrent shared hints should reuse aggregate cache");
    }


    private static boolean containsGroup(List<SelectedGroup> groups, String... terms) {
        byte[][] expect = new byte[terms.length][];
        for (int i = 0; i < terms.length; i++) {
            expect[i] = terms[i].getBytes();
        }
        for (SelectedGroup group : groups) {
            List<byte[]> got = group.getTerms();
            if (got.size() != expect.length) {
                continue;
            }
            boolean all = true;
            for (int i = 0; i < expect.length; i++) {
                if (!Arrays.equals(expect[i], got.get(i))) {
                    all = false;
                    break;
                }
            }
            if (all) {
                return true;
            }
        }
        return false;
    }

    private static DocTerms doc(int docId, String... terms) {
        List<ByteRef> refs = new ArrayList<>();
        for (int i = 0; i < terms.length; i++) {
            refs.add(ref(terms[i]));
        }
        return new DocTerms(docId, refs);
    }

    private static ByteRef ref(String term) {
        byte[] bytes = term.getBytes();
        return new ByteRef(bytes, 0, bytes.length);
    }

    private static final class PremergeHintSplit {
        private final List<ExclusiveFpRowsProcessingApi.PremergeHint> mutexGroupHints;
        private final List<ExclusiveFpRowsProcessingApi.PremergeHint> singleTermHints;

        private PremergeHintSplit(
                List<ExclusiveFpRowsProcessingApi.PremergeHint> mutexGroupHints,
                List<ExclusiveFpRowsProcessingApi.PremergeHint> singleTermHints
        ) {
            this.mutexGroupHints = mutexGroupHints;
            this.singleTermHints = singleTermHints;
        }
    }

    private static PremergeHintSplit buildPremergeHintSplit(LineFileProcessingResult.FinalIndexData finalData) {
        List<ExclusiveFpRowsProcessingApi.PremergeHint> mutex = new ArrayList<>();
        for (SelectedGroup group : finalData.getHighFreqMutexGroupPostings()) {
            List<ByteRef> refs = new ArrayList<>();
            for (byte[] term : group.getTerms()) {
                refs.add(ByteRef.wrap(Arrays.copyOf(term, term.length)));
            }
            mutex.add(new ExclusiveFpRowsProcessingApi.PremergeHint(refs));
        }
        List<ExclusiveFpRowsProcessingApi.PremergeHint> single = new ArrayList<>();
        for (LineFileProcessingResult.HotTermDocList hot : finalData.getHighFreqSingleTermPostings()) {
            single.add(new ExclusiveFpRowsProcessingApi.PremergeHint(
                    Collections.singletonList(ByteRef.wrap(Arrays.copyOf(hot.getTerm(), hot.getTerm().length)))));
        }
        return new PremergeHintSplit(mutex, single);
    }

    private static void assertTermCoverageInvariant(
            List<DocTerms> sourceRows,
            LineFileProcessingResult.FinalIndexData finalData
    ) {
        Set<ByteArrayKey> expected = collectTermsFromRows(sourceRows);
        Set<ByteArrayKey> actual = new LinkedHashSet<>();
        for (SelectedGroup group : finalData.getHighFreqMutexGroupPostings()) {
            for (byte[] term : group.getTerms()) {
                actual.add(new ByteArrayKey(term));
            }
        }
        for (LineFileProcessingResult.HotTermDocList hot : finalData.getHighFreqSingleTermPostings()) {
            actual.add(new ByteArrayKey(hot.getTerm()));
        }
        for (DocTerms row : finalData.getLowHitForwardRows()) {
            for (byte[] term : row.getTermsUnsafe()) {
                actual.add(new ByteArrayKey(term));
            }
        }
        assertTrue(expected.equals(actual));
    }

    private static boolean hasHintTermsInHighFreqLayers(
            LineFileProcessingResult.FinalIndexData finalData,
            PremergeHintSplit hints
    ) {
        Set<ByteArrayKey> highTerms = new LinkedHashSet<>();
        for (SelectedGroup group : finalData.getHighFreqMutexGroupPostings()) {
            for (byte[] term : group.getTerms()) {
                highTerms.add(new ByteArrayKey(term));
            }
        }
        for (LineFileProcessingResult.HotTermDocList hot : finalData.getHighFreqSingleTermPostings()) {
            highTerms.add(new ByteArrayKey(hot.getTerm()));
        }
        for (ExclusiveFpRowsProcessingApi.PremergeHint hint : hints.mutexGroupHints) {
            for (ByteRef ref : hint.getTermRefs()) {
                if (highTerms.contains(new ByteArrayKey(ref.copyBytes()))) {
                    return true;
                }
            }
        }
        for (ExclusiveFpRowsProcessingApi.PremergeHint hint : hints.singleTermHints) {
            for (ByteRef ref : hint.getTermRefs()) {
                if (highTerms.contains(new ByteArrayKey(ref.copyBytes()))) {
                    return true;
                }
            }
        }
        return false;
    }

    private static Set<ByteArrayKey> collectTermsFromRows(List<DocTerms> rows) {
        Set<ByteArrayKey> out = new LinkedHashSet<>();
        for (DocTerms row : rows) {
            for (byte[] term : row.getTermsUnsafe()) {
                out.add(new ByteArrayKey(term));
            }
        }
        return out;
    }

    private static boolean hasHighFreqTerm(
            LineFileProcessingResult.FinalIndexData finalData,
            String term
    ) {
        byte[] needle = term.getBytes();
        for (SelectedGroup group : finalData.getHighFreqMutexGroupPostings()) {
            for (byte[] item : group.getTerms()) {
                if (Arrays.equals(needle, item)) {
                    return true;
                }
            }
        }
        for (LineFileProcessingResult.HotTermDocList hot : finalData.getHighFreqSingleTermPostings()) {
            if (Arrays.equals(needle, hot.getTerm())) {
                return true;
            }
        }
        return false;
    }

    private static boolean hasMutexGroup(
            LineFileProcessingResult.FinalIndexData finalData,
            String... terms
    ) {
        return containsGroup(finalData.getHighFreqMutexGroupPostings(), terms);
    }

    private static boolean hasTerms(
            ExclusiveFrequentItemsetSelector.PremergeHintCandidate candidate,
            String... terms
    ) {
        List<ByteRef> refs = candidate.getTermRefs();
        if (refs.size() != terms.length) {
            return false;
        }
        for (int i = 0; i < terms.length; i++) {
            if (!Arrays.equals(terms[i].getBytes(), refs.get(i).copyBytes())) {
                return false;
            }
        }
        return true;
    }

}
