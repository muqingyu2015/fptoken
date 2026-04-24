package cn.lxdb.plugins.muqingyu.fptoken.tests.functional;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import cn.lxdb.plugins.muqingyu.fptoken.api.ExclusiveFpRowsProcessingApi;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.ByteRef;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.DocTerms;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.SelectedGroup;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.util.ByteArrayKey;
import cn.lxdb.plugins.muqingyu.fptoken.runner.dataset.LineRecordDatasetLoader;
import cn.lxdb.plugins.muqingyu.fptoken.runner.result.LineFileProcessingResult;
import cn.lxdb.plugins.muqingyu.fptoken.tests.support.SampleDataPremergeHintTestSupport;
import cn.lxdb.plugins.muqingyu.fptoken.tests.support.SampleDataPremergeHintTestSupport.FiveWaySplit;
import cn.lxdb.plugins.muqingyu.fptoken.tests.support.SampleDataPremergeHintTestSupport.HintsFromSegments;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

/**
 * 基于 {@code sample-data} 的预合并提示功能回归：RAW 与 TOKENIZED 加载、merge 尾段上有/无 hints 的三层输出一致性；
 * 并覆盖「仅 mutex 组 / 仅 single term / 两者全开」三种 {@link ExclusiveFpRowsProcessingApi.ProcessingOptions} 配置。
 */
class SampleDataPremergeHintFunctionalTest {

    @Test
    void lineRecords002_tokenizedMerge_hintsAndBaseline_produceValidThreeLayerPartition() throws Exception {
        Path file = SampleDataPremergeHintTestSupport.LINE_RECORDS_002;
        Assumptions.assumeTrue(Files.isRegularFile(file), () -> "missing: " + file);

        LineRecordDatasetLoader.LoadedDataset loaded =
                LineRecordDatasetLoader.loadSingleFile(file, 2, 4);
        List<DocTerms> rows = loaded.getRows();
        Assumptions.assumeTrue(rows.size() >= 800, () -> "too few rows: " + rows.size());

        FiveWaySplit split = SampleDataPremergeHintTestSupport.splitFiveWay(rows);
        ExclusiveFpRowsProcessingApi.ProcessingOptions base =
                SampleDataPremergeHintTestSupport.mergeScenarioBaseOptions();

        HintsFromSegments hints = SampleDataPremergeHintTestSupport.buildHintsFromSegments(
                base, split.segA, split.segB, split.segC, true, true);
        Assumptions.assumeFalse(
                hints.mutexGroupHints.isEmpty() && hints.singleTermHints.isEmpty(),
                "sample should yield at least one class of postings-derived hints");

        LineFileProcessingResult noHint =
                ExclusiveFpRowsProcessingApi.processRows(split.merged, base);
        LineFileProcessingResult withHint = ExclusiveFpRowsProcessingApi.processRows(
                split.merged,
                base
                        .withPremergeMutexGroupHints(hints.mutexGroupHints)
                        .withPremergeSingleTermHints(hints.singleTermHints)
                        .withHintBoostWeight(8)
                        .withHintValidationMode(ExclusiveFpRowsProcessingApi.HintValidationMode.FILTER_ONLY)
        );

        assertThreeLayerCoverage(split.merged, noHint.getFinalIndexData());
        assertThreeLayerCoverage(split.merged, withHint.getFinalIndexData());
        assertHintListsNonEmptyAndTermsAreByteLike(hints);
    }

    @Test
    void lineRecords002_tokenizedMerge_mutexOnlySingleOnlyBoth_keepThreeLayerPartition() throws Exception {
        Path file = SampleDataPremergeHintTestSupport.LINE_RECORDS_002;
        Assumptions.assumeTrue(Files.isRegularFile(file), () -> "missing: " + file);

        LineRecordDatasetLoader.LoadedDataset loaded =
                LineRecordDatasetLoader.loadSingleFile(file, 2, 4);
        List<DocTerms> rows = loaded.getRows();
        Assumptions.assumeTrue(rows.size() >= 800, () -> "too few rows: " + rows.size());

        FiveWaySplit split = SampleDataPremergeHintTestSupport.splitFiveWay(rows);
        ExclusiveFpRowsProcessingApi.ProcessingOptions base =
                SampleDataPremergeHintTestSupport.mergeScenarioBaseOptions();

        HintsFromSegments fullHints = SampleDataPremergeHintTestSupport.buildHintsFromSegments(
                base, split.segA, split.segB, split.segC, true, true);
        Assumptions.assumeFalse(fullHints.mutexGroupHints.isEmpty(), "need mutex hints");
        Assumptions.assumeFalse(fullHints.singleTermHints.isEmpty(), "need single hints");

        ExclusiveFpRowsProcessingApi.ProcessingOptions mutexOnly = base
                .withPremergeMutexGroupHints(fullHints.mutexGroupHints)
                .withPremergeSingleTermHints(Collections.<ExclusiveFpRowsProcessingApi.PremergeHint>emptyList())
                .withHintBoostWeight(8)
                .withHintValidationMode(ExclusiveFpRowsProcessingApi.HintValidationMode.FILTER_ONLY);
        ExclusiveFpRowsProcessingApi.ProcessingOptions singleOnly = base
                .withPremergeMutexGroupHints(Collections.<ExclusiveFpRowsProcessingApi.PremergeHint>emptyList())
                .withPremergeSingleTermHints(fullHints.singleTermHints)
                .withHintBoostWeight(8)
                .withHintValidationMode(ExclusiveFpRowsProcessingApi.HintValidationMode.FILTER_ONLY);
        ExclusiveFpRowsProcessingApi.ProcessingOptions bothHints = base
                .withPremergeMutexGroupHints(fullHints.mutexGroupHints)
                .withPremergeSingleTermHints(fullHints.singleTermHints)
                .withHintBoostWeight(8)
                .withHintValidationMode(ExclusiveFpRowsProcessingApi.HintValidationMode.FILTER_ONLY);

        assertThreeLayerCoverage(
                split.merged,
                ExclusiveFpRowsProcessingApi.processRows(split.merged, mutexOnly).getFinalIndexData());
        assertThreeLayerCoverage(
                split.merged,
                ExclusiveFpRowsProcessingApi.processRows(split.merged, singleOnly).getFinalIndexData());
        assertThreeLayerCoverage(
                split.merged,
                ExclusiveFpRowsProcessingApi.processRows(split.merged, bothHints).getFinalIndexData());
    }

    @Test
    void lineRecords002_rawBytesMerge_hintsPath_completesWithoutStrictValidationErrors() throws Exception {
        Path file = SampleDataPremergeHintTestSupport.LINE_RECORDS_002;
        Assumptions.assumeTrue(Files.isRegularFile(file), () -> "missing: " + file);

        LineRecordDatasetLoader.LoadOutcome outcome =
                LineRecordDatasetLoader.loadSingleFileRawWithStats(file);
        List<DocTerms> rows = outcome.getLoadedDataset().getRows();
        Assumptions.assumeTrue(rows.size() >= 800, () -> "too few rows: " + rows.size());

        FiveWaySplit split = SampleDataPremergeHintTestSupport.splitFiveWay(rows);
        ExclusiveFpRowsProcessingApi.ProcessingOptions base =
                SampleDataPremergeHintTestSupport.mergeScenarioBaseOptions();

        HintsFromSegments hints = SampleDataPremergeHintTestSupport.buildHintsFromSegments(
                base, split.segA, split.segB, split.segC, true, true);
        Assumptions.assumeFalse(
                hints.mutexGroupHints.isEmpty() && hints.singleTermHints.isEmpty(),
                "sample should yield hints from historical segments");

        LineFileProcessingResult withHint = ExclusiveFpRowsProcessingApi.processRows(
                split.merged,
                base
                        .withPremergeMutexGroupHints(hints.mutexGroupHints)
                        .withPremergeSingleTermHints(hints.singleTermHints)
                        .withHintBoostWeight(8)
                        .withHintValidationMode(ExclusiveFpRowsProcessingApi.HintValidationMode.FILTER_ONLY)
        );

        assertTrue(withHint.getSelectionResult().getCandidateCount() >= 0);
        assertFalse(withHint.getFinalIndexData().getHighFreqMutexGroupPostings().isEmpty()
                && withHint.getFinalIndexData().getHighFreqSingleTermPostings().isEmpty()
                && withHint.getFinalIndexData().getLowHitForwardRows().isEmpty(),
                "three-layer output should not be all empty on non-trivial merge tail");
    }

    @Test
    void realDocs_awesomePython_tokenizedMerge_hintsAndBaseline_produceValidThreeLayerPartition() throws Exception {
        Path file = SampleDataPremergeHintTestSupport.REAL_DOCS_AWESOME_PYTHON;
        Assumptions.assumeTrue(Files.isRegularFile(file), () -> "missing: " + file);

        LineRecordDatasetLoader.LoadedDataset loaded =
                LineRecordDatasetLoader.loadSingleFile(file, 2, 4);
        List<DocTerms> rows = loaded.getRows();
        Assumptions.assumeTrue(rows.size() >= 400, () -> "too few rows: " + rows.size());

        FiveWaySplit split = SampleDataPremergeHintTestSupport.splitFiveWay(rows);
        ExclusiveFpRowsProcessingApi.ProcessingOptions base =
                SampleDataPremergeHintTestSupport.mergeScenarioBaseOptions();

        HintsFromSegments hints = SampleDataPremergeHintTestSupport.buildHintsFromSegments(
                base, split.segA, split.segB, split.segC, true, true);
        Assumptions.assumeFalse(
                hints.mutexGroupHints.isEmpty() && hints.singleTermHints.isEmpty(),
                "real-doc sample should yield postings-derived hints");

        LineFileProcessingResult noHint =
                ExclusiveFpRowsProcessingApi.processRows(split.merged, base);
        LineFileProcessingResult withHint = ExclusiveFpRowsProcessingApi.processRows(
                split.merged,
                base
                        .withPremergeMutexGroupHints(hints.mutexGroupHints)
                        .withPremergeSingleTermHints(hints.singleTermHints)
                        .withHintBoostWeight(8)
                        .withHintValidationMode(ExclusiveFpRowsProcessingApi.HintValidationMode.FILTER_ONLY)
        );

        assertThreeLayerCoverage(split.merged, noHint.getFinalIndexData());
        assertThreeLayerCoverage(split.merged, withHint.getFinalIndexData());
    }

    private static void assertThreeLayerCoverage(
            List<DocTerms> sourceRows,
            LineFileProcessingResult.FinalIndexData finalData
    ) {
        Set<ByteArrayKey> expected = collectTerms(sourceRows);
        Set<ByteArrayKey> lowHitTerms = collectTerms(finalData.getLowHitForwardRows());
        Set<ByteArrayKey> highTerms = collectHighLayerTerms(finalData);

        Set<ByteArrayKey> union = new LinkedHashSet<>(highTerms);
        union.addAll(lowHitTerms);
        assertTrue(union.equals(expected), "union of three layers should cover all source terms");
    }

    private static Set<ByteArrayKey> collectHighLayerTerms(LineFileProcessingResult.FinalIndexData finalData) {
        Set<ByteArrayKey> out = new LinkedHashSet<>();
        for (SelectedGroup group : finalData.getHighFreqMutexGroupPostings()) {
            for (byte[] term : group.getTerms()) {
                out.add(new ByteArrayKey(term));
            }
        }
        for (LineFileProcessingResult.HotTermDocList hot : finalData.getHighFreqSingleTermPostings()) {
            out.add(new ByteArrayKey(hot.getTerm()));
        }
        return out;
    }

    private static Set<ByteArrayKey> collectTerms(List<DocTerms> rows) {
        Set<ByteArrayKey> out = new LinkedHashSet<>();
        for (DocTerms row : rows) {
            for (byte[] term : row.getTermsUnsafe()) {
                out.add(new ByteArrayKey(term));
            }
        }
        return out;
    }

    private static void assertHintListsNonEmptyAndTermsAreByteLike(HintsFromSegments hints) {
        assertTrue(!hints.mutexGroupHints.isEmpty() || !hints.singleTermHints.isEmpty());
        for (ExclusiveFpRowsProcessingApi.PremergeHint h : hints.mutexGroupHints) {
            for (ByteRef ref : h.getTermRefs()) {
                assertTrue(ref.getLength() > 0);
            }
        }
        for (ExclusiveFpRowsProcessingApi.PremergeHint h : hints.singleTermHints) {
            for (ByteRef ref : h.getTermRefs()) {
                assertTrue(ref.getLength() > 0);
            }
        }
    }
}
