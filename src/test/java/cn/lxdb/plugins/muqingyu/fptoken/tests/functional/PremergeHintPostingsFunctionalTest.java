package cn.lxdb.plugins.muqingyu.fptoken.tests.functional;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import cn.lxdb.plugins.muqingyu.fptoken.api.ExclusiveFpRowsProcessingApi;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.ByteRef;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.DocTerms;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.SelectedGroup;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.util.ByteArrayKey;
import cn.lxdb.plugins.muqingyu.fptoken.runner.result.LineFileProcessingResult;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * 功能测试：围绕 pre-merge hint 的两条路径（有提示 / 无提示）验证结果结构正确性。
 */
class PremergeHintPostingsFunctionalTest {

    @Test
    void withAndWithoutHints_fromMutexAndSinglePostings_shouldBothProduceValidThreeLayerOutput() {
        ExclusiveFpRowsProcessingApi.ProcessingOptions options = ExclusiveFpRowsProcessingApi.defaultOptions()
                .withMinSupport(3)
                .withMinItemsetSize(2)
                .withMaxItemsetSize(6)
                .withHotTermThresholdExclusive(4)
                .withSampleRatio(0.45d)
                .withHintBoostWeight(0);

        List<DocTerms> segmentA = buildRows(0, 900, 13);
        List<DocTerms> segmentB = buildRows(900, 900, 29);
        List<DocTerms> merged = buildRows(10_000, 1800, 71);

        LineFileProcessingResult segAResult = ExclusiveFpRowsProcessingApi.processRows(segmentA, options);
        LineFileProcessingResult segBResult = ExclusiveFpRowsProcessingApi.processRows(segmentB, options);

        List<ExclusiveFpRowsProcessingApi.PremergeHint> mutexHints = new ArrayList<>();
        List<ExclusiveFpRowsProcessingApi.PremergeHint> singleHints = new ArrayList<>();
        appendHintsFromPostings(segAResult.getFinalIndexData(), mutexHints, singleHints);
        appendHintsFromPostings(segBResult.getFinalIndexData(), mutexHints, singleHints);
        assertFalse(mutexHints.isEmpty() && singleHints.isEmpty(),
                "hints should be generated from historical high-frequency postings");

        LineFileProcessingResult noHint = ExclusiveFpRowsProcessingApi.processRows(merged, options);
        LineFileProcessingResult withHint = ExclusiveFpRowsProcessingApi.processRows(
                merged,
                options
                        .withPremergeMutexGroupHints(mutexHints)
                        .withPremergeSingleTermHints(singleHints)
                        .withHintBoostWeight(8)
                        .withHintValidationMode(ExclusiveFpRowsProcessingApi.HintValidationMode.FILTER_ONLY)
        );

        assertThreeLayerCoverage(merged, noHint.getFinalIndexData());
        assertThreeLayerCoverage(merged, withHint.getFinalIndexData());
        assertHintSourceTermsAppearInHints(mutexHints, singleHints);
    }

    private static void appendHintsFromPostings(
            LineFileProcessingResult.FinalIndexData finalData,
            List<ExclusiveFpRowsProcessingApi.PremergeHint> mutexOut,
            List<ExclusiveFpRowsProcessingApi.PremergeHint> singleOut
    ) {
        for (SelectedGroup g : finalData.getHighFreqMutexGroupPostings()) {
            List<ByteRef> refs = new ArrayList<>();
            for (byte[] term : g.getTerms()) {
                refs.add(ByteRef.wrap(Arrays.copyOf(term, term.length)));
            }
            mutexOut.add(new ExclusiveFpRowsProcessingApi.PremergeHint(refs));
        }
        for (LineFileProcessingResult.HotTermDocList hot : finalData.getHighFreqSingleTermPostings()) {
            singleOut.add(new ExclusiveFpRowsProcessingApi.PremergeHint(
                    Collections.singletonList(ByteRef.wrap(Arrays.copyOf(hot.getTerm(), hot.getTerm().length)))));
        }
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

    private static void assertHintSourceTermsAppearInHints(
            List<ExclusiveFpRowsProcessingApi.PremergeHint> mutexHints,
            List<ExclusiveFpRowsProcessingApi.PremergeHint> singleHints
    ) {
        assertTrue(!mutexHints.isEmpty(), "should contain hints derived from highFreqMutexGroupPostings");
        assertTrue(!singleHints.isEmpty(), "should contain hints derived from highFreqSingleTermPostings");
    }

    private static List<DocTerms> buildRows(int docIdStart, int count, int seed) {
        List<DocTerms> out = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            int docId = docIdStart + i;
            List<ByteRef> terms = new ArrayList<>();
            terms.add(ref("K" + (i % 18)));
            terms.add(ref("V" + ((i / 2 + seed) % 9)));
            if (i % 3 == 0) {
                terms.add(ref("ANCHOR_A"));
                terms.add(ref("ANCHOR_B"));
            }
            terms.add(ref("N" + ((i + seed) % 31)));
            if (i % 10 == 0) {
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
}
