package cn.lxdb.plugins.muqingyu.fptoken.tests.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import cn.lxdb.plugins.muqingyu.fptoken.ExclusiveFrequentItemsetSelector;
import cn.lxdb.plugins.muqingyu.fptoken.config.SelectorConfig;
import cn.lxdb.plugins.muqingyu.fptoken.index.TermTidsetIndex;
import cn.lxdb.plugins.muqingyu.fptoken.miner.BeamFrequentItemsetMiner;
import cn.lxdb.plugins.muqingyu.fptoken.model.CandidateItemset;
import cn.lxdb.plugins.muqingyu.fptoken.model.ExclusiveSelectionResult;
import cn.lxdb.plugins.muqingyu.fptoken.picker.TwoPhaseExclusiveItemsetPicker;
import cn.lxdb.plugins.muqingyu.fptoken.tests.ByteArrayTestSupport;
import cn.lxdb.plugins.muqingyu.fptoken.util.ByteArrayUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;

class SelectorPipelineIntegrationTest {

    private static final int DEFAULT_MAX_FREQUENT_TERM_COUNT = 4096;
    private static final int DEFAULT_MAX_BRANCHING_FACTOR = 24;
    private static final int DEFAULT_BEAM_WIDTH = 48;
    private static final int DEFAULT_MAX_SWAP_TRIALS = 128;

    @Test
    void facadeAndManualPipeline_matchOnStatsAndSelectionFingerprint() {
        List<cn.lxdb.plugins.muqingyu.fptoken.model.DocTerms> rows = new ArrayList<>();
        byte[] a = ByteArrayTestSupport.hex("AA");
        byte[] b = ByteArrayTestSupport.hex("BB");
        byte[] c = ByteArrayTestSupport.hex("CC");
        for (int i = 0; i < 50; i++) {
            rows.add(ByteArrayTestSupport.doc(i, a, b, c));
        }

        int minSupport = 20;
        int minItemsetSize = 2;
        int maxItemsetSize = 6;
        int maxCandidateCount = 20_000;

        ExclusiveSelectionResult facade = ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(
                rows, minSupport, minItemsetSize, maxItemsetSize, maxCandidateCount);

        SelectorConfig config = new SelectorConfig(minSupport, minItemsetSize, maxItemsetSize, maxCandidateCount);
        TermTidsetIndex index = TermTidsetIndex.build(rows);
        BeamFrequentItemsetMiner miner = new BeamFrequentItemsetMiner();
        var mining = miner.mineWithStats(
                index.getTidsetsByTermId(),
                config,
                DEFAULT_MAX_FREQUENT_TERM_COUNT,
                DEFAULT_MAX_BRANCHING_FACTOR,
                DEFAULT_BEAM_WIDTH);
        TwoPhaseExclusiveItemsetPicker picker = new TwoPhaseExclusiveItemsetPicker();
        List<CandidateItemset> selected = picker.pick(
                mining.getCandidates(),
                index.getIdToTerm().size(),
                DEFAULT_MAX_SWAP_TRIALS);

        assertEquals(mining.getFrequentTermCount(), facade.getFrequentTermCount());
        assertEquals(mining.getGeneratedCandidateCount(), facade.getCandidateCount());
        assertEquals(mining.getIntersectionCount(), facade.getIntersectionCount());
        assertEquals(mining.isTruncatedByCandidateLimit(), facade.isTruncatedByCandidateLimit());
        assertEquals(toCandidateFingerprint(selected, index.getIdToTerm()),
                ByteArrayTestSupport.groupsFingerprint(facade.getGroups()));
    }

    @Test
    void integrationResult_keepsMutualExclusionAndLengthBounds() {
        List<cn.lxdb.plugins.muqingyu.fptoken.model.DocTerms> rows = ByteArrayTestSupport.pcapLikeBatch(30, 64, 16, 8);
        ExclusiveSelectionResult r = ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(
                rows, 5, 2, 5, 50_000);

        assertTrue(ByteArrayTestSupport.pairwiseTermsDisjoint(r.getGroups()));
        assertTrue(ByteArrayTestSupport.allGroupsTermCountInRange(r.getGroups(), 2, 5));
        assertTrue(r.getCandidateCount() <= r.getMaxCandidateCount());
    }

    @Test
    void tinyCandidateLimit_matchesManualPipelineTruncationSignal() {
        List<cn.lxdb.plugins.muqingyu.fptoken.model.DocTerms> rows = new ArrayList<>();
        byte[] a = ByteArrayTestSupport.hex("AA");
        byte[] b = ByteArrayTestSupport.hex("BB");
        byte[] c = ByteArrayTestSupport.hex("CC");
        for (int i = 0; i < 40; i++) {
            rows.add(ByteArrayTestSupport.doc(i, a, b, c));
        }

        int minSupport = 5;
        int minItemsetSize = 1;
        int maxItemsetSize = 5;
        int maxCandidateCount = 1;
        ExclusiveSelectionResult facade = ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(
                rows, minSupport, minItemsetSize, maxItemsetSize, maxCandidateCount);

        SelectorConfig config = new SelectorConfig(minSupport, minItemsetSize, maxItemsetSize, maxCandidateCount);
        TermTidsetIndex index = TermTidsetIndex.build(rows);
        var mining = new BeamFrequentItemsetMiner().mineWithStats(
                index.getTidsetsByTermId(),
                config,
                DEFAULT_MAX_FREQUENT_TERM_COUNT,
                DEFAULT_MAX_BRANCHING_FACTOR,
                DEFAULT_BEAM_WIDTH);
        assertEquals(mining.isTruncatedByCandidateLimit(), facade.isTruncatedByCandidateLimit());
        assertEquals(mining.getGeneratedCandidateCount(), facade.getCandidateCount());
        assertTrue(facade.getCandidateCount() <= 1);
    }

    @Test
    void sparseDocIds_facadeAndManualPipeline_keepSameSelectionFingerprint() {
        List<cn.lxdb.plugins.muqingyu.fptoken.model.DocTerms> rows = new ArrayList<>();
        byte[] term = ByteArrayTestSupport.hex("DEAD");
        rows.add(ByteArrayTestSupport.doc(5, term));
        rows.add(ByteArrayTestSupport.doc(13, term));
        rows.add(ByteArrayTestSupport.doc(21, term));

        int minSupport = 2;
        int minItemsetSize = 1;
        int maxItemsetSize = 3;
        int maxCandidateCount = 2000;
        ExclusiveSelectionResult facade = ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(
                rows, minSupport, minItemsetSize, maxItemsetSize, maxCandidateCount);

        SelectorConfig config = new SelectorConfig(minSupport, minItemsetSize, maxItemsetSize, maxCandidateCount);
        TermTidsetIndex index = TermTidsetIndex.build(rows);
        var mining = new BeamFrequentItemsetMiner().mineWithStats(
                index.getTidsetsByTermId(),
                config,
                DEFAULT_MAX_FREQUENT_TERM_COUNT,
                DEFAULT_MAX_BRANCHING_FACTOR,
                DEFAULT_BEAM_WIDTH);
        List<CandidateItemset> selected = new TwoPhaseExclusiveItemsetPicker().pick(
                mining.getCandidates(),
                index.getIdToTerm().size(),
                DEFAULT_MAX_SWAP_TRIALS);

        assertEquals(toCandidateFingerprint(selected, index.getIdToTerm()),
                ByteArrayTestSupport.groupsFingerprint(facade.getGroups()));
    }

    @Test
    void negativeDocId_consistentFailureFromFacadeAndManualIndexBuild() {
        List<cn.lxdb.plugins.muqingyu.fptoken.model.DocTerms> rows = new ArrayList<>();
        rows.add(ByteArrayTestSupport.doc(-1, ByteArrayTestSupport.hex("AA")));
        assertThrows(IllegalArgumentException.class,
                () -> ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(rows, 1, 1, 3, 1000));
        assertThrows(IllegalArgumentException.class, () -> TermTidsetIndex.build(rows));
    }

    private static String toCandidateFingerprint(List<CandidateItemset> selected, List<byte[]> idToTerm) {
        List<String> parts = new ArrayList<>();
        for (CandidateItemset c : selected) {
            List<String> termsHex = new ArrayList<>();
            for (int termId : c.getTermIds()) {
                termsHex.add(ByteArrayUtils.toHex(idToTerm.get(termId)));
            }
            Collections.sort(termsHex);
            parts.add(String.join(",", termsHex) + ":" + c.getSupport());
        }
        Collections.sort(parts);
        return String.join("|", parts);
    }
}
