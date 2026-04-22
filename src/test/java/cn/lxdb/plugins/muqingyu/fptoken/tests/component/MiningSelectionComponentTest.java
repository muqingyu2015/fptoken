package cn.lxdb.plugins.muqingyu.fptoken.tests.component;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.config.SelectorConfig;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.index.TermTidsetIndex;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.miner.BeamFrequentItemsetMiner;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.CandidateItemset;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.picker.TwoPhaseExclusiveItemsetPicker;
import cn.lxdb.plugins.muqingyu.fptoken.tests.ByteArrayTestSupport;
import cn.lxdb.plugins.muqingyu.fptoken.tests.CandidateFixture;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import org.junit.jupiter.api.Test;

class MiningSelectionComponentTest {

    @Test
    void minerAndPicker_pipelineProducesMutexSubset() {
        List<cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.DocTerms> rows = new ArrayList<>();
        byte[] a = ByteArrayTestSupport.hex("AA");
        byte[] b = ByteArrayTestSupport.hex("BB");
        byte[] c = ByteArrayTestSupport.hex("CC");
        for (int i = 0; i < 60; i++) {
            rows.add(ByteArrayTestSupport.doc(i, a, b, c));
        }

        SelectorConfig cfg = new SelectorConfig(15, 2, 5, 10_000);
        TermTidsetIndex index = TermTidsetIndex.build(rows);
        BeamFrequentItemsetMiner miner = new BeamFrequentItemsetMiner();
        var mining = miner.mineWithStats(index.getTidsetsByTermId(), cfg, 256, 12, 24);

        TwoPhaseExclusiveItemsetPicker picker = new TwoPhaseExclusiveItemsetPicker();
        List<CandidateItemset> selected = picker.pick(mining.getCandidates(), index.getIdToTerm().size(), 64);

        assertFalse(mining.getCandidates().isEmpty(), "expected mined candidates");
        assertTrue(CandidateFixture.mutuallyExclusiveByTermId(selected));
        assertTrue(isSubsetByContent(selected, mining.getCandidates()));
    }

    @Test
    void minerAndPicker_pipelineHandlesNoFrequentCandidates() {
        List<cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.DocTerms> rows = new ArrayList<>();
        for (int i = 0; i < 30; i++) {
            rows.add(ByteArrayTestSupport.doc(i, new byte[] {(byte) i}));
        }

        SelectorConfig cfg = new SelectorConfig(5, 2, 4, 10_000);
        TermTidsetIndex index = TermTidsetIndex.build(rows);
        BeamFrequentItemsetMiner miner = new BeamFrequentItemsetMiner();
        var mining = miner.mineWithStats(index.getTidsetsByTermId(), cfg, 256, 12, 24);

        TwoPhaseExclusiveItemsetPicker picker = new TwoPhaseExclusiveItemsetPicker();
        List<CandidateItemset> selected = picker.pick(mining.getCandidates(), index.getIdToTerm().size(), 64);

        assertTrue(mining.getCandidates().isEmpty());
        assertTrue(selected.isEmpty());
    }

    @Test
    void minerAndPicker_pipelineRespectsTinyCandidateLimit() {
        List<cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.DocTerms> rows = new ArrayList<>();
        byte[] a = ByteArrayTestSupport.hex("AA");
        byte[] b = ByteArrayTestSupport.hex("BB");
        byte[] c = ByteArrayTestSupport.hex("CC");
        for (int i = 0; i < 40; i++) {
            rows.add(ByteArrayTestSupport.doc(i, a, b, c));
        }

        SelectorConfig cfg = new SelectorConfig(5, 1, 5, 1);
        TermTidsetIndex index = TermTidsetIndex.build(rows);
        var mining = new BeamFrequentItemsetMiner().mineWithStats(index.getTidsetsByTermId(), cfg, 256, 12, 24);
        List<CandidateItemset> selected =
                new TwoPhaseExclusiveItemsetPicker().pick(mining.getCandidates(), index.getIdToTerm().size(), 64);

        assertTrue(mining.isTruncatedByCandidateLimit());
        assertTrue(mining.getGeneratedCandidateCount() <= 1);
        assertTrue(selected.size() <= 1);
        assertTrue(CandidateFixture.mutuallyExclusiveByTermId(selected));
    }

    @Test
    void minerAndPicker_pipelineHandlesRowsWithOnlyEmptyTerms() {
        List<cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.DocTerms> rows = new ArrayList<>();
        rows.add(ByteArrayTestSupport.doc(0, java.util.Collections.emptyList()));
        rows.add(ByteArrayTestSupport.doc(1, java.util.Collections.emptyList()));

        SelectorConfig cfg = new SelectorConfig(1, 1, 4, 100);
        TermTidsetIndex index = TermTidsetIndex.build(rows);
        var mining = new BeamFrequentItemsetMiner().mineWithStats(index.getTidsetsByTermId(), cfg, 256, 12, 24);
        List<CandidateItemset> selected =
                new TwoPhaseExclusiveItemsetPicker().pick(mining.getCandidates(), 1, 64);

        assertTrue(index.getIdToTerm().isEmpty());
        assertTrue(mining.getCandidates().isEmpty());
        assertTrue(selected.isEmpty());
    }

    private static boolean isSubsetByContent(List<CandidateItemset> selected, List<CandidateItemset> all) {
        for (CandidateItemset s : selected) {
            boolean found = false;
            for (CandidateItemset c : all) {
                if (sameCandidate(s, c)) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                return false;
            }
        }
        return true;
    }

    private static boolean sameCandidate(CandidateItemset a, CandidateItemset b) {
        if (!java.util.Arrays.equals(a.getTermIds(), b.getTermIds())) {
            return false;
        }
        if (a.getSupport() != b.getSupport()) {
            return false;
        }
        BitSet ab = a.getDocBits();
        BitSet bb = b.getDocBits();
        return ab.equals(bb);
    }
}
