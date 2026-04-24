package cn.lxdb.plugins.muqingyu.fptoken.tests.unit;

import cn.lxdb.plugins.muqingyu.fptoken.tests.CandidateFixture;

import cn.lxdb.plugins.muqingyu.fptoken.tests.ByteArrayTestSupport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import cn.lxdb.plugins.muqingyu.fptoken.ExclusiveFrequentItemsetSelector;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.config.SelectorConfig;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.index.TermTidsetIndex;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.miner.BeamFrequentItemsetMiner;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.CandidateItemset;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.DocTerms;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.ExclusiveSelectionResult;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.FrequentItemsetMiningResult;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.SelectedGroup;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.util.ByteArrayKey;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.picker.GreedyExclusiveItemsetPicker;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.picker.TwoPhaseExclusiveItemsetPicker;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * 鎸夎鏍肩紪鍙凤紙TI/BK/BM/GP/TP/FS/SC锛夎ˉ鍏呯殑鍥炲綊鐢ㄤ緥锛涢儴鍒嗙紪鍙蜂緷璧栫鏈夊疄鐜帮紝浠呭仛琛屼负绾ф垨鏂囨。璇存槑銆?
 */
class DetailedSpecByIdTest {

    // --- TermTidsetIndex (TI) ---

    @Test
    void TI004_sameTermAcrossThreeDocs() {
        byte[] w = {0x11};
        List<DocTerms> rows = new ArrayList<>();
        rows.add(ByteArrayTestSupport.doc(0, w));
        rows.add(ByteArrayTestSupport.doc(1, w));
        rows.add(ByteArrayTestSupport.doc(2, w));
        TermTidsetIndex idx = TermTidsetIndex.build(rows);
        assertEquals(1, idx.getIdToTerm().size());
        BitSet b = idx.getTidsetsByTermId().get(0);
        assertEquals(3, b.cardinality());
        assertTrue(b.get(0) && b.get(1) && b.get(2));
    }

    @Test
    void TI005_threeDistinctTermsOneDocEach() {
        byte[] a = {1};
        byte[] b = {2};
        byte[] c = {3};
        List<DocTerms> rows = new ArrayList<>();
        rows.add(ByteArrayTestSupport.doc(0, a));
        rows.add(ByteArrayTestSupport.doc(1, b));
        rows.add(ByteArrayTestSupport.doc(2, c));
        TermTidsetIndex idx = TermTidsetIndex.build(rows);
        assertEquals(3, idx.getIdToTerm().size());
        for (int t = 0; t < 3; t++) {
            assertEquals(1, idx.getTidsetsByTermId().get(t).cardinality());
            assertTrue(idx.getTidsetsByTermId().get(t).get(t));
        }
    }

    @Test
    void TI006_termIdsContiguousByFirstOccurrence() {
        byte[] a = {0x10};
        byte[] b = {0x20};
        byte[] c = {0x30};
        List<DocTerms> rows = new ArrayList<>();
        rows.add(ByteArrayTestSupport.doc(0, c, a));
        rows.add(ByteArrayTestSupport.doc(1, b));
        TermTidsetIndex idx = TermTidsetIndex.build(rows);
        assertEquals(3, idx.getIdToTerm().size());
        assertTrue(Arrays.equals(c, idx.getIdToTerm().get(0)));
        assertTrue(Arrays.equals(a, idx.getIdToTerm().get(1)));
        assertTrue(Arrays.equals(b, idx.getIdToTerm().get(2)));
    }

    @Test
    void TI007_nullAndEmptyTermsSkippedInDocTerms() {
        byte[] good = {0x55};
        List<byte[]> raw = new ArrayList<>();
        raw.add(null);
        raw.add(new byte[0]);
        raw.add(good);
        List<DocTerms> rows = new ArrayList<>();
        rows.add(new DocTerms(0, raw));
        rows.add(ByteArrayTestSupport.doc(1, good));
        TermTidsetIndex idx = TermTidsetIndex.build(rows);
        assertEquals(1, idx.getIdToTerm().size());
        BitSet b = idx.getTidsetsByTermId().get(0);
        assertEquals(2, b.cardinality());
        assertTrue(b.get(0) && b.get(1));
    }

    @Test
    void TI008_largeBuildTenThousandDocs() {
        List<byte[]> vocab = new ArrayList<>(5000);
        for (int i = 0; i < 5000; i++) {
            vocab.add(new byte[] {(byte) (i >>> 8), (byte) i});
        }
        List<DocTerms> rows = new ArrayList<>(10_000);
        for (int d = 0; d < 10_000; d++) {
            rows.add(ByteArrayTestSupport.doc(d, vocab.get(d % 5000)));
        }
        TermTidsetIndex idx = TermTidsetIndex.build(rows);
        assertEquals(5000, idx.getIdToTerm().size());
        assertEquals(5000, idx.getTidsetsByTermId().size());
    }

    // --- ByteArrayKey (BK) ---

    @Test
    void BK001_to_BK004_contentAndEmptyEquality() {
        ByteArrayKey a = new ByteArrayKey(new byte[] {1, 2, 3});
        ByteArrayKey b = new ByteArrayKey(new byte[] {1, 2, 3});
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
        assertNotEquals(a, new ByteArrayKey(new byte[] {1, 2, 4}));
        assertNotEquals(new ByteArrayKey(new byte[] {1, 2}), new ByteArrayKey(new byte[] {1, 2, 3}));
        ByteArrayKey e0 = new ByteArrayKey(new byte[0]);
        ByteArrayKey e1 = new ByteArrayKey(new byte[0]);
        assertEquals(e0, e1);
        assertEquals(e0.hashCode(), e1.hashCode());
    }

    @Test
    void BK005_BK006_BK007_nullReflexiveSymmetric() {
        ByteArrayKey k = new ByteArrayKey(new byte[] {9});
        assertFalse(k.equals(null));
        assertEquals(k, k);
        ByteArrayKey o = new ByteArrayKey(new byte[] {9});
        assertEquals(k, o);
        assertEquals(o, k);
    }

    @Test
    void BK008_hashMapLookupByContent() {
        Map<ByteArrayKey, String> m = new HashMap<>();
        m.put(new ByteArrayKey(new byte[] {1, 2}), "x");
        assertEquals("x", m.get(new ByteArrayKey(new byte[] {1, 2})));
    }

    // --- BeamFrequentItemsetMiner (BM) ---

    private static BitSet bs(int... docs) {
        BitSet b = new BitSet();
        for (int d : docs) {
            b.set(d);
        }
        return b;
    }

    @Test
    void BM005_frequent4Itemset() {
        List<BitSet> tid = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            BitSet b = new BitSet();
            for (int d = 0; d < 12; d++) {
                b.set(d);
            }
            tid.add(b);
        }
        BeamFrequentItemsetMiner miner = new BeamFrequentItemsetMiner();
        SelectorConfig cfg = new SelectorConfig(4, 1, 6, 8000);
        FrequentItemsetMiningResult r = miner.mineWithStats(tid, cfg, 0, 32, 64);
        boolean has4 = false;
        for (CandidateItemset c : r.getCandidates()) {
            if (c.length() == 4 && c.getSupport() == 12) {
                has4 = true;
                break;
            }
        }
        assertTrue(has4, () -> "expected some 4-itemset support 12: " + r.getCandidates());
    }

    @Test
    void BM006_minLength3_excludesPairs() {
        List<BitSet> tid = new ArrayList<>();
        tid.add(bs(0, 1, 2, 3));
        tid.add(bs(0, 1, 2, 3));
        tid.add(bs(0, 1, 2, 3, 4));
        BeamFrequentItemsetMiner miner = new BeamFrequentItemsetMiner();
        SelectorConfig cfg = new SelectorConfig(2, 3, 5, 5000);
        FrequentItemsetMiningResult r = miner.mineWithStats(tid, cfg, 0, 16, 32);
        for (CandidateItemset c : r.getCandidates()) {
            assertTrue(c.length() >= 3, () -> "unexpected short itemset: " + c.length());
        }
    }

    @Test
    void BM009_runtimeOverload_terminates() {
        List<BitSet> tid = new ArrayList<>();
        for (int i = 0; i < 25; i++) {
            BitSet b = new BitSet();
            for (int d = 0; d < 60; d++) {
                b.set(d);
            }
            tid.add(b);
        }
        BeamFrequentItemsetMiner miner = new BeamFrequentItemsetMiner();
        SelectorConfig cfg = new SelectorConfig(12, 2, 7, 40_000);
        FrequentItemsetMiningResult r = miner.mineWithStats(tid, cfg, 0, 64, 96, 0, 80L);
        assertNotNull(r.getCandidates());
    }

    @Test
    void BM016_widerBeam_nonDecreasingCandidates() {
        List<BitSet> tid = new ArrayList<>();
        for (int i = 0; i < 9; i++) {
            BitSet b = new BitSet();
            for (int d = 0; d < 28; d++) {
                b.set(d);
            }
            tid.add(b);
        }
        BeamFrequentItemsetMiner miner = new BeamFrequentItemsetMiner();
        SelectorConfig cfg = new SelectorConfig(8, 2, 6, 12_000);
        int c16 = miner.mineWithStats(tid, cfg, 0, 24, 16).getGeneratedCandidateCount();
        int c32 = miner.mineWithStats(tid, cfg, 0, 24, 32).getGeneratedCandidateCount();
        int c64 = miner.mineWithStats(tid, cfg, 0, 24, 64).getGeneratedCandidateCount();
        assertTrue(c32 >= c16);
        assertTrue(c64 >= c32);
    }

    @Test
    void BM027_defaultScore_matchesExplicitFormula() {
        BeamFrequentItemsetMiner def = new BeamFrequentItemsetMiner();
        BeamFrequentItemsetMiner ex = new BeamFrequentItemsetMiner((l, s) -> (l - 1) * s);
        List<BitSet> tid = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            BitSet b = new BitSet();
            for (int d = 0; d < 10; d++) {
                b.set(d);
            }
            tid.add(b);
        }
        SelectorConfig cfg = new SelectorConfig(5, 2, 5, 3000);
        assertEquals(
                def.mineWithStats(tid, cfg, 0, 16, 24).getGeneratedCandidateCount(),
                ex.mineWithStats(tid, cfg, 0, 16, 24).getGeneratedCandidateCount());
        assertEquals(0, (1 - 1) * 99);
        assertEquals(21, (4 - 1) * 7);
    }

    @Test
    void BM028_customScore_lenSquaredSupport_formulaAndBeamEquivalence() {
        List<BitSet> tid = new ArrayList<>();
        BitSet hi = new BitSet();
        for (int d = 0; d < 25; d++) {
            hi.set(d);
        }
        BitSet lo = new BitSet();
        for (int d = 0; d < 8; d++) {
            lo.set(d);
        }
        tid.add(hi);
        tid.add(lo);
        SelectorConfig cfg = new SelectorConfig(4, 2, 5, 6000);
        BeamFrequentItemsetMiner lenBiased = new BeamFrequentItemsetMiner((len, sup) -> len * len * sup);
        BeamFrequentItemsetMiner def = new BeamFrequentItemsetMiner();
        FrequentItemsetMiningResult r1 = lenBiased.mineWithStats(tid, cfg, 0, 12, 8);
        FrequentItemsetMiningResult r2 = def.mineWithStats(tid, cfg, 0, 12, 8);
        assertEquals(r2.getGeneratedCandidateCount(), r1.getGeneratedCandidateCount());
        assertEquals(r2.getIntersectionCount(), r1.getIntersectionCount());
        int sup = 10;
        assertTrue(2 * 2 * sup < 3 * 3 * sup);
        assertTrue(3 * 3 * sup < 4 * 4 * sup);
    }

    // --- GreedyExclusiveItemsetPicker (GP) ---

    @Test
    void GP003_threeDisjointSingletonsAllPicked() {
        GreedyExclusiveItemsetPicker p = new GreedyExclusiveItemsetPicker();
        List<CandidateItemset> c = new ArrayList<>();
        c.add(CandidateFixture.itemset(new int[] {0}, 0));
        c.add(CandidateFixture.itemset(new int[] {1}, 0));
        c.add(CandidateFixture.itemset(new int[] {2}, 0));
        List<CandidateItemset> out = p.pick(c, 8);
        assertEquals(3, out.size());
        assertTrue(CandidateFixture.mutuallyExclusiveByTermId(out));
    }

    @Test
    void GP004_partialOverlapPickOne() {
        GreedyExclusiveItemsetPicker p = new GreedyExclusiveItemsetPicker();
        List<CandidateItemset> c = new ArrayList<>();
        c.add(CandidateFixture.itemset(new int[] {0, 1}, 0, 1));
        c.add(CandidateFixture.itemset(new int[] {1, 2}, 0, 1));
        List<CandidateItemset> out = p.pick(c, 8);
        assertEquals(1, out.size());
    }

    @Test
    void GP005_longerSupersetPreferredOverSubsetPattern() {
        GreedyExclusiveItemsetPicker p = new GreedyExclusiveItemsetPicker();
        List<CandidateItemset> c = new ArrayList<>();
        c.add(CandidateFixture.itemset(new int[] {0, 1}, 0, 1, 2));
        c.add(CandidateFixture.itemset(new int[] {0, 1, 2}, 0, 1, 2));
        List<CandidateItemset> out = p.pick(c, 8);
        assertEquals(1, out.size());
        assertEquals(3, out.get(0).length());
    }

    @Test
    void GP014_dictionarySizeOne_termZeroOnly() {
        GreedyExclusiveItemsetPicker p = new GreedyExclusiveItemsetPicker();
        List<CandidateItemset> c = Collections.singletonList(CandidateFixture.itemset(new int[] {0}, 0, 1));
        List<CandidateItemset> out = p.pick(c, 1);
        assertEquals(1, out.size());
    }

    @Test
    void GP015_largeTermId_effectiveBitset() {
        GreedyExclusiveItemsetPicker p = new GreedyExclusiveItemsetPicker();
        int tid = 500_000;
        List<CandidateItemset> c =
                Collections.singletonList(CandidateFixture.itemset(new int[] {tid}, 0, 1));
        List<CandidateItemset> out = p.pick(c, 2);
        assertEquals(1, out.size());
        assertEquals(tid, out.get(0).getTermIds()[0]);
    }

    @Test
    void GP016_manyDisjointSingletons_mostlyAllPicked() {
        GreedyExclusiveItemsetPicker p = new GreedyExclusiveItemsetPicker();
        List<CandidateItemset> c = new ArrayList<>();
        for (int i = 0; i < 200; i++) {
            c.add(CandidateFixture.itemset(new int[] {i}, 0));
        }
        List<CandidateItemset> out = p.pick(c, 50);
        assertEquals(200, out.size());
    }

    // --- TwoPhaseExclusiveItemsetPicker (TP) ---

    @Test
    void TP020_emptyCandidates_returnsEmpty() {
        TwoPhaseExclusiveItemsetPicker p = new TwoPhaseExclusiveItemsetPicker();
        assertTrue(p.pick(Collections.emptyList(), 4, 100).isEmpty());
    }

    @Test
    void TP022_negativeMaxSwapTrials_throws() {
        TwoPhaseExclusiveItemsetPicker p = new TwoPhaseExclusiveItemsetPicker();
        assertThrows(
                IllegalArgumentException.class,
                () -> p.pick(Collections.singletonList(CandidateFixture.itemset(new int[] {0}, 0)), 2, -1));
    }

    @Test
    void TP008_weakerChallenger_noChangeMatchesGreedy() {
        List<CandidateItemset> c = new ArrayList<>();
        c.add(CandidateFixture.itemset(new int[] {0}, 0, 1, 2, 3));
        c.add(CandidateFixture.itemset(new int[] {1}, 0, 1));
        GreedyExclusiveItemsetPicker g = new GreedyExclusiveItemsetPicker();
        TwoPhaseExclusiveItemsetPicker t = new TwoPhaseExclusiveItemsetPicker();
        List<CandidateItemset> gOut = g.pick(c, 8);
        List<CandidateItemset> tOut = t.pick(c, 8, 500);
        assertTrue(CandidateFixture.samePickOrder(gOut, tOut));
    }

    // --- ExclusiveFrequentItemsetSelector (FS) ---

    @Test
    void FS001_shortcutReturnsGroupsOnly() {
        List<DocTerms> rows = new ArrayList<>();
        byte[] pat = ByteArrayTestSupport.hex("AABB");
        for (int i = 0; i < 20; i++) {
            rows.add(ByteArrayTestSupport.doc(i, pat));
        }
        List<SelectedGroup> g =
                ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsets(rows, 8, 1);
        assertNotNull(g);
    }

    @Test
    void FS002_fullListOverload() {
        List<DocTerms> rows = new ArrayList<>();
        byte[] pat = {1, 2};
        for (int i = 0; i < 18; i++) {
            rows.add(ByteArrayTestSupport.doc(i, pat));
        }
        List<SelectedGroup> g = ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsets(rows, 5, 1, 4, 5000);
        assertNotNull(g);
    }

    @Test
    void FS003_withStatsDefaultSizes() {
        List<DocTerms> rows = new ArrayList<>();
        for (int i = 0; i < 15; i++) {
            rows.add(ByteArrayTestSupport.doc(i, new byte[] {9}));
        }
        ExclusiveSelectionResult r = ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(rows, 3, 1);
        assertNotNull(r.getGroups());
        assertTrue(r.getMaxCandidateCount() > 0);
    }

    @Test
    void FS005_termBytesRoundTrip() {
        byte[] needle = ByteArrayTestSupport.hex("CCDD");
        List<DocTerms> rows = new ArrayList<>();
        for (int i = 0; i < 25; i++) {
            rows.add(ByteArrayTestSupport.doc(i, needle, ByteArrayTestSupport.hex("01")));
        }
        List<SelectedGroup> g =
                ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsets(rows, 10, 2, 5, 8000);
        assertTrue(ByteArrayTestSupport.anyGroupContainsTerm(g, needle));
    }

    @Test
    void FS006_docIdsAscending() {
        byte[] w = {7};
        List<DocTerms> rows = new ArrayList<>();
        rows.add(ByteArrayTestSupport.doc(100, w));
        rows.add(ByteArrayTestSupport.doc(5, w));
        rows.add(ByteArrayTestSupport.doc(20, w));
        List<SelectedGroup> g = ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsets(rows, 2, 1, 4, 3000);
        for (SelectedGroup sg : g) {
            assertTrue(ByteArrayTestSupport.isSortedAscending(sg.getDocIds()));
        }
    }

    @Test
    void FS008_statsPropagatedWhenMiningRuns() {
        byte[] common = ByteArrayTestSupport.hex("FACE");
        List<DocTerms> rows = new ArrayList<>();
        for (int i = 0; i < 35; i++) {
            rows.add(ByteArrayTestSupport.doc(i, common, ByteArrayTestSupport.hex("02")));
        }
        int maxCand = 500;
        ExclusiveSelectionResult r =
                ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(rows, 12, 2, 5, maxCand);
        assertEquals(maxCand, r.getMaxCandidateCount());
        assertTrue(r.getFrequentTermCount() >= 0);
        assertTrue(r.getCandidateCount() >= 0);
        assertTrue(r.getIntersectionCount() >= 0);
    }

    @Test
    void FS012_indexedButNoFrequentPatterns() {
        List<DocTerms> rows = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            rows.add(ByteArrayTestSupport.doc(i, new byte[] {(byte) i}));
        }
        ExclusiveSelectionResult r =
                ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(rows, 50, 2, 4, 10_000);
        assertTrue(r.getGroups().isEmpty());
    }

    // --- SelectorConfig (SC) ---

    @Test
    void SC001_valid() {
        SelectorConfig c = new SelectorConfig(10, 2, 6, 1000);
        assertEquals(10, c.getMinSupport());
        assertEquals(6, c.getMaxItemsetSize());
    }

    @Test
    void SC006_allOnes() {
        SelectorConfig c = new SelectorConfig(1, 1, 1, 1);
        assertEquals(1, c.getMinItemsetSize());
        assertEquals(1, c.getMaxItemsetSize());
    }
}

