package cn.lxdb.plugins.muqingyu.fptoken.tests.unit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.config.SelectorConfig;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.miner.BeamFrequentItemsetMiner;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.CandidateItemset;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.FrequentItemsetMiningResult;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.config.EngineTuningConfig;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.lang.reflect.Method;
import org.junit.jupiter.api.Test;

class BeamFrequentItemsetMinerTest {

    @Test
    void emptyTidsets_returnsEmptyStats() {
        BeamFrequentItemsetMiner miner = new BeamFrequentItemsetMiner();
        SelectorConfig cfg = new SelectorConfig(1, 1, 4, 1000);
        FrequentItemsetMiningResult r = miner.mineWithStats(Collections.emptyList(), cfg, 0, 8, 16);
        assertTrue(r.getCandidates().isEmpty());
        assertEquals(0, r.getFrequentTermCount());
        assertEquals(0, r.getGeneratedCandidateCount());
        assertEquals(0, r.getIntersectionCount());
        assertFalse(r.isTruncatedByCandidateLimit());
    }

    @Test
    void nullConfig_withNonEmptyTidsets_throwsIllegalArgument() {
        List<BitSet> tid = new ArrayList<>();
        tid.add(bit(0));
        BeamFrequentItemsetMiner miner = new BeamFrequentItemsetMiner();
        assertThrows(IllegalArgumentException.class, () -> miner.mineWithStats(tid, null, 0, 8, 16));
    }

    @Test
    void nullOrEmptyTidsets_returnEmptyEvenIfConfigNull() {
        BeamFrequentItemsetMiner miner = new BeamFrequentItemsetMiner();
        FrequentItemsetMiningResult r1 = miner.mineWithStats(null, null, 0, 8, 16);
        FrequentItemsetMiningResult r2 = miner.mineWithStats(Collections.emptyList(), null, 0, 8, 16);
        assertTrue(r1.getCandidates().isEmpty());
        assertEquals(0, r1.getFrequentTermCount());
        assertTrue(r2.getCandidates().isEmpty());
        assertEquals(0, r2.getGeneratedCandidateCount());
    }

    @Test
    void tidsetsContainingNullElement_throwIllegalArgument() {
        List<BitSet> tid = new ArrayList<>();
        tid.add(new BitSet());
        tid.add(null);
        SelectorConfig cfg = new SelectorConfig(1, 1, 3, 1000);
        BeamFrequentItemsetMiner miner = new BeamFrequentItemsetMiner();
        assertThrows(IllegalArgumentException.class, () -> miner.mineWithStats(tid, cfg, 0, 8, 16));
    }

    @Test
    void noFrequentTerms_returnsEmpty() {
        List<BitSet> tid = new ArrayList<>();
        tid.add(bit(0));
        tid.add(bit(1));
        BeamFrequentItemsetMiner miner = new BeamFrequentItemsetMiner();
        SelectorConfig cfg = new SelectorConfig(5, 1, 3, 1000);
        FrequentItemsetMiningResult r = miner.mineWithStats(tid, cfg, 0, 8, 16);
        assertTrue(r.getCandidates().isEmpty());
    }

    @Test
    void singleFrequentTerm_minItemset1_emitsSingleton() {
        List<BitSet> tid = new ArrayList<>();
        BitSet b0 = new BitSet();
        b0.set(0);
        b0.set(1);
        b0.set(2);
        tid.add(b0);
        BeamFrequentItemsetMiner miner = new BeamFrequentItemsetMiner();
        SelectorConfig cfg = new SelectorConfig(2, 1, 3, 1000);
        FrequentItemsetMiningResult r = miner.mineWithStats(tid, cfg, 0, 8, 32);
        assertTrue(r.getFrequentTermCount() >= 1);
        assertFalse(r.getCandidates().isEmpty());
        CandidateItemset first = r.getCandidates().get(0);
        assertEquals(3, first.getSupport());
        assertEquals(0, first.getEstimatedSaving());
        assertTrue(r.getGeneratedCandidateCount() == r.getCandidates().size());
    }

    @Test
    void pairCooccurrence_minItemset2() {
        List<BitSet> tid = new ArrayList<>();
        BitSet t0 = new BitSet();
        t0.set(0);
        t0.set(1);
        t0.set(2);
        BitSet t1 = new BitSet();
        t1.set(0);
        t1.set(1);
        tid.add(t0);
        tid.add(t1);
        BeamFrequentItemsetMiner miner = new BeamFrequentItemsetMiner();
        SelectorConfig cfg = new SelectorConfig(2, 2, 4, 5000);
        FrequentItemsetMiningResult r = miner.mineWithStats(tid, cfg, 0, 16, 32);
        boolean hasPair = false;
        for (CandidateItemset c : r.getCandidates()) {
            if (c.length() == 2 && c.getSupport() == 2) {
                int[] ids = c.getTermIds();
                if (ids[0] == 0 && ids[1] == 1) {
                    hasPair = true;
                    assertEquals(2, c.getEstimatedSaving());
                    break;
                }
            }
        }
        assertTrue(hasPair, () -> "expected frequent pair {0,1} with support 2: " + r.getCandidates());
        assertTrue(r.getIntersectionCount() > 0);
    }

    @Test
    void maxCandidateCount_truncates() {
        List<BitSet> tid = new ArrayList<>();
        for (int i = 0; i < 8; i++) {
            BitSet b = new BitSet();
            for (int d = 0; d < 10; d++) {
                b.set(d);
            }
            tid.add(b);
        }
        BeamFrequentItemsetMiner miner = new BeamFrequentItemsetMiner();
        SelectorConfig cfg = new SelectorConfig(3, 1, 4, 4);
        FrequentItemsetMiningResult r = miner.mineWithStats(tid, cfg, 0, 32, 64);
        assertTrue(r.isTruncatedByCandidateLimit());
        assertTrue(r.getGeneratedCandidateCount() <= 4);
        assertEquals(r.getGeneratedCandidateCount(), r.getCandidates().size());
    }

    @Test
    void nullScoreFunction_sameAsDefaultFormula() {
        List<BitSet> tid = twoTermPairTidsets();
        SelectorConfig cfg = new SelectorConfig(2, 2, 4, 5000);
        FrequentItemsetMiningResult r0 = new BeamFrequentItemsetMiner(null).mineWithStats(tid, cfg, 0, 16, 32);
        FrequentItemsetMiningResult r1 = new BeamFrequentItemsetMiner((len, sup) -> (len - 1) * sup)
                .mineWithStats(tid, cfg, 0, 16, 32);
        assertEquals(r0.getGeneratedCandidateCount(), r1.getGeneratedCandidateCount());
        assertEquals(r0.getIntersectionCount(), r1.getIntersectionCount());
    }

    @Test
    void tripletCooccurrence_minItemset3_prefixChainMaterializedOrder() {
        List<BitSet> tid = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            BitSet b = new BitSet();
            b.set(0);
            b.set(1);
            b.set(2);
            tid.add(b);
        }
        BeamFrequentItemsetMiner miner = new BeamFrequentItemsetMiner();
        SelectorConfig cfg = new SelectorConfig(3, 3, 4, 5000);
        FrequentItemsetMiningResult r = miner.mineWithStats(tid, cfg, 0, 16, 32);
        boolean ok = false;
        for (CandidateItemset c : r.getCandidates()) {
            if (c.length() == 3 && c.getSupport() == 3) {
                int[] ids = c.getTermIds();
                if (ids[0] == 0 && ids[1] == 1 && ids[2] == 2) {
                    assertEquals(6, c.getEstimatedSaving());
                    ok = true;
                    break;
                }
            }
        }
        assertTrue(ok, () -> "expected 3-itemset {0,1,2} support 3: " + r.getCandidates());
    }

    @Test
    void minItemsetSizeGreaterThan2_shouldSkipShortCandidatesAndFindTriplet() {
        List<BitSet> tid = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            BitSet b = new BitSet();
            b.set(0);
            b.set(1);
            b.set(2);
            tid.add(b);
        }
        BeamFrequentItemsetMiner miner = new BeamFrequentItemsetMiner();
        SelectorConfig cfg = new SelectorConfig(3, 3, 4, 5000);
        FrequentItemsetMiningResult r = miner.mineWithStats(tid, cfg, 0, 16, 32);

        boolean hasTriplet = false;
        for (CandidateItemset c : r.getCandidates()) {
            assertTrue(c.length() >= 3, () -> "unexpected short candidate when minItemsetSize=3: " + c);
            if (c.length() == 3 && c.getSupport() == 3) {
                int[] ids = c.getTermIds();
                if (ids[0] == 0 && ids[1] == 1 && ids[2] == 2) {
                    hasTriplet = true;
                }
            }
        }
        assertTrue(hasTriplet);
    }

    @Test
    void maxFrequentTermCount_oneTerm_cannotFormMinSize2() {
        List<BitSet> tid = new ArrayList<>();
        BitSet b0 = new BitSet();
        b0.set(0);
        b0.set(1);
        tid.add(b0);
        BitSet b1 = new BitSet();
        b1.set(2);
        tid.add(b1);
        BeamFrequentItemsetMiner miner = new BeamFrequentItemsetMiner();
        SelectorConfig cfg = new SelectorConfig(2, 2, 4, 1000);
        FrequentItemsetMiningResult r = miner.mineWithStats(tid, cfg, 1, 8, 16);
        assertEquals(1, r.getFrequentTermCount());
        assertTrue(r.getCandidates().isEmpty());
    }

    @Test
    void maxIdleLevels_stopsWhenNoDeeperCandidates() {
        List<BitSet> tid = new ArrayList<>();
        BitSet b = new BitSet();
        b.set(0);
        b.set(1);
        tid.add(b);
        BeamFrequentItemsetMiner miner = new BeamFrequentItemsetMiner();
        SelectorConfig cfg = new SelectorConfig(2, 2, 4, 1000);
        FrequentItemsetMiningResult r = miner.mineWithStats(tid, cfg, 0, 8, 16, 1, 0L);
        assertTrue(r.getCandidates().isEmpty());
        assertEquals(0, r.getIntersectionCount());
    }

    @Test
    void negativeTimeoutAndIdleValues_behaveLikeDisabledControls() {
        List<BitSet> tid = twoTermPairTidsets();
        BeamFrequentItemsetMiner miner = new BeamFrequentItemsetMiner();
        SelectorConfig cfg = new SelectorConfig(2, 2, 4, 5000);
        FrequentItemsetMiningResult baseline = miner.mineWithStats(tid, cfg, 0, 16, 32, 0, 0L);
        FrequentItemsetMiningResult negativeControls = miner.mineWithStats(tid, cfg, 0, 16, 32, -3, -1L);
        assertEquals(baseline.getGeneratedCandidateCount(), negativeControls.getGeneratedCandidateCount());
        assertEquals(baseline.getIntersectionCount(), negativeControls.getIntersectionCount());
    }

    @Test
    void maxBranchingFactor_wideExploresAtLeastAsMuchAsNarrow() {
        List<BitSet> tid = new ArrayList<>();
        for (int i = 0; i < 6; i++) {
            BitSet b = new BitSet();
            for (int d = 0; d < 12; d++) {
                b.set(d);
            }
            tid.add(b);
        }
        BeamFrequentItemsetMiner miner = new BeamFrequentItemsetMiner();
        SelectorConfig cfg = new SelectorConfig(8, 2, 4, 8000);
        FrequentItemsetMiningResult narrow = miner.mineWithStats(tid, cfg, 0, 1, 48);
        FrequentItemsetMiningResult wide = miner.mineWithStats(tid, cfg, 0, 32, 48);
        assertTrue(wide.getGeneratedCandidateCount() >= narrow.getGeneratedCandidateCount());
    }

    @Test
    void beamWidthNonPositive_usesDefaultWidth_behaviorStableOnTinyInput() {
        List<BitSet> tid = twoTermPairTidsets();
        SelectorConfig cfg = new SelectorConfig(2, 2, 4, 5000);
        BeamFrequentItemsetMiner miner = new BeamFrequentItemsetMiner();
        FrequentItemsetMiningResult explicit = miner.mineWithStats(tid, cfg, 0, 16, 64);
        FrequentItemsetMiningResult zeroBeam = miner.mineWithStats(tid, cfg, 0, 16, 0);
        assertEquals(explicit.getGeneratedCandidateCount(), zeroBeam.getGeneratedCandidateCount());
    }

    @Test
    void maxFrequentTermCount_nonPositive_behavesAsUnlimitedOnTinyInput() {
        List<BitSet> tid = twoTermPairTidsets();
        SelectorConfig cfg = new SelectorConfig(2, 2, 4, 5000);
        BeamFrequentItemsetMiner miner = new BeamFrequentItemsetMiner();
        FrequentItemsetMiningResult unlimited = miner.mineWithStats(tid, cfg, 0, 16, 32);
        FrequentItemsetMiningResult negative = miner.mineWithStats(tid, cfg, -1, 16, 32);
        assertEquals(unlimited.getFrequentTermCount(), negative.getFrequentTermCount());
        assertEquals(unlimited.getGeneratedCandidateCount(), negative.getGeneratedCandidateCount());
    }

    @Test
    void maxBranchingFactor_nonPositive_behavesAsUnlimitedOnTinyInput() {
        List<BitSet> tid = twoTermPairTidsets();
        SelectorConfig cfg = new SelectorConfig(2, 2, 4, 5000);
        BeamFrequentItemsetMiner miner = new BeamFrequentItemsetMiner();
        FrequentItemsetMiningResult unlimited = miner.mineWithStats(tid, cfg, 0, 16, 32);
        FrequentItemsetMiningResult zeroBranch = miner.mineWithStats(tid, cfg, 0, 0, 32);
        FrequentItemsetMiningResult negativeBranch = miner.mineWithStats(tid, cfg, 0, -5, 32);
        assertEquals(unlimited.getGeneratedCandidateCount(), zeroBranch.getGeneratedCandidateCount());
        assertEquals(unlimited.getGeneratedCandidateCount(), negativeBranch.getGeneratedCandidateCount());
    }

    @Test
    void allTidsetsEmpty_noFrequentTerms() {
        List<BitSet> tid = new ArrayList<>();
        tid.add(new BitSet());
        tid.add(new BitSet());
        SelectorConfig cfg = new SelectorConfig(1, 1, 4, 500);
        FrequentItemsetMiningResult r = new BeamFrequentItemsetMiner().mineWithStats(tid, cfg, 0, 16, 32);
        assertEquals(0, r.getFrequentTermCount());
        assertTrue(r.getCandidates().isEmpty());
    }

    @Test
    void readableMineAliases_matchMineWithStatsBehavior() {
        List<BitSet> tid = twoTermPairTidsets();
        SelectorConfig cfg = new SelectorConfig(2, 2, 4, 5000);
        BeamFrequentItemsetMiner miner = new BeamFrequentItemsetMiner();

        FrequentItemsetMiningResult legacy = miner.mineWithStats(tid, cfg, 0, 16, 32);
        FrequentItemsetMiningResult alias = miner.mine(tid, cfg, 0, 16, 32);
        assertEquals(legacy.getGeneratedCandidateCount(), alias.getGeneratedCandidateCount());
        assertEquals(legacy.getIntersectionCount(), alias.getIntersectionCount());

        FrequentItemsetMiningResult legacyExt = miner.mineWithStats(tid, cfg, 0, 16, 32, 0, 0L);
        FrequentItemsetMiningResult aliasExt = miner.mine(tid, cfg, 0, 16, 32, 0, 0L);
        assertEquals(legacyExt.getGeneratedCandidateCount(), aliasExt.getGeneratedCandidateCount());
        assertEquals(legacyExt.getIntersectionCount(), aliasExt.getIntersectionCount());
    }

    private static List<BitSet> twoTermPairTidsets() {
        List<BitSet> tid = new ArrayList<>();
        BitSet t0 = new BitSet();
        t0.set(0);
        t0.set(1);
        t0.set(2);
        BitSet t1 = new BitSet();
        t1.set(0);
        t1.set(1);
        tid.add(t0);
        tid.add(t1);
        return tid;
    }

    @Test
    void minEqualsMaxItemsetSize_singletonPath() {
        List<BitSet> tid = new ArrayList<>();
        BitSet b = new BitSet();
        b.set(0);
        b.set(1);
        tid.add(b);
        BeamFrequentItemsetMiner miner = new BeamFrequentItemsetMiner();
        SelectorConfig cfg = new SelectorConfig(1, 1, 1, 500);
        FrequentItemsetMiningResult r = miner.mineWithStats(tid, cfg, 0, 8, 16);
        assertEquals(1, r.getCandidates().size());
        assertEquals(1, r.getCandidates().get(0).length());
    }

    @Test
    void maxFrequentTermCount_twoOfThree_equalSupport_keepsSmallestIds() {
        List<BitSet> tid = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            BitSet b = new BitSet();
            for (int d = 0; d < 8; d++) {
                b.set(d);
            }
            tid.add(b);
        }
        BeamFrequentItemsetMiner miner = new BeamFrequentItemsetMiner();
        SelectorConfig cfg = new SelectorConfig(4, 2, 4, 2000);
        FrequentItemsetMiningResult r = miner.mineWithStats(tid, cfg, 2, 16, 32);
        assertEquals(2, r.getFrequentTermCount());
        for (CandidateItemset c : r.getCandidates()) {
            for (int id : c.getTermIds()) {
                assertTrue(id <= 1, () -> "termId should be 0 or 1 only, got " + id);
            }
        }
    }

    @Test
    void spec_singleStrongFrequentTerm_topMFrequentLimitsToOne() {
        List<BitSet> tid = new ArrayList<>();
        tid.add(bitsetFromDocIds(1, 2, 3, 4, 5));
        tid.add(bitsetFromDocIds(2, 3, 4));
        BeamFrequentItemsetMiner miner = new BeamFrequentItemsetMiner();
        SelectorConfig cfg = new SelectorConfig(3, 1, 4, 500);
        FrequentItemsetMiningResult r = miner.mineWithStats(tid, cfg, 1, 8, 16);
        assertEquals(1, r.getFrequentTermCount());
        assertEquals(1, r.getCandidates().size());
        assertEquals(1, r.getCandidates().get(0).length());
        assertEquals(0, r.getCandidates().get(0).getTermIds()[0]);
        assertEquals(5, r.getCandidates().get(0).getSupport());
    }

    @Test
    void spec_threeFrequentSingletons_noPairRequired() {
        List<BitSet> tid = new ArrayList<>();
        tid.add(bitsetFromDocIds(1, 2, 3, 4, 5));
        tid.add(bitsetFromDocIds(2, 3, 4, 6));
        tid.add(bitsetFromDocIds(1, 2, 3));
        BeamFrequentItemsetMiner miner = new BeamFrequentItemsetMiner();
        SelectorConfig cfg = new SelectorConfig(3, 1, 4, 2000);
        FrequentItemsetMiningResult r = miner.mineWithStats(tid, cfg, 0, 16, 32);
        assertEquals(3, r.getFrequentTermCount());
        Set<Integer> singletons = new HashSet<>();
        for (CandidateItemset c : r.getCandidates()) {
            if (c.length() == 1) {
                singletons.add(c.getTermIds()[0]);
            }
        }
        assertTrue(singletons.contains(0));
        assertTrue(singletons.contains(1));
        assertTrue(singletons.contains(2));
    }

    @Test
    void spec_singletonsAndFrequentPair01_minSupport2() {
        List<BitSet> tid = new ArrayList<>();
        tid.add(bitsetFromDocIds(1, 2, 3, 4, 5));
        tid.add(bitsetFromDocIds(2, 3, 4, 6));
        tid.add(bitsetFromDocIds(1, 2, 3));
        BeamFrequentItemsetMiner miner = new BeamFrequentItemsetMiner();
        SelectorConfig cfg = new SelectorConfig(2, 1, 4, 5000);
        FrequentItemsetMiningResult r = miner.mineWithStats(tid, cfg, 0, 32, 64);
        assertEquals(3, r.getFrequentTermCount());
        boolean hasPair01 = false;
        for (CandidateItemset c : r.getCandidates()) {
            if (c.length() == 2) {
                int[] ids = c.getTermIds();
                if (ids[0] == 0 && ids[1] == 1 && c.getSupport() == 3) {
                    hasPair01 = true;
                    break;
                }
            }
        }
        assertTrue(hasPair01, () -> "expected pair {0,1} support 3: " + r.getCandidates());
    }

    @Test
    void narrowBeam_fewerCandidatesThanWideBeam_onDenseCooccurrence() {
        List<BitSet> tid = new ArrayList<>();
        for (int i = 0; i < 8; i++) {
            BitSet b = new BitSet();
            for (int d = 0; d < 24; d++) {
                b.set(d);
            }
            tid.add(b);
        }
        BeamFrequentItemsetMiner miner = new BeamFrequentItemsetMiner();
        SelectorConfig cfg = new SelectorConfig(10, 2, 6, 8000);
        FrequentItemsetMiningResult narrow = miner.mineWithStats(tid, cfg, 0, 32, 1);
        FrequentItemsetMiningResult wide = miner.mineWithStats(tid, cfg, 0, 32, 128);
        assertTrue(wide.getGeneratedCandidateCount() > narrow.getGeneratedCandidateCount());
    }

    @Test
    void maxBranchingFactor_one_boundsExplorationComparedToWide() {
        List<BitSet> tid = new ArrayList<>();
        for (int i = 0; i < 7; i++) {
            BitSet b = new BitSet();
            for (int d = 0; d < 16; d++) {
                b.set(d);
            }
            tid.add(b);
        }
        BeamFrequentItemsetMiner miner = new BeamFrequentItemsetMiner();
        SelectorConfig cfg = new SelectorConfig(8, 2, 5, 6000);
        FrequentItemsetMiningResult b1 = miner.mineWithStats(tid, cfg, 0, 1, 48);
        FrequentItemsetMiningResult b32 = miner.mineWithStats(tid, cfg, 0, 32, 48);
        assertTrue(b32.getGeneratedCandidateCount() >= b1.getGeneratedCandidateCount());
        assertTrue(b1.getGeneratedCandidateCount() < b32.getGeneratedCandidateCount());
    }

    @Test
    void allCandidates_respectMaxItemsetSize() {
        List<BitSet> tid = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            BitSet b = new BitSet();
            for (int d = 0; d < 30; d++) {
                b.set(d);
            }
            tid.add(b);
        }
        BeamFrequentItemsetMiner miner = new BeamFrequentItemsetMiner();
        int maxLen = 3;
        SelectorConfig cfg = new SelectorConfig(5, 1, maxLen, 50_000);
        FrequentItemsetMiningResult r = miner.mineWithStats(tid, cfg, 0, 64, 96);
        for (CandidateItemset c : r.getCandidates()) {
            assertTrue(c.length() <= maxLen, () -> "len=" + c.length() + " > " + maxLen);
        }
    }

    @Test
    void smallDataset_intersectionCountMatchesExpectedForTwoTermPair() {
        List<BitSet> tid = twoTermPairTidsets();
        BeamFrequentItemsetMiner miner = new BeamFrequentItemsetMiner();
        SelectorConfig cfg = new SelectorConfig(2, 2, 2, 5000);
        FrequentItemsetMiningResult r = miner.mineWithStats(tid, cfg, 0, 16, 32);
        assertEquals(2, r.getFrequentTermCount());
        assertEquals(1, r.getIntersectionCount());
        assertTrue(r.getGeneratedCandidateCount() >= 1);
    }

    @Test
    void constantScoreFunction_beamStillEmitsValidFrequentPairs() {
        List<BitSet> tid = twoTermPairTidsets();
        SelectorConfig cfg = new SelectorConfig(2, 2, 4, 5000);
        FrequentItemsetMiningResult r =
                new BeamFrequentItemsetMiner((len, sup) -> 7).mineWithStats(tid, cfg, 0, 16, 32);
        boolean hasPair = false;
        for (CandidateItemset c : r.getCandidates()) {
            if (c.length() == 2 && c.getSupport() == 2) {
                hasPair = true;
                break;
            }
        }
        assertTrue(hasPair, () -> "constant scorer should still discover {0,1} pair: " + r.getCandidates());
    }

    @Test
    void longValueBiasedScore_shouldNotBePrunedTooEarlyByBeamFrontier() {
        List<BitSet> tid = new ArrayList<>();
        BitSet t0 = new BitSet();
        t0.set(0);
        t0.set(1);
        t0.set(2);
        t0.set(3);
        BitSet t1 = new BitSet();
        t1.set(0);
        t1.set(1);
        t1.set(2);
        BitSet t2 = new BitSet();
        t2.set(0);
        t2.set(1);
        t2.set(2);
        BitSet t3 = new BitSet();
        t3.set(0);
        t3.set(1);
        t3.set(2);
        BitSet distractor = new BitSet();
        for (int d = 0; d < 20; d++) {
            distractor.set(100 + d);
        }
        tid.add(t0);
        tid.add(t1);
        tid.add(t2);
        tid.add(t3);
        tid.add(distractor);

        BeamFrequentItemsetMiner lengthBiasedMiner =
                new BeamFrequentItemsetMiner((len, sup) -> len * 10_000 + sup);
        SelectorConfig cfg = new SelectorConfig(3, 2, 4, 10_000);
        FrequentItemsetMiningResult r = lengthBiasedMiner.mineWithStats(tid, cfg, 0, 8, 1);

        boolean hasLong = false;
        for (CandidateItemset c : r.getCandidates()) {
            if (c.length() == 4 && c.getSupport() == 3) {
                int[] ids = c.getTermIds();
                if (ids[0] == 0 && ids[1] == 1 && ids[2] == 2 && ids[3] == 3) {
                    hasLong = true;
                    break;
                }
            }
        }
        assertTrue(hasLong, () -> "expected long high-value itemset {0,1,2,3}: " + r.getCandidates());
    }

    @Test
    void suffixMaxSupportBound_shouldBeComputedForEarlySupersetPruning() throws Exception {
        BeamFrequentItemsetMiner miner = new BeamFrequentItemsetMiner();
        Method method = BeamFrequentItemsetMiner.class.getDeclaredMethod(
                "buildSuffixMaxSupportByPos",
                int[].class,
                int[].class
        );
        method.setAccessible(true);

        int[] frequentTermIds = new int[] {2, 5, 7};
        int[] supportsByTermId = new int[10];
        supportsByTermId[2] = 6;
        supportsByTermId[5] = 4;
        supportsByTermId[7] = 9;

        int[] suffixMax = (int[]) method.invoke(miner, frequentTermIds, supportsByTermId);
        assertEquals(9, suffixMax[0]);
        assertEquals(9, suffixMax[1]);
        assertEquals(9, suffixMax[2]);
        assertEquals(0, suffixMax[3]);
    }

    @Test
    void softTidsetCache_shouldReduceRepeatedIntersectionWorkAcrossRuns() {
        List<BitSet> tid = new ArrayList<>();
        for (int i = 0; i < 6; i++) {
            BitSet b = new BitSet();
            for (int d = 0; d < 20; d++) {
                b.set(d);
            }
            tid.add(b);
        }
        BeamFrequentItemsetMiner miner = new BeamFrequentItemsetMiner();
        SelectorConfig cfg = new SelectorConfig(8, 2, 5, 20_000);
        String prop = "fptoken.miner.reuseTidsetCacheAcrossCalls";
        String old = System.getProperty(prop);
        try {
            System.setProperty(prop, "true");
            FrequentItemsetMiningResult first = miner.mineWithStats(tid, cfg, 0, 32, 64);
            FrequentItemsetMiningResult second = miner.mineWithStats(tid, cfg, 0, 32, 64);
            assertTrue(second.getIntersectionCount() <= first.getIntersectionCount());
        } finally {
            restoreSystemProperty(prop, old);
        }
    }

    @Test
    void scratchBitSetCapacity_shouldGrowAndShrinkWithExpectedScale() throws Exception {
        BeamFrequentItemsetMiner miner = new BeamFrequentItemsetMiner();
        Method getScratch = BeamFrequentItemsetMiner.class.getDeclaredMethod("getScratchBitSet", int.class);
        getScratch.setAccessible(true);

        getScratch.invoke(miner, 8192);
        int expanded = readScratchCapacityBits(miner);
        assertTrue(expanded >= 8192);

        getScratch.invoke(miner, EngineTuningConfig.MIN_BITSET_CAPACITY);
        int shrunk = readScratchCapacityBits(miner);
        assertTrue(shrunk < expanded);
        assertTrue(shrunk >= EngineTuningConfig.MIN_BITSET_CAPACITY);
    }

    @Test
    void compactPathStack_shouldMaterializePrefixAndAppendTerm() throws Exception {
        BeamFrequentItemsetMiner miner = new BeamFrequentItemsetMiner();
        Class<?> prefixClass = Class.forName(
                "cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.miner.BeamFrequentItemsetMiner$PrefixState"
        );
        java.lang.reflect.Constructor<?> ctor = prefixClass.getDeclaredConstructors()[0];
        ctor.setAccessible(true);

        BitSet b = new BitSet();
        b.set(0);
        Object p0 = ctor.newInstance(null, 3, (BitSet) b.clone(), 0, 1, 1, 1, 1);
        Object p1 = ctor.newInstance(p0, 5, (BitSet) b.clone(), 1, 2, 1, 1, 1);

        Method append = BeamFrequentItemsetMiner.class.getDeclaredMethod("appendTermId", prefixClass, int.class);
        append.setAccessible(true);
        int[] out = (int[]) append.invoke(miner, p1, 8);
        assertEquals(3, out.length);
        assertEquals(3, out[0]);
        assertEquals(5, out[1]);
        assertEquals(8, out[2]);
    }

    private static void restoreSystemProperty(String key, String oldValue) {
        if (oldValue == null) {
            System.clearProperty(key);
        } else {
            System.setProperty(key, oldValue);
        }
    }

    private static int readScratchCapacityBits(BeamFrequentItemsetMiner miner) throws Exception {
        java.lang.reflect.Field threadLocalField =
                BeamFrequentItemsetMiner.class.getDeclaredField("threadLocalScratch");
        threadLocalField.setAccessible(true);
        @SuppressWarnings("unchecked")
        ThreadLocal<Object> local = (ThreadLocal<Object>) threadLocalField.get(miner);
        Object holder = local.get();
        java.lang.reflect.Field capField = holder.getClass().getDeclaredField("capacityBits");
        capField.setAccessible(true);
        return ((Integer) capField.get(holder)).intValue();
    }

    private static BitSet bitsetFromDocIds(int... docIds) {
        BitSet b = new BitSet();
        for (int d : docIds) {
            b.set(d);
        }
        return b;
    }

    private static BitSet bit(int d) {
        BitSet b = new BitSet();
        b.set(d);
        return b;
    }
}

