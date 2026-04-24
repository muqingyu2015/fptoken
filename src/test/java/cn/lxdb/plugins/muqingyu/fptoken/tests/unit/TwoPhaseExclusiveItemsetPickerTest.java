package cn.lxdb.plugins.muqingyu.fptoken.tests.unit;

import cn.lxdb.plugins.muqingyu.fptoken.tests.CandidateFixture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.CandidateItemset;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.picker.GreedyExclusiveItemsetPicker;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.picker.TwoPhaseExclusiveItemsetPicker;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;

/** {@link TwoPhaseExclusiveItemsetPicker}锛氬弬鏁般€佷笌璐績涓€鑷存€с€佷簩闃舵鐑熸祴銆佷簰鏂ャ€?*/
class TwoPhaseExclusiveItemsetPickerTest {

    private final TwoPhaseExclusiveItemsetPicker two = new TwoPhaseExclusiveItemsetPicker();
    private final GreedyExclusiveItemsetPicker greedy = new GreedyExclusiveItemsetPicker();

    @Test
    void parameterValidation() {
        assertThrows(IllegalArgumentException.class, () -> two.pick(null, 1, 0));
        assertThrows(
                IllegalArgumentException.class,
                () -> two.pick(Collections.singletonList(CandidateFixture.itemset(new int[] {0}, 0)), 0, 1));
        assertThrows(
                IllegalArgumentException.class,
                () -> two.pick(Collections.singletonList(CandidateFixture.itemset(new int[] {0}, 0)), 1, -1));
        assertThrows(
                IllegalArgumentException.class,
                () -> two.pick(
                        Collections.singletonList(CandidateFixture.itemset(new int[] {0}, 0)),
                        1,
                        1,
                        0,
                        0
                ));
    }

    @Test
    void candidatesContainingNullElement_throwIllegalArgument() {
        List<CandidateItemset> c = new ArrayList<>();
        c.add(CandidateFixture.itemset(new int[] {0}, 0));
        c.add(null);
        assertThrows(IllegalArgumentException.class, () -> two.pick(c, 8, 10));
    }

    @Test
    void emptyCandidates_returnsEmpty() {
        List<CandidateItemset> out = two.pick(Collections.emptyList(), 4, 10);
        assertTrue(out.isEmpty());
    }

    @Test
    void emptyCandidates_stillValidateDictionaryAndSwapTrials() {
        assertThrows(IllegalArgumentException.class, () -> two.pick(Collections.emptyList(), 0, 1));
        assertThrows(IllegalArgumentException.class, () -> two.pick(Collections.emptyList(), 1, -1));
    }

    @Test
    void maxSwapTrials_largeBudget_terminatesAndMutex() {
        List<CandidateItemset> c = new ArrayList<>();
        for (int k = 0; k < 8; k++) {
            c.add(CandidateFixture.itemset(new int[] {k}, 0, 1));
        }
        List<CandidateItemset> t = two.pick(c, 16, 50_000);
        assertTrue(CandidateFixture.mutuallyExclusiveByTermId(t));
        assertFalse(t.isEmpty());
    }

    @Test
    void maxSwapTrials_integerMaxValue_noFailureOnTinyInput() {
        List<CandidateItemset> c = Collections.singletonList(CandidateFixture.itemset(new int[] {0}, 0, 1));
        List<CandidateItemset> out = two.pick(c, 4, Integer.MAX_VALUE);
        assertEquals(1, out.size());
    }

    @Test
    void maxSwapTrialsZero_allowed() {
        List<CandidateItemset> c = Collections.singletonList(CandidateFixture.itemset(new int[] {0}, 0, 1));
        List<CandidateItemset> t = two.pick(c, 4, 0);
        assertEquals(1, t.size());
    }

    @Test
    void zeroSwapTrials_matchesGreedy() {
        List<CandidateItemset> c = new ArrayList<>();
        c.add(CandidateFixture.itemset(new int[] {0, 1}, 0, 1, 2));
        c.add(CandidateFixture.itemset(new int[] {1, 2}, 0, 1));
        c.add(CandidateFixture.itemset(new int[] {3}, 0));
        List<CandidateItemset> g = greedy.pick(c, 8);
        List<CandidateItemset> t = two.pick(c, 8, 0);
        assertTrue(CandidateFixture.samePickOrder(g, t));
    }

    @Test
    void phase2_withBudget_remainsMutex() {
        List<CandidateItemset> c = new ArrayList<>();
        for (int k = 0; k < 16; k++) {
            c.add(CandidateFixture.itemset(new int[] {k}, 0, 1, 2, 3));
        }
        c.add(CandidateFixture.itemset(new int[] {0, 1}, 0, 1, 2));
        List<CandidateItemset> t = two.pick(c, 32, 80);
        assertTrue(CandidateFixture.mutuallyExclusiveByTermId(t));
    }

    @Test
    void maxSwapTrials_respected() {
        List<CandidateItemset> c = new ArrayList<>();
        for (int k = 0; k < 20; k++) {
            c.add(CandidateFixture.itemset(new int[] {k}, 0, 1));
        }
        List<CandidateItemset> t = two.pick(c, 64, 3);
        assertTrue(CandidateFixture.mutuallyExclusiveByTermId(t));
    }

    @Test
    void negativeTermId_throws() {
        List<CandidateItemset> c = Collections.singletonList(CandidateFixture.itemset(new int[] {-1}, 0));
        assertThrows(IllegalArgumentException.class, () -> two.pick(c, 4, 1));
    }

    @Test
    void termIdGreaterThanDictionarySize_isHandledByEffectiveBitset() {
        List<CandidateItemset> c = Collections.singletonList(CandidateFixture.itemset(new int[] {99}, 0));
        List<CandidateItemset> out = two.pick(c, 1, 3);
        assertEquals(1, out.size());
        assertEquals(99, out.get(0).getTermIds()[0]);
    }

    @Test
    void phase2_oneOpt_replacesWeakerIncumbent() {
        CandidateItemset c1 = CandidateFixture.itemset(new int[] {0, 1}, 0, 1);
        CandidateItemset c3 = CandidateFixture.itemset(new int[] {3}, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
        CandidateItemset c2 = CandidateFixture.itemset(new int[] {2}, 0, 1, 2, 3, 4);
        BitSet tiny = new BitSet();
        tiny.set(0);
        CandidateItemset c4 = CandidateFixture.itemset(new int[] {0}, tiny, 3000);

        List<CandidateItemset> c = new ArrayList<>();
        c.add(c1);
        c.add(c3);
        c.add(c2);
        c.add(c4);

        List<CandidateItemset> t = two.pick(c, 8, 20);
        assertTrue(CandidateFixture.mutuallyExclusiveByTermId(t));
        assertEquals(3, t.size());
        assertTrue(t.contains(c4));
        assertTrue(t.contains(c2));
        assertTrue(t.contains(c3));
    }

    @Test
    void maxSwapTrials_one_allowsAtMostOneSuccessfulReplacement() {
        CandidateItemset incumbent = CandidateFixture.itemset(new int[] {0, 1}, 0, 1); // greedy first
        CandidateItemset safe = CandidateFixture.itemset(new int[] {2}, 0, 1, 2, 3, 4);
        CandidateItemset challengerA = CandidateFixture.itemset(new int[] {0}, new BitSet(), 3000);
        CandidateItemset challengerB = CandidateFixture.itemset(new int[] {1}, new BitSet(), 3000);

        List<CandidateItemset> candidates = new ArrayList<>();
        candidates.add(incumbent);
        candidates.add(safe);
        candidates.add(challengerA);
        candidates.add(challengerB);

        List<CandidateItemset> baseline = two.pick(candidates, 8, 0);
        List<CandidateItemset> out = two.pick(candidates, 8, 1);
        assertTrue(CandidateFixture.mutuallyExclusiveByTermId(out));
        int added = 0;
        for (CandidateItemset candidate : out) {
            if (!baseline.contains(candidate)) {
                added++;
            }
        }
        assertTrue(added <= 1);
    }

    @Test
    void estimatedBytesPerTerm_shouldAffectMinNetGainFiltering() {
        // len=2, support=3 -> estimatedSaving=3
        // cost(2B/term)=4 => net=-1 (will be filtered by minNetGain=1)
        // cost(1B/term)=2 => net=1 (can pass minNetGain=1)
        CandidateItemset pair = CandidateFixture.itemset(new int[] {0, 1}, 0, 1, 2);
        CandidateItemset safeSingle = CandidateFixture.itemset(new int[] {2}, 0, 1, 2, 3);
        List<CandidateItemset> candidates = new ArrayList<>();
        candidates.add(pair);
        candidates.add(safeSingle);

        List<CandidateItemset> highCost =
                two.pick(candidates, 8, 0, 1, 2);
        List<CandidateItemset> lowCost =
                two.pick(candidates, 8, 0, 1, 1);

        assertFalse(highCost.contains(pair));
        assertTrue(lowCost.contains(pair));
    }

    @Test
    void coverageRewardPerTerm_shouldPreferLongerCandidate_whenNetGainEqual() {
        // pair: len=2 support=3 => netGain(1B)=1
        CandidateItemset pair = CandidateFixture.itemset(new int[] {0, 1}, 0, 1, 2);
        // single: len=1 support=2 => netGain(1B)=1
        CandidateItemset single = CandidateFixture.itemset(new int[] {2}, 0, 1);
        List<CandidateItemset> candidates = new ArrayList<>();
        candidates.add(single);
        candidates.add(pair);

        List<CandidateItemset> noCoverageBias =
                two.pick(candidates, 8, 10, 0, 1, 0);
        List<CandidateItemset> withCoverageBias =
                two.pick(candidates, 8, 10, 0, 1, 4);

        assertTrue(noCoverageBias.size() >= 1);
        assertTrue(withCoverageBias.size() >= 1);
        assertTrue(withCoverageBias.contains(pair));
    }

    @Test
    void runtimeScoringWeights_shouldAllowDynamicPriorityInjection() {
        CandidateItemset incumbent = CandidateFixture.itemset(new int[] {0, 1}, 0, 1, 2);
        CandidateItemset safe = CandidateFixture.itemset(new int[] {2}, 0, 1, 2, 3);
        CandidateItemset challengerHighSupport = CandidateFixture.itemset(new int[] {0}, 0, 1, 2, 3, 4, 5);
        List<CandidateItemset> candidates = new ArrayList<>();
        candidates.add(incumbent);
        candidates.add(safe);
        candidates.add(challengerHighSupport);

        TwoPhaseExclusiveItemsetPicker.resetRuntimeScoringWeightsToDefaults();
        List<CandidateItemset> baseline = two.pick(candidates, 8, 20, 0, 1, 0);
        assertFalse(baseline.contains(challengerHighSupport));

        try {
            TwoPhaseExclusiveItemsetPicker.setRuntimeScoringWeights(
                    new TwoPhaseExclusiveItemsetPicker.ScoringWeights(0L, 100_000L, 0L, 0L, 0L, 0L)
            );
            List<CandidateItemset> adjusted = two.pick(candidates, 8, 20, 0, 1, 0);
            assertTrue(adjusted.contains(challengerHighSupport));
        } finally {
            TwoPhaseExclusiveItemsetPicker.resetRuntimeScoringWeightsToDefaults();
        }
    }

    @Test
    void zeroSwapTrials_shouldFollowWeightedObjectiveNotLengthBias() {
        CandidateItemset longer = CandidateFixture.itemset(new int[] {0, 1}, 0, 1, 2);
        CandidateItemset higherSupportSingle = CandidateFixture.itemset(new int[] {0}, 0, 1, 2, 3, 4, 5);
        List<CandidateItemset> candidates = new ArrayList<>();
        candidates.add(longer);
        candidates.add(higherSupportSingle);

        List<CandidateItemset> out = two.pick(
                candidates,
                8,
                0,
                0,
                1,
                0,
                new TwoPhaseExclusiveItemsetPicker.ScoringWeights(0L, 1L, 0L, 0L, 0L, 0L)
        );

        assertEquals(1, out.size());
        assertSame(higherSupportSingle, out.get(0));
    }

    @Test
    void greedyBias_exhaustiveSmallUniverse_shouldQuantifyGapAgainstGlobalOptimal() {
        TwoPhaseExclusiveItemsetPicker.ScoringWeights supportOnlyWeights =
                new TwoPhaseExclusiveItemsetPicker.ScoringWeights(0L, 1L, 0L, 0L, 0L, 0L);
        List<CandidateItemset> baseUniverse = new ArrayList<>();
        baseUniverse.add(CandidateFixture.itemset(new int[] {0, 1}, new BitSet(), 6)); // long-first trap
        baseUniverse.add(CandidateFixture.itemset(new int[] {0}, new BitSet(), 4));
        baseUniverse.add(CandidateFixture.itemset(new int[] {1}, new BitSet(), 4));
        baseUniverse.add(CandidateFixture.itemset(new int[] {2, 3}, new BitSet(), 7));
        baseUniverse.add(CandidateFixture.itemset(new int[] {2}, new BitSet(), 3));
        baseUniverse.add(CandidateFixture.itemset(new int[] {3}, new BitSet(), 3));
        baseUniverse.add(CandidateFixture.itemset(new int[] {4}, new BitSet(), 2));
        baseUniverse.add(CandidateFixture.itemset(new int[] {0, 4}, new BitSet(), 5));

        final int dictionarySize = 8;
        int totalCases = 0;
        long totalGreedy = 0L;
        long totalOptimal = 0L;
        int maxGap = 0;
        int greedyWorseCases = 0;

        for (int subsetMask = 1; subsetMask < (1 << baseUniverse.size()); subsetMask++) {
            List<CandidateItemset> candidates = new ArrayList<>();
            for (int i = 0; i < baseUniverse.size(); i++) {
                if (((subsetMask >>> i) & 1) != 0) {
                    candidates.add(baseUniverse.get(i));
                }
            }
            List<CandidateItemset> greedyPick = greedy.pick(candidates, dictionarySize, 1, 0, supportOnlyWeights);
            int greedyScore = sumSupport(greedyPick);
            int optimalScore = bruteForceOptimalSupport(candidates, dictionarySize);
            totalCases++;
            totalGreedy += greedyScore;
            totalOptimal += optimalScore;
            int gap = optimalScore - greedyScore;
            if (gap > maxGap) {
                maxGap = gap;
            }
            if (gap > 0) {
                greedyWorseCases++;
            }
            assertTrue(greedyScore <= optimalScore);
        }

        assertTrue(totalCases > 0);
        assertTrue(greedyWorseCases > 0);
        assertTrue(maxGap >= 2);
        double avgRatio = totalGreedy / (double) totalOptimal;
        assertTrue(avgRatio > 0.0d && avgRatio < 1.0d);
    }

    private static int sumSupport(List<CandidateItemset> picked) {
        int total = 0;
        for (int i = 0; i < picked.size(); i++) {
            total += picked.get(i).getSupport();
        }
        return total;
    }

    private static int bruteForceOptimalSupport(List<CandidateItemset> candidates, int dictionarySize) {
        int n = candidates.size();
        int best = 0;
        for (int mask = 0; mask < (1 << n); mask++) {
            BitSet used = new BitSet(dictionarySize);
            int score = 0;
            boolean valid = true;
            for (int i = 0; i < n; i++) {
                if (((mask >>> i) & 1) == 0) {
                    continue;
                }
                CandidateItemset c = candidates.get(i);
                int[] termIds = c.getTermIdsUnsafe();
                for (int t = 0; t < termIds.length; t++) {
                    int termId = termIds[t];
                    if (used.get(termId)) {
                        valid = false;
                        break;
                    }
                }
                if (!valid) {
                    break;
                }
                for (int t = 0; t < termIds.length; t++) {
                    used.set(termIds[t]);
                }
                score += c.getSupport();
            }
            if (valid && score > best) {
                best = score;
            }
        }
        return best;
    }
}

