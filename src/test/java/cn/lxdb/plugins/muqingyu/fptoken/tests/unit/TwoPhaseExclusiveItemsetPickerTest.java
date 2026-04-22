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

        List<CandidateItemset> g = greedy.pick(c, 8);
        assertSame(c1, g.get(0));

        List<CandidateItemset> t = two.pick(c, 8, 20);
        assertTrue(CandidateFixture.mutuallyExclusiveByTermId(t));
        assertEquals(3, t.size());
        assertSame(c4, t.get(0));
        assertEquals(3000, t.get(0).getSupport());
        assertFalse(CandidateFixture.sameContent(g.get(0), t.get(0)));
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

        List<CandidateItemset> out = two.pick(candidates, 8, 1);
        assertTrue(CandidateFixture.mutuallyExclusiveByTermId(out));
        assertTrue(out.contains(challengerA));
        assertFalse(out.contains(challengerB));
    }
}

