package cn.lxdb.plugins.muqingyu.fptoken.tests.unit;

import cn.lxdb.plugins.muqingyu.fptoken.tests.CandidateFixture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import cn.lxdb.plugins.muqingyu.fptoken.model.CandidateItemset;
import cn.lxdb.plugins.muqingyu.fptoken.picker.GreedyExclusiveItemsetPicker;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;

/** {@link GreedyExclusiveItemsetPicker}锛氬弬鏁般€佹帓搴忚涔夈€佷簰鏂ャ€乪ffectiveSize銆?*/
class GreedyExclusiveItemsetPickerTest {

    private final GreedyExclusiveItemsetPicker picker = new GreedyExclusiveItemsetPicker();

    @Test
    void nullCandidates_throws() {
        assertThrows(IllegalArgumentException.class, () -> picker.pick(null, 10));
    }

    @Test
    void candidatesContainingNullElement_throwIllegalArgument() {
        List<CandidateItemset> c = new ArrayList<>();
        c.add(CandidateFixture.itemset(new int[] {0}, 0));
        c.add(null);
        assertThrows(IllegalArgumentException.class, () -> picker.pick(c, 8));
    }

    @Test
    void emptyCandidates_returnsEmpty() {
        assertTrue(picker.pick(Collections.emptyList(), 5).isEmpty());
    }

    @Test
    void emptyCandidates_ignoreDictionaryValidationAndReturnEmpty() {
        assertTrue(picker.pick(Collections.emptyList(), 0).isEmpty());
        assertTrue(picker.pick(Collections.emptyList(), -99).isEmpty());
    }

    @Test
    void invalidDictionarySize_throws() {
        List<CandidateItemset> c = Collections.singletonList(CandidateFixture.itemset(new int[] {0}, 0));
        assertThrows(IllegalArgumentException.class, () -> picker.pick(c, 0));
        assertThrows(IllegalArgumentException.class, () -> picker.pick(c, -1));
    }

    @Test
    void dictionarySize_minimumPositive_okWhenOnlyTermZero() {
        List<CandidateItemset> c = Collections.singletonList(CandidateFixture.itemset(new int[] {0}, 0, 1));
        List<CandidateItemset> out = picker.pick(c, 1);
        assertEquals(1, out.size());
    }

    @Test
    void dictionarySize_veryLarge_noFailure() {
        List<CandidateItemset> c = Collections.singletonList(CandidateFixture.itemset(new int[] {3, 9}, 0, 1));
        List<CandidateItemset> out = picker.pick(c, Integer.MAX_VALUE / 2);
        assertEquals(1, out.size());
    }

    @Test
    void dictionarySize_integerMaxValue_noFailureOnTinyInput() {
        List<CandidateItemset> c = Collections.singletonList(CandidateFixture.itemset(new int[] {0}, 0));
        List<CandidateItemset> out = picker.pick(c, Integer.MAX_VALUE);
        assertEquals(1, out.size());
    }

    @Test
    void negativeTermId_throws() {
        List<CandidateItemset> c = Collections.singletonList(CandidateFixture.itemset(new int[] {-1}, 0));
        assertThrows(IllegalArgumentException.class, () -> picker.pick(c, 4));
    }

    @Test
    void mutualExclusion_skipsConflicting() {
        List<CandidateItemset> c = new ArrayList<>();
        c.add(CandidateFixture.itemset(new int[] {0, 1}, 0, 1, 2));
        c.add(CandidateFixture.itemset(new int[] {1, 2}, 0, 1, 2));
        c.add(CandidateFixture.itemset(new int[] {3}, 0, 1));
        List<CandidateItemset> out = picker.pick(c, 8);
        assertTrue(CandidateFixture.mutuallyExclusiveByTermId(out));
        assertEquals(2, out.size());
    }

    @Test
    void sortPrefersLongerFirst() {
        List<CandidateItemset> c = new ArrayList<>();
        c.add(CandidateFixture.itemset(new int[] {0}, 0, 1, 2, 3));
        c.add(CandidateFixture.itemset(new int[] {0, 1}, 0, 1, 2));
        List<CandidateItemset> out = picker.pick(c, 4);
        assertEquals(1, out.size());
        assertEquals(2, out.get(0).length());
    }

    @Test
    void undersizedDictionarySize_stillWorks() {
        List<CandidateItemset> c = Arrays.asList(
                CandidateFixture.itemset(new int[] {0, 5}, 0, 1), CandidateFixture.itemset(new int[] {2}, 0));
        List<CandidateItemset> out = picker.pick(c, 2);
        assertTrue(CandidateFixture.mutuallyExclusiveByTermId(out));
        assertEquals(2, out.size());
    }

    @Test
    void termIdGreaterThanDictionarySize_isHandledByEffectiveBitset() {
        List<CandidateItemset> c = Collections.singletonList(CandidateFixture.itemset(new int[] {99}, 0, 1));
        List<CandidateItemset> out = picker.pick(c, 1);
        assertEquals(1, out.size());
        assertEquals(99, out.get(0).getTermIds()[0]);
    }

    @Test
    void tieBreakFirstTermId_ascending() {
        List<CandidateItemset> c = new ArrayList<>();
        c.add(CandidateFixture.itemset(new int[] {1, 2}, 0, 1));
        c.add(CandidateFixture.itemset(new int[] {0, 2}, 0, 1));
        List<CandidateItemset> out = picker.pick(c, 8);
        assertEquals(1, out.size());
        assertEquals(0, out.get(0).getTermIds()[0]);
    }

    @Test
    void emptyTermIds_itemsetAccepted() {
        List<CandidateItemset> c = new ArrayList<>();
        c.add(CandidateFixture.itemset(new int[] {}, 0));
        c.add(CandidateFixture.itemset(new int[] {0}, 0, 1));
        List<CandidateItemset> out = picker.pick(c, 4);
        assertEquals(2, out.size());
        assertTrue(CandidateFixture.mutuallyExclusiveByTermId(out));
    }

    @Test
    void pick_doesNotMutateInputList() {
        List<CandidateItemset> c = new ArrayList<>();
        c.add(CandidateFixture.itemset(new int[] {2}, 0));
        c.add(CandidateFixture.itemset(new int[] {1}, 0));
        c.add(CandidateFixture.itemset(new int[] {0}, 0));
        List<CandidateItemset> snapshot = new ArrayList<>(c);
        List<CandidateItemset> out = picker.pick(c, 8);
        assertEquals(snapshot.size(), c.size());
        for (int i = 0; i < c.size(); i++) {
            assertSame(snapshot.get(i), c.get(i));
        }
        assertNotSame(c, out);
    }

    @Test
    void tieBreak_supportDescending_whenLengthAndSavingEqual() {
        List<CandidateItemset> c = new ArrayList<>();
        c.add(CandidateFixture.itemset(new int[] {0}, 0, 1, 2));
        c.add(CandidateFixture.itemset(new int[] {1}, 0, 1, 2, 3, 4));
        List<CandidateItemset> out = picker.pick(c, 4);
        assertEquals(2, out.size());
        assertEquals(1, out.get(0).getTermIds()[0]);
        assertEquals(5, out.get(0).getSupport());
        assertEquals(0, out.get(1).getTermIds()[0]);
    }

    @Test
    void sameLength_prefersHigherEstimatedSaving() {
        List<CandidateItemset> c = new ArrayList<>();
        c.add(CandidateFixture.itemset(new int[] {2, 3}, 0, 1));
        c.add(CandidateFixture.itemset(new int[] {0, 1}, 0, 1, 2, 3, 4));
        List<CandidateItemset> out = picker.pick(c, 8);
        assertEquals(2, out.size());
        assertEquals(0, out.get(0).getTermIds()[0]);
        assertEquals(5, out.get(0).getSupport());
        assertEquals(5, out.get(0).getEstimatedSaving());
        assertEquals(2, out.get(1).getSupport());
    }

    @Test
    void singleCandidate_selected() {
        CandidateItemset only = CandidateFixture.itemset(new int[] {7}, 0, 1, 2);
        List<CandidateItemset> out = picker.pick(Collections.singletonList(only), 16);
        assertEquals(1, out.size());
        assertSame(only, out.get(0));
    }

    @Test
    void allPairsShareTerm_takeOne() {
        List<CandidateItemset> c = new ArrayList<>();
        c.add(CandidateFixture.itemset(new int[] {0}, 0));
        c.add(CandidateFixture.itemset(new int[] {0}, 0, 1));
        c.add(CandidateFixture.itemset(new int[] {0}, 0, 1, 2));
        List<CandidateItemset> out = picker.pick(c, 4);
        assertEquals(1, out.size());
        assertEquals(3, out.get(0).getSupport());
    }

    @Test
    void disjointCandidates_allSelected() {
        List<CandidateItemset> c = new ArrayList<>();
        c.add(CandidateFixture.itemset(new int[] {0}, 0, 1));
        c.add(CandidateFixture.itemset(new int[] {2}, 2, 3));
        c.add(CandidateFixture.itemset(new int[] {4, 5}, 4, 5));
        List<CandidateItemset> out = picker.pick(c, 8);
        assertEquals(3, out.size());
        assertTrue(CandidateFixture.mutuallyExclusiveByTermId(out));
    }

    @Test
    void duplicateTermIdsInsideCandidate_isHandled() {
        List<CandidateItemset> c = new ArrayList<>();
        c.add(CandidateFixture.itemset(new int[] {1, 1, 2}, 0, 1));
        c.add(CandidateFixture.itemset(new int[] {2, 3}, 0, 1));
        List<CandidateItemset> out = picker.pick(c, 6);
        assertEquals(1, out.size());
        assertEquals(1, out.get(0).getTermIds()[0]);
    }
}

