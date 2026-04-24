package cn.lxdb.plugins.muqingyu.fptoken.tests.unit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.CandidateItemset;
import java.util.BitSet;
import org.junit.jupiter.api.Test;

class CandidateItemsetTest {

    @Test
    void nullConstructorArgs_throwNpe() {
        BitSet bits = new BitSet();
        bits.set(0);
        assertThrows(NullPointerException.class, () -> new CandidateItemset(null, bits));
        assertThrows(NullPointerException.class, () -> new CandidateItemset(new int[] {1}, null));
        assertThrows(NullPointerException.class, () -> new CandidateItemset(null, bits, 1));
    }

    @Test
    void length_and_estimatedSaving() {
        BitSet bits = new BitSet();
        bits.set(0);
        bits.set(2);
        int[] t = {1, 4, 9};
        CandidateItemset c = new CandidateItemset(t, bits);
        assertEquals(3, c.length());
        assertEquals(2, c.getSupport());
        assertEquals((3 - 1) * 2, c.getEstimatedSaving());
    }

    @Test
    void constructorWithExplicitSupport() {
        BitSet bits = new BitSet();
        bits.set(0);
        bits.set(1);
        bits.set(2);
        int[] t = {0, 1};
        CandidateItemset c = new CandidateItemset(t, bits, 2);
        assertEquals(2, c.getSupport());
        assertEquals(2, c.getEstimatedSaving());
    }

    @Test
    void getTermIds_returnsDefensiveCopy() {
        int[] t = {7, 8};
        BitSet b = new BitSet();
        b.set(0);
        CandidateItemset c = new CandidateItemset(t, b);
        t[0] = 99;
        assertEquals(7, c.getTermIds()[0]);
        int[] copy = c.getTermIds();
        copy[0] = 55;
        assertEquals(7, c.getTermIds()[0]);
    }

    @Test
    void getDocBits_returnsDefensiveCopy() {
        BitSet b = new BitSet();
        b.set(1);
        CandidateItemset c = new CandidateItemset(new int[] {0}, b);
        b.set(5);
        assertEquals(false, c.getDocBits().get(5));
        BitSet copy = c.getDocBits();
        copy.set(9);
        assertEquals(false, c.getDocBits().get(9));
    }

    @Test
    void emptyTermIds_lengthZero_estimatedSavingClampedToZero() {
        BitSet b = new BitSet();
        b.set(0);
        CandidateItemset c = new CandidateItemset(new int[0], b, 5);
        assertEquals(0, c.length());
        assertEquals(0, c.getEstimatedSaving());
        assertEquals(5, c.getSupport());
    }

    @Test
    void explicitSupport_mayDifferFromDocBitsCardinality() {
        BitSet bits = new BitSet();
        bits.set(0);
        bits.set(1);
        bits.set(2);
        CandidateItemset c = new CandidateItemset(new int[] {9, 10}, bits, 1);
        assertEquals(1, c.getSupport());
        assertEquals(1, c.getEstimatedSaving());
        assertEquals(3, c.getDocBits().cardinality());
    }

    @Test
    void negativeSupport_keepsEstimatedSavingClampedToZero() {
        BitSet bits = new BitSet();
        bits.set(0);
        CandidateItemset c = new CandidateItemset(new int[] {1, 2, 3}, bits, -5);
        assertEquals(-5, c.getSupport());
        assertEquals(0, c.getEstimatedSaving());
    }

    @Test
    void hugeSupport_estimatedSaving_isCappedToIntMax() {
        BitSet bits = new BitSet();
        bits.set(0);
        CandidateItemset c = new CandidateItemset(new int[] {1, 2, 3}, bits, Integer.MAX_VALUE);
        assertEquals(Integer.MAX_VALUE, c.getEstimatedSaving());
        assertNotEquals(0, c.getEstimatedSaving());
    }

    @Test
    void priorityBoost_defaultsToZero() {
        BitSet bits = new BitSet();
        bits.set(0);
        CandidateItemset c = new CandidateItemset(new int[] {1, 2}, bits, 2);
        assertEquals(0, c.getPriorityBoost());
    }

    @Test
    void withPriorityBoost_shouldReturnBoostedInstance() {
        BitSet bits = new BitSet();
        bits.set(0);
        CandidateItemset c = new CandidateItemset(new int[] {3, 4}, bits, 1);
        CandidateItemset boosted = c.withPriorityBoost(7);
        assertEquals(7, boosted.getPriorityBoost());
        assertEquals(c.getSupport(), boosted.getSupport());
        assertEquals(c.getEstimatedSaving(), boosted.getEstimatedSaving());
        assertEquals(c.length(), boosted.length());
    }

    @Test
    void withPriorityBoost_nonPositive_shouldReturnSameInstance() {
        BitSet bits = new BitSet();
        bits.set(0);
        CandidateItemset c = new CandidateItemset(new int[] {5, 6}, bits, 1);
        assertSame(c, c.withPriorityBoost(0));
        assertSame(c, c.withPriorityBoost(-2));
    }
}

