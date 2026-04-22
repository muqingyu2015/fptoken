package cn.lxdb.plugins.muqingyu.fptoken.tests.unit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.util.ByteArrayKey;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.util.ByteArrayUtils;
import java.util.Arrays;
import java.util.HashSet;
import org.junit.jupiter.api.Test;

class ByteArrayKeyTest {

    @Test
    void constructor_copiesInput_mutatingOriginalDoesNotChangeKey() {
        byte[] raw = {1, 2, 3};
        ByteArrayKey k = new ByteArrayKey(raw);
        raw[0] = 9;
        assertTrue(Arrays.equals(k.bytes(), new byte[] {1, 2, 3}));
    }

    @Test
    void hashCode_matchesByteArrayUtilsHash() {
        byte[] v = {0x7F, (byte) 0x80, 0};
        ByteArrayKey k = new ByteArrayKey(v);
        assertEquals(ByteArrayUtils.hash(v), k.hashCode());
    }

    @Test
    void equals_reflexive_symmetric_andWorksInHashSet() {
        byte[] a = {1, 2};
        ByteArrayKey k1 = new ByteArrayKey(a);
        ByteArrayKey k2 = new ByteArrayKey(new byte[] {1, 2});
        assertEquals(k1, k1);
        assertEquals(k1, k2);
        assertEquals(k2, k1);
        assertNotEquals(k1, null);
        assertNotEquals(k1, "12");

        HashSet<ByteArrayKey> set = new HashSet<>();
        set.add(k1);
        assertTrue(set.contains(k2));
        assertEquals(1, set.size());
    }

    @Test
    void equals_differentLengthOrContent() {
        assertNotEquals(new ByteArrayKey(new byte[] {1}), new ByteArrayKey(new byte[] {1, 2}));
        assertNotEquals(new ByteArrayKey(new byte[] {1, 2}), new ByteArrayKey(new byte[] {1, 3}));
    }

    @Test
    void compareTo_unsignedByteOrder() {
        ByteArrayKey low = new ByteArrayKey(new byte[] {0x00});
        ByteArrayKey high = new ByteArrayKey(new byte[] {(byte) 0xFF});
        assertTrue(low.compareTo(high) < 0);
        assertTrue(high.compareTo(low) > 0);
        assertEquals(0, new ByteArrayKey(new byte[] {1, 2}).compareTo(new ByteArrayKey(new byte[] {1, 2})));
    }

    @Test
    void compareTo_prefixRule_shorterPrefixFirst() {
        ByteArrayKey shortKey = new ByteArrayKey(new byte[] {0x01, 0x02});
        ByteArrayKey longKey = new ByteArrayKey(new byte[] {0x01, 0x02, 0x03});
        assertTrue(shortKey.compareTo(longKey) < 0);
        assertTrue(longKey.compareTo(shortKey) > 0);
    }

    @Test
    void bytes_returnsInternalReference_mutationIsVisible() {
        ByteArrayKey k = new ByteArrayKey(new byte[] {1, 2, 3});
        byte[] exposed = k.bytes();
        exposed[0] = 9;
        assertEquals(9, k.bytes()[0] & 0xFF);
    }

    @Test
    void constructor_null_throws() {
        assertThrows(NullPointerException.class, () -> new ByteArrayKey(null));
    }

    @Test
    void forLookup_usesCallerArrayWithoutCopy_andUsesProvidedHash() {
        byte[] raw = new byte[] {1, 2, 3};
        ByteArrayKey lookup = ByteArrayKey.forLookup(raw, 123456789);
        assertSame(raw, lookup.bytes());
        assertEquals(123456789, lookup.hashCode());

        raw[0] = 9;
        assertEquals(9, lookup.bytes()[0] & 0xFF);
        assertEquals(new ByteArrayKey(new byte[] {9, 2, 3}), lookup);
    }
}

