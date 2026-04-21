package cn.lxdb.plugins.muqingyu.fptoken.tests.unit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import cn.lxdb.plugins.muqingyu.fptoken.util.ByteArrayUtils;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

class ByteArrayUtilsTest {

    @Test
    void copy_isIndependent() {
        byte[] a = {1, 2, 3};
        byte[] b = ByteArrayUtils.copy(a);
        a[0] = 9;
        assertEquals(1, b[0] & 0xFF);
    }

    @Test
    void copy_null_throwsNpe() {
        assertThrows(NullPointerException.class, () -> ByteArrayUtils.copy(null));
    }

    @Test
    void hash_deterministic() {
        byte[] a = {(byte) 0xFF, 0};
        int h1 = ByteArrayUtils.hash(a);
        int h2 = ByteArrayUtils.hash(ByteArrayUtils.copy(a));
        assertEquals(h1, h2);
    }

    @Test
    void hash_null_throwsNpe() {
        assertThrows(NullPointerException.class, () -> ByteArrayUtils.hash(null));
    }

    @Test
    void compareUnsigned() {
        assertTrue(ByteArrayUtils.compareUnsigned(new byte[] {1}, new byte[] {2}) < 0);
        assertTrue(ByteArrayUtils.compareUnsigned(new byte[] {(byte) 0xFF}, new byte[] {1}) > 0);
        assertEquals(0, ByteArrayUtils.compareUnsigned(new byte[] {1, 2}, new byte[] {1, 2}));
        assertTrue(ByteArrayUtils.compareUnsigned(new byte[] {1, 2}, new byte[] {1, 2, 3}) < 0);
    }

    @Test
    void compareUnsigned_nullArgs_throwNpe() {
        assertThrows(NullPointerException.class, () -> ByteArrayUtils.compareUnsigned(null, new byte[] {1}));
        assertThrows(NullPointerException.class, () -> ByteArrayUtils.compareUnsigned(new byte[] {1}, null));
    }

    @Test
    void toHex() {
        assertEquals("010AFF", ByteArrayUtils.toHex(new byte[] {1, 10, (byte) 0xFF}));
    }

    @Test
    void toHex_emptyArray_returnsEmptyString() {
        assertEquals("", ByteArrayUtils.toHex(new byte[0]));
    }

    @Test
    void toHex_null_throwsNpe() {
        assertThrows(NullPointerException.class, () -> ByteArrayUtils.toHex(null));
    }

    @Test
    void formatTermsHex_emptyAndSingleton() {
        assertTrue(ByteArrayUtils.formatTermsHex(Collections.emptyList()).isEmpty());
        List<String> one = ByteArrayUtils.formatTermsHex(Collections.singletonList(new byte[] {0x0A, 0x0B}));
        assertEquals(1, one.size());
        assertEquals("0A0B", one.get(0));
    }

    @Test
    void formatTermsHex_withNullTerm_throwsNpe() {
        assertThrows(
                NullPointerException.class,
                () -> ByteArrayUtils.formatTermsHex(Collections.singletonList(null)));
    }

    @Test
    void formatTermsHex_nullList_throwsNpe() {
        assertThrows(NullPointerException.class, () -> ByteArrayUtils.formatTermsHex(null));
    }

    @Test
    void normalizeTerms_skipsNullEmpty_dedupOrder() {
        byte[] a = {1};
        byte[] b = {2};
        Set<byte[]> raw = new LinkedHashSet<>();
        raw.add(a);
        raw.add(null);
        raw.add(new byte[0]);
        raw.add(b);
        raw.add(ByteArrayUtils.copy(a));
        List<byte[]> out = ByteArrayUtils.normalizeTerms(raw);
        assertEquals(2, out.size());
        assertArrayEquals(a, out.get(0));
        assertArrayEquals(b, out.get(1));
    }

    @Test
    void normalizeTerms_emptyInput_returnsEmptyList() {
        List<byte[]> out = ByteArrayUtils.normalizeTerms(Collections.emptyList());
        assertTrue(out.isEmpty());
    }

    @Test
    void normalizeTerms_nullCollection_throwsNpe() {
        assertThrows(NullPointerException.class, () -> ByteArrayUtils.normalizeTerms(null));
    }
}

