package cn.lxdb.plugins.muqingyu.fptoken.tests.unit;

import cn.lxdb.plugins.muqingyu.fptoken.tests.ByteArrayTestSupport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import cn.lxdb.plugins.muqingyu.fptoken.model.DocTerms;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

class DocTermsTest {

    @Test
    void nullTerms_throws() {
        assertThrows(NullPointerException.class, () -> new DocTerms(0, null));
    }

    @Test
    void getDocId_roundTrip() {
        DocTerms d = ByteArrayTestSupport.doc(42, new byte[] {1});
        assertEquals(42, d.getDocId());
    }

    @Test
    void normalize_skipsNullEmpty_preservesFirstOccurrenceOrder_dedupes() {
        byte[] a = {1};
        byte[] b = {2};
        List<byte[]> raw = new ArrayList<>();
        raw.add(null);
        raw.add(new byte[0]);
        raw.add(a);
        raw.add(b);
        raw.add(a);
        raw.add(b);
        DocTerms d = new DocTerms(0, raw);
        List<byte[]> out = d.getTerms();
        assertEquals(2, out.size());
        assertTrue(Arrays.equals(a, out.get(0)));
        assertTrue(Arrays.equals(b, out.get(1)));
    }

    @Test
    void normalize_dedupesByContentAcrossDifferentArrayInstances() {
        byte[] a1 = {7, 8};
        byte[] a2 = {7, 8};
        DocTerms d = new DocTerms(1, Arrays.asList(a1, a2));
        assertEquals(1, d.getTerms().size());
        assertTrue(Arrays.equals(a1, d.getTerms().get(0)));
    }

    @Test
    void inputMutationAfterConstruction_doesNotChangeStoredTerms() {
        byte[] t = {3, 4, 5};
        DocTerms d = new DocTerms(1, Arrays.asList(t));
        t[0] = 99;
        assertTrue(Arrays.equals(new byte[] {3, 4, 5}, d.getTerms().get(0)));
    }
}

