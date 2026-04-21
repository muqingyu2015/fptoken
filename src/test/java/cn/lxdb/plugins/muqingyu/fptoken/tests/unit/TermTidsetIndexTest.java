package cn.lxdb.plugins.muqingyu.fptoken.tests.unit;

import cn.lxdb.plugins.muqingyu.fptoken.tests.ByteArrayTestSupport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import cn.lxdb.plugins.muqingyu.fptoken.index.TermTidsetIndex;
import cn.lxdb.plugins.muqingyu.fptoken.model.DocTerms;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;

class TermTidsetIndexTest {

    @Test
    void build_nullRows_throwsNpeImmediately() {
        assertThrows(IllegalArgumentException.class, () -> TermTidsetIndex.build(null));
    }

    @Test
    void build_nullRows_throwsNpe() {
        assertThrows(IllegalArgumentException.class, () -> TermTidsetIndex.build(null));
    }

    @Test
    void build_rowListContainsNullDocTerms_throwsNpe() {
        byte[] a = {0x41};
        List<DocTerms> rows = new ArrayList<>();
        rows.add(null);
        rows.add(ByteArrayTestSupport.doc(0, a));
        assertThrows(IllegalArgumentException.class, () -> TermTidsetIndex.build(rows));
    }

    @Test
    void emptyRows_emptyLexicon() {
        TermTidsetIndex idx = TermTidsetIndex.build(Collections.emptyList());
        assertTrue(idx.getIdToTerm().isEmpty());
        assertTrue(idx.getTidsetsByTermId().isEmpty());
    }

    @Test
    void mapping_matchesDocTerms() {
        byte[] t0 = {0x0A};
        byte[] t1 = {0x0B, 0x0C};
        List<DocTerms> rows = new ArrayList<>();
        rows.add(ByteArrayTestSupport.doc(0, t0, t1));
        rows.add(ByteArrayTestSupport.doc(1, t1));
        rows.add(ByteArrayTestSupport.doc(2, t0));
        TermTidsetIndex idx = TermTidsetIndex.build(rows);
        assertEquals(2, idx.getIdToTerm().size());
        int id0 = findTermId(idx, t0);
        int id1 = findTermId(idx, t1);
        BitSet b0 = idx.getTidsetsByTermId().get(id0);
        BitSet b1 = idx.getTidsetsByTermId().get(id1);
        assertEquals(2, b0.cardinality());
        assertTrue(b0.get(0) && b0.get(2));
        assertEquals(2, b1.cardinality());
        assertTrue(b1.get(0) && b1.get(1));
    }

    @Test
    void docIds_nonContiguous_ok() {
        byte[] t = {5};
        List<DocTerms> rows = new ArrayList<>();
        rows.add(ByteArrayTestSupport.doc(10, t));
        rows.add(ByteArrayTestSupport.doc(100, t));
        TermTidsetIndex idx = TermTidsetIndex.build(rows);
        BitSet b = idx.getTidsetsByTermId().get(0);
        assertTrue(b.get(10));
        assertTrue(b.get(100));
        assertEquals(2, b.cardinality());
    }

    @Test
    void singleDoc_twoTerms_AB_bothBitsAtDoc0() {
        byte[] a = {0x41};
        byte[] b = {0x42};
        TermTidsetIndex idx = TermTidsetIndex.build(Collections.singletonList(ByteArrayTestSupport.doc(0, a, b)));
        assertEquals(2, idx.getIdToTerm().size());
        int idA = findTermId(idx, a);
        int idB = findTermId(idx, b);
        assertTrue(idx.getTidsetsByTermId().get(idA).get(0));
        assertTrue(idx.getTidsetsByTermId().get(idB).get(0));
        assertEquals(1, idx.getTidsetsByTermId().get(idA).cardinality());
        assertEquals(1, idx.getTidsetsByTermId().get(idB).cardinality());
    }

    @Test
    void twoDocs_crossTerms_AB_AC_bitPattern() {
        byte[] a = {0x41};
        byte[] b = {0x42};
        byte[] c = {0x43};
        List<DocTerms> rows = new ArrayList<>();
        rows.add(ByteArrayTestSupport.doc(0, a, b));
        rows.add(ByteArrayTestSupport.doc(1, a, c));
        TermTidsetIndex idx = TermTidsetIndex.build(rows);
        assertEquals(3, idx.getIdToTerm().size());
        int idA = findTermId(idx, a);
        int idB = findTermId(idx, b);
        int idC = findTermId(idx, c);
        BitSet ba = idx.getTidsetsByTermId().get(idA);
        BitSet bb = idx.getTidsetsByTermId().get(idB);
        BitSet bc = idx.getTidsetsByTermId().get(idC);
        assertTrue(ba.get(0) && ba.get(1));
        assertTrue(bb.get(0) && !bb.get(1));
        assertTrue(!bc.get(0) && bc.get(1));
    }

    @Test
    void singleDoc_duplicateTermContent_normalizedToOneHitPerDoc() {
        byte[] a = {0x41};
        byte[] b = {0x42};
        TermTidsetIndex idx = TermTidsetIndex.build(Collections.singletonList(ByteArrayTestSupport.doc(0, a, b, a)));
        assertEquals(2, idx.getIdToTerm().size());
        assertEquals(1, idx.getTidsetsByTermId().get(0).cardinality());
        assertEquals(1, idx.getTidsetsByTermId().get(1).cardinality());
    }

    @Test
    void singleDoc_oneTerm_oneBit() {
        byte[] t = {9};
        TermTidsetIndex idx = TermTidsetIndex.build(Collections.singletonList(ByteArrayTestSupport.doc(0, t)));
        assertEquals(1, idx.getIdToTerm().size());
        assertTrue(Arrays.equals(t, idx.getIdToTerm().get(0)));
        assertEquals(1, idx.getTidsetsByTermId().get(0).cardinality());
        assertTrue(idx.getTidsetsByTermId().get(0).get(0));
    }

    @Test
    void termIds_followFirstOccurrenceAcrossRows() {
        byte[] first = {1};
        byte[] second = {2};
        List<DocTerms> rows = new ArrayList<>();
        rows.add(ByteArrayTestSupport.doc(0, second));
        rows.add(ByteArrayTestSupport.doc(1, first));
        TermTidsetIndex idx = TermTidsetIndex.build(rows);
        assertEquals(2, idx.getIdToTerm().size());
        assertTrue(Arrays.equals(second, idx.getIdToTerm().get(0)));
        assertTrue(Arrays.equals(first, idx.getIdToTerm().get(1)));
    }

    @Test
    void distinctArraysSameContent_mapToSingleTermId() {
        byte[] a = {1, 2, 3};
        byte[] b = {1, 2, 3};
        List<DocTerms> rows = new ArrayList<>();
        rows.add(ByteArrayTestSupport.doc(0, a));
        rows.add(ByteArrayTestSupport.doc(1, b));
        TermTidsetIndex idx = TermTidsetIndex.build(rows);
        assertEquals(1, idx.getIdToTerm().size());
        assertEquals(2, idx.getTidsetsByTermId().get(0).cardinality());
    }

    @Test
    void sameTermRepeatedInOneDoc_singleSetBit() {
        byte[] t = {7};
        TermTidsetIndex idx = TermTidsetIndex.build(
                Collections.singletonList(ByteArrayTestSupport.doc(3, t, t, t)));
        assertEquals(1, idx.getIdToTerm().size());
        BitSet b = idx.getTidsetsByTermId().get(0);
        assertEquals(1, b.cardinality());
        assertTrue(b.get(3));
    }

    @Test
    void nullAndEmptyTerms_inDocAreSkipped() {
        List<byte[]> terms = new ArrayList<>();
        terms.add(null);
        terms.add(new byte[0]);
        terms.add(new byte[] {0x55});
        terms.add(new byte[] {0x55});
        TermTidsetIndex idx = TermTidsetIndex.build(Collections.singletonList(ByteArrayTestSupport.doc(9, terms)));
        assertEquals(1, idx.getIdToTerm().size());
        assertEquals(1, idx.getTidsetsByTermId().size());
        assertTrue(idx.getTidsetsByTermId().get(0).get(9));
    }

    @Test
    void negativeDocId_inRows_throwsIllegalArgument() {
        List<DocTerms> rows = Collections.singletonList(ByteArrayTestSupport.doc(-1, new byte[] {0x11}));
        assertThrows(IllegalArgumentException.class, () -> TermTidsetIndex.build(rows));
    }

    @Test
    void getters_returnDefensiveCopies() {
        List<DocTerms> rows = Collections.singletonList(ByteArrayTestSupport.doc(0, new byte[] {0x22}));
        TermTidsetIndex idx = TermTidsetIndex.build(rows);

        List<byte[]> idToTerm = idx.getIdToTerm();
        List<BitSet> tidsets = idx.getTidsetsByTermId();
        idToTerm.add(new byte[] {0x33});
        tidsets.add(new BitSet());
        assertEquals(1, idx.getIdToTerm().size());
        assertEquals(1, idx.getTidsetsByTermId().size());

        List<byte[]> idToTermCopy = idx.getIdToTerm();
        idToTermCopy.get(0)[0] = 0x77;
        assertEquals(0x22, idx.getIdToTerm().get(0)[0] & 0xFF);

        List<BitSet> tidsetsCopy = idx.getTidsetsByTermId();
        tidsetsCopy.get(0).clear(0);
        assertTrue(idx.getTidsetsByTermId().get(0).get(0));
    }

    private static int findTermId(TermTidsetIndex idx, byte[] term) {
        List<byte[]> dict = idx.getIdToTerm();
        for (int i = 0; i < dict.size(); i++) {
            if (java.util.Arrays.equals(dict.get(i), term)) {
                return i;
            }
        }
        throw new AssertionError("term not found");
    }
}

