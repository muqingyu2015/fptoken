package cn.lxdb.plugins.muqingyu.fptoken.tests.unit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import cn.lxdb.plugins.muqingyu.fptoken.model.CandidateItemset;
import cn.lxdb.plugins.muqingyu.fptoken.model.ExclusiveSelectionResult;
import cn.lxdb.plugins.muqingyu.fptoken.model.FrequentItemsetMiningResult;
import cn.lxdb.plugins.muqingyu.fptoken.model.SelectedGroup;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;

/** P2 缁撴灉 / 璇嶇粍 DTO锛氬瓧娈甸€忎紶涓庡紩鐢ㄨ涔夛紙涓庣敓浜х被琛屼负涓€鑷达級銆?*/
class ModelResultsTest {

    @Test
    void frequentItemsetMiningResult_getters() {
        List<CandidateItemset> c = Collections.emptyList();
        FrequentItemsetMiningResult r = new FrequentItemsetMiningResult(c, 7, 0, 11, true);
        assertNotSame(c, r.getCandidates());
        assertEquals(7, r.getFrequentTermCount());
        assertEquals(0, r.getGeneratedCandidateCount());
        assertEquals(11, r.getIntersectionCount());
        assertTrue(r.isTruncatedByCandidateLimit());
        assertThrows(UnsupportedOperationException.class, () -> r.getCandidates().add(null));
    }

    @Test
    void exclusiveSelectionResult_getters() {
        List<SelectedGroup> g = Collections.emptyList();
        ExclusiveSelectionResult r = new ExclusiveSelectionResult(g, 3, 100, 5, 500_000, false);
        assertNotSame(g, r.getGroups());
        assertEquals(3, r.getFrequentTermCount());
        assertEquals(100, r.getCandidateCount());
        assertEquals(5, r.getIntersectionCount());
        assertEquals(500_000, r.getMaxCandidateCount());
        assertFalse(r.isTruncatedByCandidateLimit());
        assertThrows(UnsupportedOperationException.class, () -> r.getGroups().add(null));
    }

    @Test
    void selectedGroup_getters_andToString() {
        byte[] t = {0x0A, 0x0B};
        List<byte[]> terms = new ArrayList<>();
        terms.add(t);
        List<Integer> docs = Arrays.asList(1, 2, 3);
        SelectedGroup g = new SelectedGroup(terms, docs, 3, 4);
        assertNotSame(terms, g.getTerms());
        assertNotSame(docs, g.getDocIds());
        assertEquals(3, g.getSupport());
        assertEquals(4, g.getEstimatedSaving());
        terms.get(0)[0] = 0x55;
        assertEquals(0x0A, g.getTerms().get(0)[0]);
        String s = g.toString();
        assertTrue(s.contains("support=3"));
        assertTrue(s.contains("estimatedSaving=4"));
        assertTrue(s.contains("docIds="));
        assertTrue(s.contains("0A0B"));
    }

    @Test
    void resultModels_allowNegativeCounters_roundTripAsIs() {
        FrequentItemsetMiningResult mining = new FrequentItemsetMiningResult(
                Collections.emptyList(), -1, -2, -3, false);
        assertEquals(-1, mining.getFrequentTermCount());
        assertEquals(-2, mining.getGeneratedCandidateCount());
        assertEquals(-3, mining.getIntersectionCount());

        ExclusiveSelectionResult selection = new ExclusiveSelectionResult(
                Collections.emptyList(), -4, -5, -6, -7, true);
        assertEquals(-4, selection.getFrequentTermCount());
        assertEquals(-5, selection.getCandidateCount());
        assertEquals(-6, selection.getIntersectionCount());
        assertEquals(-7, selection.getMaxCandidateCount());
        assertTrue(selection.isTruncatedByCandidateLimit());
    }
}

