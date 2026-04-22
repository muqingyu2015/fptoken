package cn.lxdb.plugins.muqingyu.fptoken.tests.unit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import cn.lxdb.plugins.muqingyu.fptoken.ExclusiveFrequentItemsetSelector;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.DocTerms;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.ExclusiveSelectionResult;
import cn.lxdb.plugins.muqingyu.fptoken.tests.ByteArrayTestSupport;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;

class ExclusiveFrequentItemsetSelectorStatsUnitTest {

    @Test
    void emptyRows_echoesMaxCandidateCountAndZeroStats() {
        int maxCandidateCount = 1234;
        ExclusiveSelectionResult r = ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(
                Collections.emptyList(), 2, 1, 6, maxCandidateCount);

        assertNotNull(r);
        assertTrue(r.getGroups().isEmpty());
        assertEquals(0, r.getFrequentTermCount());
        assertEquals(0, r.getCandidateCount());
        assertEquals(0, r.getIntersectionCount());
        assertEquals(maxCandidateCount, r.getMaxCandidateCount());
        assertFalse(r.isTruncatedByCandidateLimit());
    }

    @Test
    void minedResult_neverViolatesBasicStatsInvariants() {
        List<DocTerms> rows = new ArrayList<>();
        byte[] core = ByteArrayTestSupport.hex("ABCD");
        byte[] alt = ByteArrayTestSupport.hex("1234");
        for (int i = 0; i < 40; i++) {
            rows.add(ByteArrayTestSupport.doc(i, core, alt, new byte[] {(byte) (i & 0x07)}));
        }

        ExclusiveSelectionResult r = ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(
                rows, 8, 2, 6, 5000);

        assertTrue(r.getFrequentTermCount() >= 0);
        assertTrue(r.getCandidateCount() >= 0);
        assertTrue(r.getIntersectionCount() >= 0);
        assertTrue(r.getCandidateCount() <= r.getMaxCandidateCount());
        assertTrue(ByteArrayTestSupport.pairwiseTermsDisjoint(r.getGroups()));
    }

    @Test
    void nullRows_threeArgOverload_returnsEmptyAndZeroStats() {
        ExclusiveSelectionResult r = ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(null, 2, 1);
        assertNotNull(r);
        assertTrue(r.getGroups().isEmpty());
        assertEquals(0, r.getFrequentTermCount());
        assertEquals(0, r.getCandidateCount());
        assertEquals(0, r.getIntersectionCount());
    }

    @Test
    void listOnlyOverload_emptyRows_returnsEmptyList() {
        List<DocTerms> rows = Collections.emptyList();
        List<cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.SelectedGroup> groups =
                ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsets(rows, 2, 1);
        assertTrue(groups.isEmpty());
    }

    @Test
    void invalidArguments_propagateFromSelectorConfig() {
        List<DocTerms> rows = Collections.singletonList(ByteArrayTestSupport.doc(0, new byte[] {1}));
        assertThrows(IllegalArgumentException.class,
                () -> ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(rows, 0, 1, 4, 1000));
        assertThrows(IllegalArgumentException.class,
                () -> ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(rows, 1, 0, 4, 1000));
        assertThrows(IllegalArgumentException.class,
                () -> ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(rows, 1, 3, 2, 1000));
        assertThrows(IllegalArgumentException.class,
                () -> ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(rows, 1, 1, 4, 0));
    }

    @Test
    void tinyCandidateLimit_canTriggerTruncation() {
        List<DocTerms> rows = new ArrayList<>();
        for (int i = 0; i < 30; i++) {
            rows.add(ByteArrayTestSupport.doc(i, new byte[] {1}, new byte[] {2}, new byte[] {3}));
        }
        ExclusiveSelectionResult r = ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(
                rows, 5, 1, 5, 1);
        assertTrue(r.getCandidateCount() <= 1);
        assertTrue(r.isTruncatedByCandidateLimit());
        assertEquals(1, r.getMaxCandidateCount());
    }
}
