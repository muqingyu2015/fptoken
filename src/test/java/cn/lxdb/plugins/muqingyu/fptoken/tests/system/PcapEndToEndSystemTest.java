package cn.lxdb.plugins.muqingyu.fptoken.tests.system;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import cn.lxdb.plugins.muqingyu.fptoken.ExclusiveFrequentItemsetSelector;
import cn.lxdb.plugins.muqingyu.fptoken.model.DocTerms;
import cn.lxdb.plugins.muqingyu.fptoken.model.ExclusiveSelectionResult;
import cn.lxdb.plugins.muqingyu.fptoken.tests.ByteArrayTestSupport;
import java.util.List;
import org.junit.jupiter.api.Test;

class PcapEndToEndSystemTest {

    @Test
    void endToEnd_pcapLikeBatch_isDeterministicAndStable() {
        List<DocTerms> rows = ByteArrayTestSupport.pcapLikeBatch(40, 128, 32, 16);

        ExclusiveSelectionResult run1 = ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(
                rows, 8, 2, 6, 80_000);
        ExclusiveSelectionResult run2 = ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(
                rows, 8, 2, 6, 80_000);

        assertEquals(ByteArrayTestSupport.groupsFingerprint(run1.getGroups()),
                ByteArrayTestSupport.groupsFingerprint(run2.getGroups()));
        assertEquals(run1.getCandidateCount(), run2.getCandidateCount());
        assertEquals(run1.getFrequentTermCount(), run2.getFrequentTermCount());
        assertTrue(ByteArrayTestSupport.pairwiseTermsDisjoint(run1.getGroups()));
    }

    @Test
    void endToEnd_parameterGrid_smallSmoke() {
        List<DocTerms> rows = ByteArrayTestSupport.pcapLikeBatch(25, 96, 24, 12);
        int[][] grid = new int[][] {
            {4, 2, 5, 15_000},
            {6, 2, 6, 20_000},
            {8, 3, 6, 30_000}
        };

        for (int[] p : grid) {
            ExclusiveSelectionResult r = ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(
                    rows, p[0], p[1], p[2], p[3]);
            assertTrue(r.getCandidateCount() <= p[3]);
            assertTrue(ByteArrayTestSupport.pairwiseTermsDisjoint(r.getGroups()));
            assertTrue(ByteArrayTestSupport.allGroupsTermCountInRange(r.getGroups(), p[1], p[2]));
        }
    }

    @Test
    void endToEnd_sparseDocIds_keepAscendingDocIdOrderInGroups() {
        List<DocTerms> rows = List.of(
                ByteArrayTestSupport.doc(7, ByteArrayTestSupport.hex("AA")),
                ByteArrayTestSupport.doc(20, ByteArrayTestSupport.hex("AA")),
                ByteArrayTestSupport.doc(35, ByteArrayTestSupport.hex("AA")));

        ExclusiveSelectionResult r = ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(
                rows, 2, 1, 3, 1000);
        assertTrue(!r.getGroups().isEmpty());
        assertEquals(List.of(7, 20, 35), r.getGroups().get(0).getDocIds());
    }

    @Test
    void endToEnd_tinyCandidateLimit_setsTruncationSignal() {
        List<DocTerms> rows = ByteArrayTestSupport.pcapLikeBatch(30, 96, 24, 12);
        ExclusiveSelectionResult r = ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(
                rows, 4, 1, 5, 1);
        assertTrue(r.isTruncatedByCandidateLimit());
        assertTrue(r.getCandidateCount() <= 1);
    }
}
