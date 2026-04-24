package cn.lxdb.plugins.muqingyu.fptoken.tests.functional;

import cn.lxdb.plugins.muqingyu.fptoken.tests.ByteArrayTestSupport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import cn.lxdb.plugins.muqingyu.fptoken.ExclusiveFrequentItemsetSelector;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.DocTerms;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.ExclusiveSelectionResult;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.SelectedGroup;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * 璁捐鏂囨。銆屽姛鑳?/ 浜掓枼 / 鏁版嵁瀹屾暣鎬?/ 杈圭晫銆嶇敤渚嬬紪鍙?F-001锝濬-018锛團-001 绛夊湪 {@link
 * ExclusiveFrequentItemsetSelectorFunctionalTest} 涓凡鏈夌瓑浠峰疄鐜帮紝姝ゅ琛ュ叏鍏朵綑椤癸級銆?
 */
class ExclusiveFrequentItemsetSelectorDesignDocTest {

    private static final int MAX_C = 80_000;

    @Test
    void f002_singleRecordSingleWindow_repeatedPatterns() {
        byte[] window = ByteArrayTestSupport.hex("010203010203");
        List<DocTerms> rows =
                Collections.singletonList(ByteArrayTestSupport.doc(0, ByteArrayTestSupport.slidingItems123(window)));
        ExclusiveSelectionResult r =
                ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(rows, 1, 1, 6, MAX_C);
        assertFalse(r.getGroups().isEmpty());
        assertTrue(ByteArrayTestSupport.pairwiseTermsDisjoint(r.getGroups()));
    }

    @Test
    void f003_minSupport_filtersRareTerm() {
        byte[] hi = ByteArrayTestSupport.hex("CC");
        byte[] lo = ByteArrayTestSupport.hex("EE");
        List<DocTerms> rows = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            rows.add(ByteArrayTestSupport.doc(i, hi, ByteArrayTestSupport.hex("01")));
        }
        for (int i = 0; i < 3; i++) {
            rows.add(ByteArrayTestSupport.doc(50 + i, lo, ByteArrayTestSupport.hex("02")));
        }
        ExclusiveSelectionResult r =
                ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(rows, 10, 1, 6, MAX_C);
        assertFalse(ByteArrayTestSupport.anyGroupContainsTerm(r.getGroups(), lo));
        for (SelectedGroup g : r.getGroups()) {
            assertTrue(g.getSupport() >= 10);
        }
    }

    @Test
    void f004_itemsetLength_between3and4() {
        byte[] core = ByteArrayTestSupport.hex("AA");
        List<DocTerms> rows = new ArrayList<>();
        for (int i = 0; i < 70; i++) {
            byte[] a = {0x01};
            byte[] b = {0x02};
            byte[] c = {0x03};
            rows.add(ByteArrayTestSupport.doc(i, core, a, b, c));
        }
        ExclusiveSelectionResult r =
                ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(rows, 12, 3, 4, MAX_C);
        assertTrue(ByteArrayTestSupport.allGroupsTermCountInRange(r.getGroups(), 3, 4));
    }

    @Test
    void f005_maxCandidateCount_truncationFlag() {
        List<DocTerms> rows = new ArrayList<>();
        for (int d = 0; d < 120; d++) {
            List<byte[]> terms = new ArrayList<>();
            for (int v = 0; v < 12; v++) {
                terms.add(new byte[] {(byte) (0x20 + v)});
            }
            rows.add(ByteArrayTestSupport.doc(d, terms));
        }
        int cap = 15;
        ExclusiveSelectionResult r =
                ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(rows, 4, 2, 8, cap);
        assertTrue(
                r.isTruncatedByCandidateLimit(),
                () -> "expected mining truncated by candidate cap=" + cap + " count=" + r.getCandidateCount());
        assertTrue(r.getCandidateCount() <= cap);
    }

    @Test
    void f006_disjointTriples_bothMayAppear() {
        byte[] a = {0x01};
        byte[] b = {0x02};
        byte[] c = {0x03};
        byte[] d = {0x04};
        byte[] e = {0x05};
        byte[] f = {0x06};
        List<DocTerms> rows = new ArrayList<>();
        for (int i = 0; i < 28; i++) {
            rows.add(ByteArrayTestSupport.doc(i, a, b, c));
        }
        for (int i = 0; i < 28; i++) {
            rows.add(ByteArrayTestSupport.doc(28 + i, d, e, f));
        }
        ExclusiveSelectionResult r =
                ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(rows, 12, 3, 6, MAX_C);
        assertTrue(ByteArrayTestSupport.pairwiseTermsDisjoint(r.getGroups()));
        assertTrue(
                ByteArrayTestSupport.anyGroupHasExactlyTerms(r.getGroups(), new byte[][] {a, b, c})
                        || ByteArrayTestSupport.anyGroupHasExactlyTerms(r.getGroups(), new byte[][] {d, e, f}),
                () -> "expected at least one full triple in output: " + r.getGroups());
    }

    @Test
    void f007_f009_pairwiseDisjoint_underOverlapAndManyCandidates() {
        byte[] a = {0x0A};
        byte[] b = {0x0B};
        byte[] c = {0x0C};
        List<DocTerms> rows = new ArrayList<>();
        for (int i = 0; i < 25; i++) {
            rows.add(ByteArrayTestSupport.doc(i, a, b, c));
        }
        for (int i = 0; i < 25; i++) {
            rows.add(ByteArrayTestSupport.doc(25 + i, c, ByteArrayTestSupport.hex("DD")));
        }
        ExclusiveSelectionResult r =
                ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(rows, 8, 2, 6, MAX_C);
        assertTrue(ByteArrayTestSupport.pairwiseTermsDisjoint(r.getGroups()));

        List<DocTerms> many = new ArrayList<>();
        for (int i = 0; i < 90; i++) {
            List<byte[]> terms = new ArrayList<>();
            for (int k = 0; k < 8; k++) {
                terms.add(new byte[] {(byte) (0x40 + (i + k) % 8)});
            }
            many.add(ByteArrayTestSupport.doc(i, terms));
        }
        ExclusiveSelectionResult r2 =
                ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(many, 4, 2, 6, MAX_C);
        assertTrue(ByteArrayTestSupport.pairwiseTermsDisjoint(r2.getGroups()));
    }

    @Test
    void f008_longerItemsetPreferredOverSubsetPattern() {
        byte[] a = {0x11};
        byte[] b = {0x12};
        byte[] c = {0x13};
        List<DocTerms> rows = new ArrayList<>();
        for (int i = 0; i < 45; i++) {
            rows.add(ByteArrayTestSupport.doc(i, a, b, c));
        }
        for (int i = 0; i < 45; i++) {
            rows.add(ByteArrayTestSupport.doc(45 + i, a, b));
        }
        ExclusiveSelectionResult r =
                ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(rows, 20, 2, 6, MAX_C);
        boolean hasAbc = ByteArrayTestSupport.anyGroupHasExactlyTerms(r.getGroups(), new byte[][] {a, b, c});
        if (hasAbc) {
            assertFalse(
                    ByteArrayTestSupport.anyGroupHasExactlyTerms(r.getGroups(), new byte[][] {a, b}),
                    "if {A,B,C} selected, {A,B} alone should not appear as a separate group");
        }
        assertTrue(ByteArrayTestSupport.pairwiseTermsDisjoint(r.getGroups()));
    }

    @Test
    void f010_termRoundTrip_bytesMatch() {
        byte[] pat = ByteArrayTestSupport.hex("DEADBEEF");
        List<DocTerms> rows = new ArrayList<>();
        for (int i = 0; i < 35; i++) {
            rows.add(ByteArrayTestSupport.doc(i, pat));
        }
        ExclusiveSelectionResult r =
                ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(rows, 15, 1, 4, MAX_C);
        assertTrue(ByteArrayTestSupport.anyGroupContainsTerm(r.getGroups(), pat));
    }

    @Test
    void f011_docIdsAscending() {
        byte[] t = {0x55};
        List<DocTerms> rows = new ArrayList<>();
        for (int i = 0; i < 30; i++) {
            rows.add(ByteArrayTestSupport.doc(i * 3, t));
        }
        ExclusiveSelectionResult r =
                ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(rows, 5, 1, 4, MAX_C);
        for (SelectedGroup g : r.getGroups()) {
            assertTrue(ByteArrayTestSupport.isSortedAscending(g.getDocIds()), () -> "docIds: " + g.getDocIds());
        }
    }

    @Test
    void f012_supportMatchesDocCount() {
        byte[] t = {0x7E};
        List<DocTerms> rows = new ArrayList<>();
        int n = 37;
        for (int i = 0; i < n; i++) {
            rows.add(ByteArrayTestSupport.doc(i, t));
        }
        ExclusiveSelectionResult r =
                ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(rows, 10, 1, 4, MAX_C);
        for (SelectedGroup g : r.getGroups()) {
            if (g.getTerms().size() == 1 && Arrays.equals(g.getTerms().get(0), t)) {
                assertEquals(n, g.getSupport());
                assertEquals(n, g.getDocIds().size());
            }
        }
    }

    @Test
    void f013_estimatedSavingFormula() {
        byte[] u = {0x3C};
        List<DocTerms> rows = new ArrayList<>();
        for (int i = 0; i < 22; i++) {
            rows.add(ByteArrayTestSupport.doc(i, u, ByteArrayTestSupport.hex("00"), ByteArrayTestSupport.hex("01")));
        }
        ExclusiveSelectionResult r =
                ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(rows, 8, 2, 5, MAX_C);
        for (SelectedGroup g : r.getGroups()) {
            int k = g.getTerms().size();
            int expected = ByteArrayTestSupport.expectedEstimatedSaving(k, g.getSupport());
            assertEquals(expected, g.getEstimatedSaving(), () -> "group: " + g);
        }
    }

    @Test
    void f014_noFrequentItemsets() {
        List<DocTerms> rows = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            rows.add(ByteArrayTestSupport.doc(i, new byte[] {(byte) i}));
        }
        ExclusiveSelectionResult r =
                ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(rows, 100, 1, 6, MAX_C);
        assertTrue(r.getGroups().isEmpty());
    }

    @Test
    void f016_onlyUnigrams() {
        List<DocTerms> rows = new ArrayList<>();
        for (int i = 0; i < 40; i++) {
            rows.add(ByteArrayTestSupport.doc(i, new byte[] {0x05}, new byte[] {0x06}));
        }
        ExclusiveSelectionResult r =
                ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(rows, 10, 1, 4, MAX_C);
        assertTrue(ByteArrayTestSupport.pairwiseTermsDisjoint(r.getGroups()));
    }

    @Test
    void f017_singletonItemsetsAllowed() {
        byte[] x = {0x09};
        List<DocTerms> rows = new ArrayList<>();
        for (int i = 0; i < 25; i++) {
            rows.add(ByteArrayTestSupport.doc(i, x));
        }
        ExclusiveSelectionResult r =
                ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(rows, 8, 1, 6, MAX_C);
        assertTrue(r.getGroups().stream().anyMatch(g -> g.getTerms().size() == 1));
    }

    @Test
    void f018_hugeMaxItemsetSize_noCrash() {
        List<DocTerms> rows = new ArrayList<>();
        for (int i = 0; i < 30; i++) {
            rows.add(ByteArrayTestSupport.doc(i, new byte[] {0x01}, new byte[] {0x02}));
        }
        ExclusiveSelectionResult r =
                ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(rows, 5, 1, 99, MAX_C);
        assertTrue(ByteArrayTestSupport.allGroupsTermCountInRange(r.getGroups(), 1, 99));
    }
}

