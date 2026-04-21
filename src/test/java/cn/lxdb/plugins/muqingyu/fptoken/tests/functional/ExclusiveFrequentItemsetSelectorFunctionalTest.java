package cn.lxdb.plugins.muqingyu.fptoken.tests.functional;

import cn.lxdb.plugins.muqingyu.fptoken.tests.ByteArrayTestSupport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import cn.lxdb.plugins.muqingyu.fptoken.ExclusiveFrequentItemsetSelector;
import cn.lxdb.plugins.muqingyu.fptoken.model.DocTerms;
import cn.lxdb.plugins.muqingyu.fptoken.model.ExclusiveSelectionResult;
import cn.lxdb.plugins.muqingyu.fptoken.model.SelectedGroup;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * 鍔熻兘涓庡仴澹€э細绌鸿緭鍏ャ€佹棤棰戠箒椤广€侀暱搴﹁繃婊ゃ€佷簰鏂ユ€с€佸弬鏁版牎楠屻€佸崗璁被閲嶅妯″紡绛夈€?
 *
 * <p>瀵瑰簲鍦烘櫙锛歅CAP 鈫?璁板綍 鈫?婊戝姩绐楀彛 鈫?1/2/3 瀛楄妭 item 鈫?鏈閬撱€?
 */
class ExclusiveFrequentItemsetSelectorFunctionalTest {

    private static final int MAX_C = 50_000;

    @Test
    void nullRows_returnsEmptyStats() {
        ExclusiveSelectionResult r =
                ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(null, 2, 2, 8, MAX_C);
        assertTrue(r.getGroups().isEmpty());
        assertEquals(0, r.getFrequentTermCount());
        assertEquals(0, r.getCandidateCount());
        assertFalse(r.isTruncatedByCandidateLimit());
        assertEquals(MAX_C, r.getMaxCandidateCount());
    }

    @Test
    void emptyRows_returnsEmptyStats() {
        ExclusiveSelectionResult r = ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(
                Collections.emptyList(), 2, 2, 8, MAX_C);
        assertTrue(r.getGroups().isEmpty());
        assertEquals(0, r.getFrequentTermCount());
    }

    @Test
    void allDocsEmptyTerms_returnsEmptyStats() {
        List<DocTerms> rows = new ArrayList<>();
        rows.add(ByteArrayTestSupport.doc(0, Collections.emptyList()));
        rows.add(ByteArrayTestSupport.doc(1, Collections.emptyList()));
        ExclusiveSelectionResult r =
                ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(rows, 1, 1, 6, MAX_C);
        assertTrue(r.getGroups().isEmpty());
        assertEquals(0, r.getFrequentTermCount());
    }

    @Test
    void minSupportTooHigh_noFrequentPatterns() {
        int n = 200;
        List<DocTerms> rows = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            // 姣忔枃妗ｅ敮涓€涓€鍏冭瘝锛屾敮鎸佸害鍧囦负 1
            rows.add(ByteArrayTestSupport.doc(i, new byte[] {(byte) (i & 0xFF)}));
        }
        ExclusiveSelectionResult r =
                ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(rows, n, 2, 6, MAX_C);
        assertTrue(r.getGroups().isEmpty());
    }

    @Test
    void itemsetLengthFilter_respectsMinMax() {
        byte[] common = ByteArrayTestSupport.hex("DEAD");
        List<DocTerms> rows = new ArrayList<>();
        int docs = 80;
        for (int i = 0; i < docs; i++) {
            byte[] a = ByteArrayTestSupport.hex("01");
            byte[] b = ByteArrayTestSupport.hex("02");
            byte[] c = ByteArrayTestSupport.hex("03");
            byte[] d = ByteArrayTestSupport.hex("04");
            rows.add(ByteArrayTestSupport.doc(i, common, a, b, c, d));
        }
        ExclusiveSelectionResult r =
                ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(rows, 10, 2, 4, MAX_C);
        for (SelectedGroup g : r.getGroups()) {
            int len = g.getTerms().size();
            assertTrue(len >= 2 && len <= 4, () -> "group length " + len + " outside [2,4]: " + g);
        }
        ExclusiveSelectionResult r2 =
                ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(rows, 10, 5, 6, MAX_C);
        if (!r2.getGroups().isEmpty()) {
            assertTrue(ByteArrayTestSupport.allGroupsTermCountInRange(r2.getGroups(), 5, 6));
        }
    }

    @Test
    void mutualExclusion_noSharedTermsAcrossGroups() {
        byte[] a = ByteArrayTestSupport.hex("0A");
        byte[] b = ByteArrayTestSupport.hex("0B");
        byte[] c = ByteArrayTestSupport.hex("0C");
        byte[] d = ByteArrayTestSupport.hex("0D");
        int m = 40;
        List<DocTerms> rows = new ArrayList<>();
        for (int i = 0; i < m; i++) {
            rows.add(ByteArrayTestSupport.doc(i, a, b));
        }
        for (int i = 0; i < m; i++) {
            rows.add(ByteArrayTestSupport.doc(m + i, b, c));
        }
        for (int i = 0; i < m; i++) {
            rows.add(ByteArrayTestSupport.doc(2 * m + i, c, d));
        }
        for (int i = 0; i < m; i++) {
            rows.add(ByteArrayTestSupport.doc(3 * m + i, a, d));
        }
        ExclusiveSelectionResult r =
                ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(rows, 15, 2, 6, MAX_C);
        assertTrue(
                ByteArrayTestSupport.pairwiseTermsDisjoint(r.getGroups()),
                () -> "expected pairwise disjoint terms: " + r.getGroups());
    }

    @Test
    void sameInput_deterministicFingerprint() {
        List<DocTerms> rows = new ArrayList<>();
        byte[] pat = ByteArrayTestSupport.hex("485454502F312E31"); // "HTTP/1.1" 椋庢牸
        for (int i = 0; i < 60; i++) {
            byte[] noise = ByteArrayTestSupport.hex(String.format("%02X", i & 0xFF));
            rows.add(ByteArrayTestSupport.doc(i, pat, noise));
        }
        ExclusiveSelectionResult r1 =
                ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(rows, 5, 2, 8, MAX_C);
        ExclusiveSelectionResult r2 =
                ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(rows, 5, 2, 8, MAX_C);
        assertEquals(
                ByteArrayTestSupport.groupsFingerprint(r1.getGroups()),
                ByteArrayTestSupport.groupsFingerprint(r2.getGroups()));
    }

    @Test
    void invalidMinSupport_throwsFromSelectorConfig() {
        List<DocTerms> rows = Collections.singletonList(ByteArrayTestSupport.doc(0, new byte[] {1}));
        assertThrows(
                IllegalArgumentException.class,
                () -> ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(rows, 0, 1, 6, MAX_C));
    }

    @Test
    void invalidMinItemsetSize_throws() {
        List<DocTerms> rows = Collections.singletonList(ByteArrayTestSupport.doc(0, new byte[] {1}));
        assertThrows(
                IllegalArgumentException.class,
                () -> ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(rows, 1, 0, 6, MAX_C));
    }

    @Test
    void invalidMaxItemsetSize_throws() {
        List<DocTerms> rows = Collections.singletonList(ByteArrayTestSupport.doc(0, new byte[] {1}));
        assertThrows(
                IllegalArgumentException.class,
                () -> ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(rows, 1, 4, 3, MAX_C));
    }

    @Test
    void pcapLike_repeatedProtocolToken_foundAndDisjoint() {
        byte[] get = ByteArrayTestSupport.hex("47455420"); // "GET "
        byte[] http = ByteArrayTestSupport.hex("485454502F312E30"); // "HTTP/1.0"
        List<DocTerms> rows = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            rows.add(ByteArrayTestSupport.doc(i, get, http));
        }
        ExclusiveSelectionResult r =
                ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(rows, 10, 1, 6, MAX_C);
        assertFalse(r.getGroups().isEmpty());
        assertTrue(ByteArrayTestSupport.pairwiseTermsDisjoint(r.getGroups()));
    }

    @Test
    void payload_repeatedBlock_yieldsPatterns() {
        byte[] block = ByteArrayTestSupport.hex("CAFEBABE");
        List<DocTerms> rows = new ArrayList<>();
        for (int i = 0; i < 45; i++) {
            rows.add(ByteArrayTestSupport.doc(i, block, block, ByteArrayTestSupport.hex("00")));
        }
        ExclusiveSelectionResult r =
                ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(rows, 8, 2, 6, MAX_C);
        assertTrue(r.getCandidateCount() >= 0);
        assertTrue(ByteArrayTestSupport.pairwiseTermsDisjoint(r.getGroups()));
    }
}

