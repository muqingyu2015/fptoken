package cn.lxdb.plugins.muqingyu.fptoken.tests.functional;

import cn.lxdb.plugins.muqingyu.fptoken.tests.ByteArrayTestSupport;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import cn.lxdb.plugins.muqingyu.fptoken.ExclusiveFrequentItemsetSelector;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.index.TermTidsetIndex;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.DocTerms;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.ExclusiveSelectionResult;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.SelectedGroup;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

/**
 * 璁捐鏂囨。鍦烘櫙鐢ㄤ緥 S-001锝濻-012锛氬崗璁ā鎷熴€佺獥鍙ｈ竟鐣屻€佹壒娆¤妯°€佸帇缂╁悜鎸囨爣绛夈€?
 */
class PcapScenarioDesignDocTest {

    private static final int MAX_C = 100_000;

    @Test
    void s001_protocolFixedHeader() {
        byte[] get = ByteArrayTestSupport.hex("47455420");
        byte[] http = ByteArrayTestSupport.hex("485454502F312E31");
        List<DocTerms> rows = new ArrayList<>();
        for (int i = 0; i < 55; i++) {
            rows.add(ByteArrayTestSupport.doc(i, get, http));
        }
        ExclusiveSelectionResult r =
                ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(rows, 12, 1, 6, MAX_C);
        assertTrue(ByteArrayTestSupport.anyGroupContainsTerm(r.getGroups(), get)
                || ByteArrayTestSupport.anyGroupContainsTerm(r.getGroups(), http));
        assertTrue(ByteArrayTestSupport.pairwiseTermsDisjoint(r.getGroups()));
    }

    @Test
    void s002_headerWithRandomPayload() {
        byte[] dns = ByteArrayTestSupport.hex("0001000001000000"); // 绠€鍖?DNS 澶撮鏍?
        List<DocTerms> rows = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            byte[] noise = ByteArrayTestSupport.hex(String.format("%04X", i * 7919));
            rows.add(ByteArrayTestSupport.doc(i, dns, noise));
        }
        ExclusiveSelectionResult r =
                ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(rows, 10, 1, 6, MAX_C);
        assertFalse(r.getGroups().isEmpty());
        assertTrue(ByteArrayTestSupport.pairwiseTermsDisjoint(r.getGroups()));
    }

    @Test
    void s003_crossWindowPatternNotInSingleWindowItems() {
        byte[] trigram = ByteArrayTestSupport.hex("AABBCC");
        byte[] w1 = ByteArrayTestSupport.hex("0000AABB");
        byte[] w2 = ByteArrayTestSupport.hex("CC0000");
        assertFalse(termsContainBytes(ByteArrayTestSupport.slidingItems123(w1), trigram));
        assertFalse(termsContainBytes(ByteArrayTestSupport.slidingItems123(w2), trigram));

        List<DocTerms> rows = new ArrayList<>();
        rows.add(ByteArrayTestSupport.doc(0, ByteArrayTestSupport.slidingItems123(w1)));
        rows.add(ByteArrayTestSupport.doc(1, ByteArrayTestSupport.slidingItems123(w2)));
        ExclusiveSelectionResult r =
                ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(rows, 1, 1, 6, MAX_C);
        assertFalse(ByteArrayTestSupport.anyGroupContainsTerm(r.getGroups(), trigram));
    }

    private static boolean termsContainBytes(List<byte[]> terms, byte[] needle) {
        for (byte[] t : terms) {
            if (Arrays.equals(t, needle)) {
                return true;
            }
        }
        return false;
    }

    @Test
    void s004_multiProtocolMix() {
        byte[] http = ByteArrayTestSupport.hex("48545450");
        byte[] dns = ByteArrayTestSupport.hex("0001");
        byte[] syn = ByteArrayTestSupport.hex("02");
        List<DocTerms> rows = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            rows.add(ByteArrayTestSupport.doc(i, http));
        }
        for (int i = 0; i < 20; i++) {
            rows.add(ByteArrayTestSupport.doc(20 + i, dns));
        }
        for (int i = 0; i < 20; i++) {
            rows.add(ByteArrayTestSupport.doc(40 + i, syn));
        }
        ExclusiveSelectionResult r =
                ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(rows, 8, 1, 6, MAX_C);
        assertTrue(ByteArrayTestSupport.pairwiseTermsDisjoint(r.getGroups()));
    }

    @Test
    void s005_repeatedShortPatterns() {
        byte[] a = {0x2F};
        byte[] ab = {0x2F, 0x2F};
        List<DocTerms> rows = new ArrayList<>();
        for (int i = 0; i < 60; i++) {
            rows.add(ByteArrayTestSupport.doc(i, a, ab));
        }
        ExclusiveSelectionResult r =
                ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(rows, 15, 1, 6, MAX_C);
        assertFalse(r.getGroups().isEmpty());
        assertTrue(ByteArrayTestSupport.pairwiseTermsDisjoint(r.getGroups()));
    }

    @Test
    void s006_smallBatch() {
        List<DocTerms> rows = ByteArrayTestSupport.pcapLikeBatch(100, 256, 32, 16);
        ExclusiveSelectionResult r =
                ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(rows, 8, 2, 6, MAX_C);
        assertTrue(ByteArrayTestSupport.pairwiseTermsDisjoint(r.getGroups()));
    }

    @Test
    @Tag("slow")
    @EnabledIfSystemProperty(named = "fptoken.runScaleTests", matches = "true")
    void s007_standardBatch10k() {
        List<DocTerms> rows = ByteArrayTestSupport.pcapLikeBatch(10_000, 1024, 128, 64);
        long t0 = System.nanoTime();
        ExclusiveSelectionResult r =
                ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(rows, 50, 2, 8, MAX_C);
        long ms = (System.nanoTime() - t0) / 1_000_000L;
        System.out.println("[S-007] docs=" + rows.size() + " timeMs=" + ms + " groups=" + r.getGroups().size());
        assertTrue(ByteArrayTestSupport.pairwiseTermsDisjoint(r.getGroups()));
    }

    @Test
    @Tag("slow")
    @EnabledIfSystemProperty(named = "fptoken.runScaleTests", matches = "true")
    void s008_largeBatch50k() {
        List<DocTerms> rows = ByteArrayTestSupport.pcapLikeBatch(50_000, 512, 64, 128);
        ExclusiveSelectionResult r =
                ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(rows, 80, 2, 6, MAX_C);
        assertTrue(ByteArrayTestSupport.pairwiseTermsDisjoint(r.getGroups()));
    }

    @Test
    void s009_denseItemsPerDoc() {
        List<DocTerms> rows = new ArrayList<>();
        for (int d = 0; d < 40; d++) {
            List<byte[]> terms = new ArrayList<>();
            for (int k = 0; k < 55; k++) {
                terms.add(new byte[] {(byte) k, (byte) (d & 0xFF)});
            }
            rows.add(ByteArrayTestSupport.doc(d, terms));
        }
        ExclusiveSelectionResult r =
                ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(rows, 6, 2, 6, MAX_C);
        assertTrue(ByteArrayTestSupport.pairwiseTermsDisjoint(r.getGroups()));
    }

    @Test
    void s010_patternCoverageRatio() {
        byte[] mark = {0x5E};
        int n = 80;
        List<DocTerms> rows = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            rows.add(ByteArrayTestSupport.doc(i, mark, ByteArrayTestSupport.hex("01")));
        }
        ExclusiveSelectionResult r =
                ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(rows, 20, 1, 6, MAX_C);
        Set<Integer> covered = new HashSet<>();
        for (SelectedGroup g : r.getGroups()) {
            covered.addAll(g.getDocIds());
        }
        assertFalse(covered.isEmpty());
        double ratio = covered.size() / (double) n;
        assertTrue(ratio > 0.1, () -> "coverage too low: " + ratio);
    }

    @Test
    void s011_lengthPreferenceWeakCheck() {
        byte[] a = {0x01};
        byte[] b = {0x02};
        byte[] c = {0x03};
        byte[] d = {0x04};
        List<DocTerms> rows = new ArrayList<>();
        for (int i = 0; i < 35; i++) {
            rows.add(ByteArrayTestSupport.doc(i, a, b, c, d));
        }
        ExclusiveSelectionResult r =
                ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(rows, 15, 2, 8, MAX_C);
        int maxLen = r.getGroups().stream().mapToInt(g -> g.getTerms().size()).max().orElse(0);
        assertTrue(maxLen >= 2);
        assertTrue(ByteArrayTestSupport.pairwiseTermsDisjoint(r.getGroups()));
    }

    @Test
    void s012_mutexEnforced() {
        List<DocTerms> rows = ByteArrayTestSupport.pcapLikeBatch(200, 128, 48, 24);
        ExclusiveSelectionResult r =
                ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(rows, 5, 2, 6, MAX_C);
        assertTrue(ByteArrayTestSupport.pairwiseTermsDisjoint(r.getGroups()));
    }

    @Test
    void p001_indexBuildTimingSmoke() {
        List<DocTerms> rows = ByteArrayTestSupport.pcapLikeBatch(500, 512, 64, 32);
        long t0 = System.nanoTime();
        TermTidsetIndex.build(rows);
        long ms = (System.nanoTime() - t0) / 1_000_000L;
        assertTrue(ms < Long.getLong("fptoken.indexBudgetMs", 120_000L), () -> "index build ms=" + ms);
    }
}

