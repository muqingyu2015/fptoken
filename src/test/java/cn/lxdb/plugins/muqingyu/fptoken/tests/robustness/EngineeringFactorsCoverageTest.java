package cn.lxdb.plugins.muqingyu.fptoken.tests.robustness;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import cn.lxdb.plugins.muqingyu.fptoken.ExclusiveFrequentItemsetSelector;
import cn.lxdb.plugins.muqingyu.fptoken.config.SelectorConfig;
import cn.lxdb.plugins.muqingyu.fptoken.index.TermTidsetIndex;
import cn.lxdb.plugins.muqingyu.fptoken.miner.BeamFrequentItemsetMiner;
import cn.lxdb.plugins.muqingyu.fptoken.model.CandidateItemset;
import cn.lxdb.plugins.muqingyu.fptoken.model.DocTerms;
import cn.lxdb.plugins.muqingyu.fptoken.model.ExclusiveSelectionResult;
import cn.lxdb.plugins.muqingyu.fptoken.picker.GreedyExclusiveItemsetPicker;
import cn.lxdb.plugins.muqingyu.fptoken.picker.TwoPhaseExclusiveItemsetPicker;
import cn.lxdb.plugins.muqingyu.fptoken.tests.ByteArrayTestSupport;
import cn.lxdb.plugins.muqingyu.fptoken.tests.CandidateFixture;
import cn.lxdb.plugins.muqingyu.fptoken.util.ByteArrayUtils;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.Test;

class EngineeringFactorsCoverageTest {

    @Test
    void dataQuality_duplicateRowsWithSameDocId_shouldNotInflateSupport() {
        byte[] token = ByteArrayTestSupport.hex("A1");
        List<DocTerms> rows = new ArrayList<>();
        rows.add(ByteArrayTestSupport.doc(7, token));
        rows.add(ByteArrayTestSupport.doc(7, token));
        rows.add(ByteArrayTestSupport.doc(7, token));
        rows.add(ByteArrayTestSupport.doc(9, token));

        TermTidsetIndex index = TermTidsetIndex.build(rows);
        assertEquals(1, index.getTidsetsByTermIdUnsafe().size());
        assertEquals(2, index.getTidsetsByTermIdUnsafe().get(0).cardinality());
    }

    @Test
    void dataQuality_outOfOrderInput_shouldKeepAscendingDocIdsInOutput() {
        byte[] x = ByteArrayTestSupport.hex("ABCD");
        List<DocTerms> rows = new ArrayList<>();
        rows.add(ByteArrayTestSupport.doc(9, x));
        rows.add(ByteArrayTestSupport.doc(2, x));
        rows.add(ByteArrayTestSupport.doc(5, x));

        ExclusiveSelectionResult result =
                ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(rows, 3, 1, 1, 1000);
        assertFalse(result.getGroups().isEmpty());
        assertEquals(List.of(2, 5, 9), result.getGroups().get(0).getDocIds());
    }

    @Test
    void dataQuality_sparseAndDirtyTerms_shouldNotCrashAndStayBounded() {
        List<DocTerms> rows = new ArrayList<>();
        for (int i = 0; i < 80; i++) {
            List<byte[]> terms = new ArrayList<>();
            terms.add(null);
            terms.add(new byte[0]);
            if (i % 7 == 0) {
                terms.add(ByteArrayTestSupport.hex("CAFE"));
            }
            if (i % 13 == 0) {
                terms.add(new byte[] {(byte) i});
            }
            rows.add(ByteArrayTestSupport.doc(i, terms));
        }

        ExclusiveSelectionResult result =
                ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(rows, 2, 1, 4, 5000);
        assertTrue(result.getCandidateCount() >= 0);
        assertTrue(result.getCandidateCount() <= result.getMaxCandidateCount());
    }

    @Test
    void dataQuality_fullByteRange_shouldHandleBinaryBytesCorrectly() {
        List<DocTerms> rows = new ArrayList<>();
        for (int i = 0; i < 256; i++) {
            rows.add(ByteArrayTestSupport.doc(i, new byte[] {(byte) i}));
        }

        ExclusiveSelectionResult result =
                ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(rows, 1, 1, 1, 2000);
        assertEquals(256, result.getFrequentTermCount());
        assertTrue(ByteArrayTestSupport.anyGroupContainsTerm(result.getGroups(), new byte[] {0x00}));
        assertTrue(ByteArrayTestSupport.anyGroupContainsTerm(result.getGroups(), new byte[] {(byte) 0xFF}));
    }

    @Test
    void algorithmCorrectness_lowerMinSupport_shouldNotReduceCandidateCountOnCanonicalData() {
        List<BitSet> tidsets = new ArrayList<>();
        tidsets.add(bitset(0, 1, 2, 4));
        tidsets.add(bitset(0, 1, 3, 4));
        tidsets.add(bitset(0, 2, 3, 4));
        tidsets.add(bitset(1, 2, 3));

        BeamFrequentItemsetMiner miner = new BeamFrequentItemsetMiner();
        var high = miner.mineWithStats(tidsets, new SelectorConfig(3, 1, 3, 10_000), 0, 256, 256);
        var low = miner.mineWithStats(tidsets, new SelectorConfig(2, 1, 3, 10_000), 0, 256, 256);

        assertFalse(high.isTruncatedByCandidateLimit());
        assertFalse(low.isTruncatedByCandidateLimit());
        assertTrue(low.getGeneratedCandidateCount() >= high.getGeneratedCandidateCount());
    }

    @Test
    void algorithmCorrectness_smallDataset_exactPairsShouldMatchBeamWithWideSearch() {
        byte[] a = ByteArrayTestSupport.hex("AA");
        byte[] b = ByteArrayTestSupport.hex("BB");
        byte[] c = ByteArrayTestSupport.hex("CC");
        List<DocTerms> rows = new ArrayList<>();
        rows.add(ByteArrayTestSupport.doc(0, a, b));
        rows.add(ByteArrayTestSupport.doc(1, a, b, c));
        rows.add(ByteArrayTestSupport.doc(2, a, c));
        rows.add(ByteArrayTestSupport.doc(3, b, c));

        TermTidsetIndex index = TermTidsetIndex.build(rows);
        BeamFrequentItemsetMiner miner = new BeamFrequentItemsetMiner();
        var mining = miner.mineWithStats(
                index.getTidsetsByTermIdUnsafe(),
                new SelectorConfig(2, 2, 2, 10_000),
                0,
                256,
                256);

        Set<String> exactPairs = exactFrequentPairs(index, 2);
        Set<String> minedPairs = minedPairs(mining.getCandidates(), index.getIdToTermUnsafe());
        assertEquals(exactPairs, minedPairs);
    }

    @Test
    void algorithmCorrectness_twoPhaseGapShouldNotBeWorseThanGreedyAgainstBruteforce() {
        List<CandidateItemset> candidates = new ArrayList<>();
        candidates.add(CandidateFixture.itemset(new int[] {0, 1, 2}, 0, 1, 2, 3, 4));
        candidates.add(CandidateFixture.itemset(new int[] {0, 3}, 0, 1, 2, 3, 4));
        candidates.add(CandidateFixture.itemset(new int[] {1, 4}, 0, 1, 2, 3, 4));
        candidates.add(CandidateFixture.itemset(new int[] {2, 5}, 0, 1, 2, 3, 4));
        candidates.add(CandidateFixture.itemset(new int[] {6, 7}, 0, 1, 2, 3, 4));

        GreedyExclusiveItemsetPicker greedy = new GreedyExclusiveItemsetPicker();
        TwoPhaseExclusiveItemsetPicker twoPhase = new TwoPhaseExclusiveItemsetPicker();

        List<CandidateItemset> g = greedy.pick(candidates, 32);
        List<CandidateItemset> t = twoPhase.pick(candidates, 32, 200);
        int optimal = bruteForceBestSaving(candidates);

        int greedyGap = optimal - totalSaving(g);
        int twoGap = optimal - totalSaving(t);
        assertTrue(twoGap <= greedyGap);
    }

    @Test
    void concurrency_completableFutureBatches_shouldMatchSerialResults() {
        List<List<DocTerms>> batches = List.of(
                ByteArrayTestSupport.pcapLikeBatch(8, 64, 16, 8),
                ByteArrayTestSupport.pcapLikeBatch(10, 64, 16, 8),
                ByteArrayTestSupport.pcapLikeBatch(12, 64, 16, 8));

        List<String> serial = new ArrayList<>();
        for (List<DocTerms> batch : batches) {
            serial.add(selectorFingerprint(batch));
        }

        ExecutorService pool = Executors.newFixedThreadPool(3);
        try {
            List<CompletableFuture<String>> futures = new ArrayList<>();
            for (List<DocTerms> batch : batches) {
                futures.add(CompletableFuture.supplyAsync(() -> selectorFingerprint(batch), pool));
            }

            List<String> async = new ArrayList<>();
            for (CompletableFuture<String> future : futures) {
                async.add(future.join());
            }
            assertEquals(serial, async);
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void concurrency_threadPoolReuse_shouldNotCrossContaminateTaskResults() throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            Map<Integer, String> expectedBySeed = new HashMap<>();
            for (int seed = 1; seed <= 4; seed++) {
                expectedBySeed.put(seed, selectorFingerprint(seedRows(seed)));
            }

            List<Future<String>> futures = new ArrayList<>();
            List<Integer> seeds = new ArrayList<>();
            for (int i = 0; i < 20; i++) {
                final int seed = 1 + (i % 4);
                seeds.add(seed);
                futures.add(pool.submit(() -> selectorFingerprint(seedRows(seed))));
            }

            for (int i = 0; i < futures.size(); i++) {
                int seed = seeds.get(i);
                assertEquals(expectedBySeed.get(seed), futures.get(i).get());
            }
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void security_sensitivePayloadShouldNotLeakToErrorMessage() {
        List<DocTerms> rows = Collections.singletonList(
                ByteArrayTestSupport.doc(-1, "PASSWORD=topsecret".getBytes(StandardCharsets.US_ASCII)));

        IllegalArgumentException ex = null;
        try {
            ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(rows, 1, 1, 3, 1000);
        } catch (IllegalArgumentException e) {
            ex = e;
        }
        assertTrue(ex != null);
        String msg = ex.getMessage() == null ? "" : ex.getMessage().toLowerCase();
        assertFalse(msg.contains("password"));
        assertFalse(msg.contains("topsecret"));
    }

    private static String selectorFingerprint(List<DocTerms> rows) {
        ExclusiveSelectionResult result =
                ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(rows, 3, 1, 4, 20_000);
        return ByteArrayTestSupport.groupsFingerprint(result.getGroups());
    }

    private static List<DocTerms> seedRows(int seed) {
        Random random = new Random(seed * 31L + 7L);
        List<DocTerms> rows = new ArrayList<>();
        for (int docId = 0; docId < 30; docId++) {
            List<byte[]> terms = new ArrayList<>();
            terms.add(new byte[] {(byte) (seed & 0xFF)});
            terms.add(new byte[] {(byte) (docId & 0x0F)});
            if ((docId + seed) % 3 == 0) {
                byte[] x = new byte[2];
                random.nextBytes(x);
                terms.add(x);
            }
            rows.add(ByteArrayTestSupport.doc(docId, terms));
        }
        return rows;
    }

    private static Set<String> exactFrequentPairs(TermTidsetIndex index, int minSupport) {
        List<byte[]> idToTerm = index.getIdToTermUnsafe();
        List<BitSet> tidsets = index.getTidsetsByTermIdUnsafe();
        Set<String> out = new HashSet<>();
        for (int i = 0; i < tidsets.size(); i++) {
            for (int j = i + 1; j < tidsets.size(); j++) {
                BitSet inter = (BitSet) tidsets.get(i).clone();
                inter.and(tidsets.get(j));
                if (inter.cardinality() >= minSupport) {
                    out.add(pairKey(idToTerm.get(i), idToTerm.get(j)));
                }
            }
        }
        return out;
    }

    private static Set<String> minedPairs(List<CandidateItemset> candidates, List<byte[]> idToTerm) {
        Set<String> out = new HashSet<>();
        for (CandidateItemset candidate : candidates) {
            if (candidate.length() != 2) {
                continue;
            }
            int[] ids = candidate.getTermIdsUnsafe();
            out.add(pairKey(idToTerm.get(ids[0]), idToTerm.get(ids[1])));
        }
        return out;
    }

    private static String pairKey(byte[] a, byte[] b) {
        String ha = ByteArrayUtils.toHex(a);
        String hb = ByteArrayUtils.toHex(b);
        if (ha.compareTo(hb) <= 0) {
            return ha + "|" + hb;
        }
        return hb + "|" + ha;
    }

    private static int totalSaving(List<CandidateItemset> selected) {
        int sum = 0;
        for (CandidateItemset candidate : selected) {
            sum += candidate.getEstimatedSaving();
        }
        return sum;
    }

    private static int bruteForceBestSaving(List<CandidateItemset> candidates) {
        int n = candidates.size();
        int best = 0;
        for (int mask = 0; mask < (1 << n); mask++) {
            if (!isMutuallyExclusive(mask, candidates)) {
                continue;
            }
            int saving = 0;
            for (int i = 0; i < n; i++) {
                if ((mask & (1 << i)) != 0) {
                    saving += candidates.get(i).getEstimatedSaving();
                }
            }
            if (saving > best) {
                best = saving;
            }
        }
        return best;
    }

    private static boolean isMutuallyExclusive(int mask, List<CandidateItemset> candidates) {
        Set<Integer> used = new HashSet<>();
        for (int i = 0; i < candidates.size(); i++) {
            if ((mask & (1 << i)) == 0) {
                continue;
            }
            int[] termIds = candidates.get(i).getTermIdsUnsafe();
            for (int termId : termIds) {
                if (!used.add(Integer.valueOf(termId))) {
                    return false;
                }
            }
        }
        return true;
    }

    private static BitSet bitset(int... ids) {
        BitSet bitSet = new BitSet();
        for (int id : ids) {
            bitSet.set(id);
        }
        return bitSet;
    }
}
