package cn.lxdb.plugins.muqingyu.fptoken.tests.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import cn.lxdb.plugins.muqingyu.fptoken.ExclusiveFrequentItemsetSelector;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.config.EngineTuningConfig;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.index.TermTidsetIndex;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.DocTerms;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.SelectedGroup;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.util.ByteArrayUtils;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * Migrated from legacy ad-hoc root tests:
 * HashPerfTest / DebugBuildTest / DebugPerfTest / TwoPhaseIndexTest / PerformanceTest.
 */
class LegacyAdhocRootTestsMigrationTest {

    @Test
    void hashPerfBuildSmoke_runsAcrossScales() {
        int[] docCounts = {300, 600, 1200};
        for (int docCount : docCounts) {
            List<DocTerms> docs = generateHashPerfDocs(docCount, 20, 2000);
            TermTidsetIndex index = TermTidsetIndex.build(docs);
            assertTrue(index.getIdToTermUnsafe().size() > 0);
            assertEquals(index.getIdToTermUnsafe().size(), index.getTidsetsByTermIdUnsafe().size());
        }
    }

    @Test
    void debugBuildScenario_twoPhaseFilterMatchesBuildThenFilter() {
        List<DocTerms> docs = generateDebugBuildDocs(2000);
        int minSupport = 100;

        TermTidsetIndex twoPhaseIndex =
                TermTidsetIndex.buildWithSupportBounds(docs, minSupport, EngineTuningConfig.DEFAULT_MAX_DOC_COVERAGE_RATIO);
        TermTidsetIndex fullIndex = TermTidsetIndex.build(docs);

        Map<String, Integer> expected = filterSupportMap(fullIndex, minSupport);
        Map<String, Integer> actual = supportMap(twoPhaseIndex);
        assertEquals(expected, actual);
    }

    @Test
    void debugPerfScenario_selectorSamplingPath_returnsValidShape() {
        List<DocTerms> docs = generateDebugBuildDocs(2500);
        int minSupport = 120;

        List<SelectedGroup> groups = ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsets(docs, minSupport, 2);
        assertFalse(groups == null);
        for (SelectedGroup group : groups) {
            assertTrue(group.getTerms().size() >= 2);
            assertTrue(group.getSupport() >= minSupport);
        }
    }

    @Test
    void twoPhaseIndexMatrix_matchesOldPipelineBySupportMap() {
        int[][] configs = {
            {800, 20, 800, 40},
            {1200, 25, 1200, 60},
            {1800, 30, 1800, 90}
        };
        for (int[] cfg : configs) {
            List<DocTerms> docs = generateTwoPhaseDocs(cfg[0], cfg[1] - 5, cfg[1] + 5, cfg[2]);
            int minSupport = cfg[3];

            TermTidsetIndex twoPhaseIndex =
                    TermTidsetIndex.buildWithSupportBounds(docs, minSupport, EngineTuningConfig.DEFAULT_MAX_DOC_COVERAGE_RATIO);
            TermTidsetIndex fullIndex = TermTidsetIndex.build(docs);

            Map<String, Integer> expected = filterSupportMap(fullIndex, minSupport);
            Map<String, Integer> actual = supportMap(twoPhaseIndex);
            assertEquals(expected, actual);
        }
    }

    @Test
    void performanceCoverageCacheScenario_cachedValueIsReusedAndCorrect() {
        Map<String, Double> cache = new HashMap<>();
        BitSet a = fixedBits(1, 2, 3, 5);
        BitSet b = fixedBits(3, 5, 8, 13);

        double first = calculateCoverageWithCache(cache, a, b);
        int sizeAfterFirst = cache.size();
        double second = calculateCoverageWithCache(cache, a, b);
        int sizeAfterSecond = cache.size();

        assertEquals(first, second);
        assertEquals(1, sizeAfterFirst);
        assertEquals(sizeAfterFirst, sizeAfterSecond);
    }

    private static List<DocTerms> generateHashPerfDocs(int count, int termsPerDoc, int vocabSize) {
        Random rnd = new Random(42);
        List<DocTerms> docs = new ArrayList<>(count);
        byte[][] vocab = new byte[vocabSize][];
        for (int i = 0; i < vocabSize; i++) {
            vocab[i] = ("term" + i).getBytes();
        }
        for (int docId = 0; docId < count; docId++) {
            Set<byte[]> terms = new LinkedHashSet<>();
            while (terms.size() < termsPerDoc) {
                terms.add(vocab[rnd.nextInt(vocabSize)]);
            }
            docs.add(new DocTerms(docId, new ArrayList<>(terms)));
        }
        return docs;
    }

    private static List<DocTerms> generateDebugBuildDocs(int count) {
        Random rnd = new Random(42);
        List<DocTerms> docs = new ArrayList<>(count);

        byte[][] highFreq = {
            "GET".getBytes(), "HTTP".getBytes(), "200".getBytes(), "Host".getBytes(),
            "User-Agent".getBytes(), "Content-Type".getBytes(), "Accept".getBytes(),
            "Connection".getBytes(), "POST".getBytes(), "404".getBytes()
        };
        byte[][] midFreq = new byte[30][];
        for (int i = 0; i < 30; i++) {
            midFreq[i] = ("MID_" + i).getBytes();
        }

        for (int i = 0; i < count; i++) {
            int termCount = 25 + rnd.nextInt(11);
            List<byte[]> terms = new ArrayList<>(termCount);
            for (byte[] p : highFreq) {
                if (rnd.nextDouble() < 0.30) {
                    terms.add(p);
                }
            }
            for (byte[] p : midFreq) {
                if (rnd.nextDouble() < 0.10) {
                    terms.add(p);
                }
            }
            while (terms.size() < termCount) {
                byte[] t = new byte[2 + rnd.nextInt(4)];
                rnd.nextBytes(t);
                terms.add(t);
            }
            docs.add(new DocTerms(i, terms));
        }
        return docs;
    }

    private static List<DocTerms> generateTwoPhaseDocs(int count, int minTerms, int maxTerms, int vocabTarget) {
        Random rnd = new Random(42);
        List<DocTerms> docs = new ArrayList<>(count);

        byte[][] allPatterns = new byte[vocabTarget][];
        for (int i = 0; i < 20; i++) {
            allPatterns[i] = ("H" + i).getBytes();
        }
        for (int i = 20; i < 200 && i < vocabTarget; i++) {
            allPatterns[i] = ("M" + i).getBytes();
        }
        for (int i = 200; i < vocabTarget; i++) {
            allPatterns[i] = ("L" + i).getBytes();
        }

        for (int i = 0; i < count; i++) {
            int termCount = minTerms + rnd.nextInt(maxTerms - minTerms + 1);
            Set<byte[]> terms = new HashSet<>(termCount);

            for (int j = 0; j < 20 && j < vocabTarget; j++) {
                if (rnd.nextDouble() < 0.50) {
                    terms.add(allPatterns[j]);
                }
            }
            for (int j = 20; j < 200 && j < vocabTarget; j++) {
                if (rnd.nextDouble() < 0.15) {
                    terms.add(allPatterns[j]);
                }
            }
            for (int j = 200; j < vocabTarget; j++) {
                if (rnd.nextDouble() < 0.02) {
                    terms.add(allPatterns[j]);
                }
            }
            while (terms.size() < termCount) {
                terms.add(allPatterns[rnd.nextInt(vocabTarget)]);
            }
            docs.add(new DocTerms(i, new ArrayList<>(terms)));
        }
        return docs;
    }

    private static Map<String, Integer> filterSupportMap(TermTidsetIndex index, int minSupport) {
        Map<String, Integer> out = new HashMap<>();
        List<byte[]> terms = index.getIdToTermUnsafe();
        List<BitSet> tidsets = index.getTidsetsByTermIdUnsafe();
        for (int i = 0; i < terms.size(); i++) {
            int support = tidsets.get(i).cardinality();
            if (support >= minSupport) {
                out.put(ByteArrayUtils.toHex(terms.get(i)), Integer.valueOf(support));
            }
        }
        return out;
    }

    private static Map<String, Integer> supportMap(TermTidsetIndex index) {
        Map<String, Integer> out = new HashMap<>();
        List<byte[]> terms = index.getIdToTermUnsafe();
        List<BitSet> tidsets = index.getTidsetsByTermIdUnsafe();
        for (int i = 0; i < terms.size(); i++) {
            out.put(ByteArrayUtils.toHex(terms.get(i)), Integer.valueOf(tidsets.get(i).cardinality()));
        }
        return out;
    }

    private static BitSet fixedBits(int... values) {
        BitSet bitSet = new BitSet();
        for (int value : values) {
            bitSet.set(value);
        }
        return bitSet;
    }

    private static double calculateCoverageWithCache(Map<String, Double> cache, BitSet bitset1, BitSet bitset2) {
        String key = bitset1.toString() + "::" + bitset2.toString();
        Double cached = cache.get(key);
        if (cached != null) {
            return cached.doubleValue();
        }
        double result = calculateCoverage(bitset1, bitset2);
        cache.put(key, Double.valueOf(result));
        return result;
    }

    private static double calculateCoverage(BitSet bitset1, BitSet bitset2) {
        BitSet intersection = (BitSet) bitset1.clone();
        intersection.and(bitset2);
        int intersectSize = intersection.cardinality();
        int unionSize = bitset1.cardinality() + bitset2.cardinality() - intersectSize;
        return unionSize == 0 ? 0.0 : (double) intersectSize / unionSize;
    }
}
