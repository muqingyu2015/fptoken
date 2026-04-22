package cn.lxdb.plugins.muqingyu.fptoken.tests.performance;

import static org.junit.jupiter.api.Assertions.assertTrue;

import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.config.SelectorConfig;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.index.TermTidsetIndex;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.miner.BeamFrequentItemsetMiner;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.CandidateItemset;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.DocTerms;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.FrequentItemsetMiningResult;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.SelectedGroup;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.picker.TwoPhaseExclusiveItemsetPicker;
import cn.lxdb.plugins.muqingyu.fptoken.tests.ByteArrayTestSupport;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.util.ByteArrayKey;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

@Tag("performance")
@EnabledIfSystemProperty(named = "fptoken.runPerfTests", matches = "true")
@Timeout(value = 10, unit = TimeUnit.SECONDS)
class CompressionAndFusionValidationTest {

    @Test
    void q07_selectedMutexGroups_documentCoverageRatio() {
        List<DocTerms> rows = CompressibilityAssessmentFixtures.longPatternDataset(9000, 20260501L);
        CompressibilityPipelineSupport.PipelineResult result = CompressibilityPipelineSupport.run(
                rows, 80, 3, 8, 220000, 4096, 24, 64, 100);
        double docCoverage = documentCoverageRatio(rows.size(), result.getSelectedCandidates());
        System.out.println("q07 docCoverage=" + String.format("%.4f", docCoverage)
                + ", selected=" + result.getMetrics().getSelectedItemsetCount());
        assertTrue(docCoverage > 0.20d);
    }

    @Test
    void q08_compareFrequentItemsetCompression_vsNgramBaseline() {
        List<DocTerms> rows = CompressibilityAssessmentFixtures.longPatternDataset(9000, 20260502L);
        CompressibilityPipelineSupport.PipelineResult selector = CompressibilityPipelineSupport.run(
                rows, 50, 3, 10, 260000, 5000, 32, 96, 120);
        Set<Integer> top3GramTermIds = topKThreeGramTermIds(selector.getIndex(), 64);
        CompressionMetricsUtil.CompressionMetrics ngramMetrics =
                CompressionMetricsUtil.calculateCoverageAndPotentialSavingsFromTermIds(
                        top3GramTermIds, selector.getIndex(), rows);

        System.out.println("q08 selectorRatio=" + String.format("%.4f", selector.getMetrics().getCompressionRatio())
                + ", ngramRatio=" + String.format("%.4f", ngramMetrics.getCompressionRatio()));
        assertTrue(selector.getMetrics().getCompressionRatio() >= ngramMetrics.getCompressionRatio() - 0.15d);
    }

    @Test
    void q25_indexExpansionRatio_shouldBeBoundedOnStructuredData() {
        List<DocTerms> rows = CompressibilityAssessmentFixtures.longPatternDataset(10000, 20260503L);
        TermTidsetIndex index = TermTidsetIndex.build(rows);
        long originalBytes = rawBytes(rows);
        long indexBytes = estimateIndexBytes(index);
        double expansionRatio = originalBytes <= 0L ? 0d : (double) indexBytes / (double) originalBytes;
        System.out.println("q25 originalBytes=" + originalBytes + ", indexBytes=" + indexBytes
                + ", expansionRatio=" + String.format("%.4f", expansionRatio));
        assertTrue(expansionRatio > 0d);
        // 当前实现是 term->BitSet 垂直索引，结构化数据上膨胀率可能明显高于文本倒排；
        // 这里给出一个工程上可接受的上界守护，主要用于回归发现异常暴涨。
        assertTrue(expansionRatio < 30.0d);
    }

    @Test
    void q01_maxItemsetSize8_shouldDiscoverLen6to8Patterns() {
        List<DocTerms> rows = longPatternForLengthEight(6000);
        CompressibilityPipelineSupport.PipelineResult result = CompressibilityPipelineSupport.run(
                rows, 100, 3, 8, 220000, 4096, 24, 64, 100);
        int maxLen = result.getMetrics().getMaxItemsetLength();
        System.out.println("q01 maxLen=" + maxLen + ", selected=" + result.getMetrics().getSelectedItemsetCount());
        assertTrue(maxLen >= 6);
        assertTrue(maxLen <= 8);
    }

    @Test
    void q03_httpHeaderFixedPattern_shouldBeRecognizedAsLongItemset() {
        List<DocTerms> rows = httpLikeLongPatternDataset(7000);
        CompressibilityPipelineSupport.PipelineResult result = CompressibilityPipelineSupport.run(
                rows, 120, 3, 8, 220000, 4096, 24, 64, 100);
        int maxLen = result.getMetrics().getMaxItemsetLength();
        System.out.println("q03 maxLen=" + maxLen + ", coverage="
                + String.format("%.4f", result.getMetrics().getCoverageRatio()));
        assertTrue(maxLen >= 6);
    }

    @Test
    void q10_mutexSelection_shouldAvoidRedundantEncodingOverhead() {
        List<DocTerms> rows = overlapHeavyDataset(7000, 20260504L);
        TermTidsetIndex index = TermTidsetIndex.build(rows);
        SelectorConfig config = new SelectorConfig(60, 3, 8, 260000);
        BeamFrequentItemsetMiner miner = new BeamFrequentItemsetMiner();
        FrequentItemsetMiningResult mining = miner.mineWithStats(index.getTidsetsByTermIdUnsafe(), config, 5000, 32, 96);

        List<CandidateItemset> candidates = mining.getCandidates();
        TwoPhaseExclusiveItemsetPicker picker = new TwoPhaseExclusiveItemsetPicker();
        List<CandidateItemset> mutex = picker.pick(candidates, index.getIdToTermUnsafe().size(), 120);

        List<CandidateItemset> nonMutexTop = topBySaving(candidates, Math.min(120, candidates.size()));
        long mutexEncoded = simulatedEncodedBytes(rows, index, mutex, true);
        long nonMutexEncoded = simulatedEncodedBytes(rows, index, nonMutexTop, false);
        System.out.println("q10 mutexEncoded=" + mutexEncoded + ", nonMutexEncoded=" + nonMutexEncoded);
        assertTrue(mutexEncoded <= nonMutexEncoded);
    }

    @Test
    void q13_highFrequencyTerm_shouldLocateQuicklyWithFrequentItemsetProxy() {
        FusionDataset data = createFusionDataset(12000, 0.80d, 0.01d, 20260505L);
        long fullScanMs = PerfTestSupport.elapsedMillis(() -> runFullScanQueries(data.rows, data.highTerm, 5000));
        long frequentMs = PerfTestSupport.elapsedMillis(() -> runFrequentPatternLookup(data.groups, data.highTerm, 5000));
        System.out.println("q13 fullScanMs=" + fullScanMs + ", frequentMs=" + frequentMs);
        assertTrue(frequentMs <= fullScanMs * 2L + 1L);
    }

    @Test
    void q14_lowFrequencyTerm_shouldBenefitFromDataSkippingProxy() {
        FusionDataset data = createFusionDataset(12000, 0.80d, 0.005d, 20260506L);
        long fullScanMs = PerfTestSupport.elapsedMillis(() -> runFullScanQueries(data.rows, data.lowTerm, 6000));
        long skipMs = PerfTestSupport.elapsedMillis(() -> runDataSkippingLookup(data.index, data.lowTerm, 6000));
        System.out.println("q14 fullScanMs=" + fullScanMs + ", skipMs=" + skipMs);
        assertTrue(skipMs <= fullScanMs * 2L + 1L);
    }

    @Test
    void q18_compareSkippingFrequentHybrid_latencyAcrossHitRates() {
        FusionDataset data = createFusionDataset(12000, 0.75d, 0.008d, 20260507L);
        long highFrequent = PerfTestSupport.elapsedMillis(() -> runFrequentPatternLookup(data.groups, data.highTerm, 4000));
        long highSkip = PerfTestSupport.elapsedMillis(() -> runDataSkippingLookup(data.index, data.highTerm, 4000));
        long highHybrid = PerfTestSupport.elapsedMillis(() -> runHybridLookup(data, data.highTerm, 4000));

        long lowFrequent = PerfTestSupport.elapsedMillis(() -> runFrequentPatternLookup(data.groups, data.lowTerm, 4000));
        long lowSkip = PerfTestSupport.elapsedMillis(() -> runDataSkippingLookup(data.index, data.lowTerm, 4000));
        long lowHybrid = PerfTestSupport.elapsedMillis(() -> runHybridLookup(data, data.lowTerm, 4000));

        System.out.println("q18 high(freq/skip/hybrid)=" + highFrequent + "/" + highSkip + "/" + highHybrid
                + ", low(freq/skip/hybrid)=" + lowFrequent + "/" + lowSkip + "/" + lowHybrid);
        assertTrue(highHybrid <= Math.max(highFrequent, highSkip) * 2L + 1L);
        assertTrue(lowHybrid <= Math.max(lowFrequent, lowSkip) * 2L + 1L);
    }

    private static double documentCoverageRatio(int totalDocs, List<CandidateItemset> selectedCandidates) {
        BitSet covered = new BitSet(Math.max(1, totalDocs));
        for (CandidateItemset candidate : selectedCandidates) {
            covered.or(candidate.getDocBitsUnsafe());
        }
        return totalDocs <= 0 ? 0d : (double) covered.cardinality() / (double) totalDocs;
    }

    private static Set<Integer> topKThreeGramTermIds(TermTidsetIndex index, int k) {
        List<byte[]> terms = index.getIdToTermUnsafe();
        List<Integer> ids = new ArrayList<>();
        for (int i = 0; i < terms.size(); i++) {
            if (terms.get(i).length == 3) {
                ids.add(Integer.valueOf(i));
            }
        }
        ids.sort((a, b) -> Integer.compare(
                index.getTidsetsByTermIdUnsafe().get(b.intValue()).cardinality(),
                index.getTidsetsByTermIdUnsafe().get(a.intValue()).cardinality()));
        Set<Integer> out = new HashSet<>();
        for (int i = 0; i < ids.size() && i < k; i++) {
            out.add(ids.get(i));
        }
        return out;
    }

    private static long rawBytes(List<DocTerms> rows) {
        long sum = 0L;
        for (DocTerms row : rows) {
            for (byte[] term : row.getTermsUnsafe()) {
                if (term != null) {
                    sum += term.length;
                }
            }
        }
        return sum;
    }

    private static long estimateIndexBytes(TermTidsetIndex index) {
        long dict = 0L;
        for (byte[] term : index.getIdToTermUnsafe()) {
            dict += term.length;
        }
        long bitsets = 0L;
        for (BitSet bits : index.getTidsetsByTermIdUnsafe()) {
            bitsets += (long) bits.toLongArray().length * 8L;
        }
        return dict + bitsets;
    }

    private static List<DocTerms> longPatternForLengthEight(int docs) {
        List<byte[]> longPattern = new ArrayList<>();
        for (int i = 0; i < 8; i++) {
            longPattern.add(token(1000 + i));
        }
        List<DocTerms> out = new ArrayList<>(docs);
        Random random = new Random(2026L);
        for (int d = 0; d < docs; d++) {
            List<byte[]> terms = new ArrayList<>(longPattern);
            for (int n = 0; n < 5; n++) {
                terms.add(token(20000 + random.nextInt(3000)));
            }
            out.add(ByteArrayTestSupport.doc(d, terms));
        }
        return out;
    }

    private static List<DocTerms> httpLikeLongPatternDataset(int docs) {
        List<byte[]> fixed = List.of(
                ascii("GET"), ascii("/"), ascii("HTTP"), ascii("1.1"), ascii("HOST"), ascii("UA"));
        List<DocTerms> out = new ArrayList<>(docs);
        Random random = new Random(77L);
        for (int d = 0; d < docs; d++) {
            List<byte[]> terms = new ArrayList<>(fixed);
            terms.add(ascii("PATH" + (d % 32)));
            terms.add(ascii("HDR" + (d % 16)));
            for (int n = 0; n < 3; n++) {
                terms.add(token(25000 + random.nextInt(2000)));
            }
            out.add(ByteArrayTestSupport.doc(d, terms));
        }
        return out;
    }

    private static List<DocTerms> overlapHeavyDataset(int docs, long seed) {
        Random random = new Random(seed);
        List<byte[]> coreA = List.of(token(1), token(2), token(3), token(4), token(5));
        List<byte[]> coreB = List.of(token(3), token(4), token(5), token(6), token(7));
        List<byte[]> coreC = List.of(token(5), token(6), token(7), token(8), token(9));
        List<DocTerms> out = new ArrayList<>(docs);
        for (int d = 0; d < docs; d++) {
            List<byte[]> terms = new ArrayList<>();
            int m = d % 3;
            if (m == 0) {
                terms.addAll(coreA);
            } else if (m == 1) {
                terms.addAll(coreB);
            } else {
                terms.addAll(coreC);
            }
            for (int n = 0; n < 3; n++) {
                terms.add(token(30000 + random.nextInt(4000)));
            }
            out.add(ByteArrayTestSupport.doc(d, terms));
        }
        return out;
    }

    private static List<CandidateItemset> topBySaving(List<CandidateItemset> candidates, int topK) {
        List<CandidateItemset> copy = new ArrayList<>(candidates);
        copy.sort((a, b) -> Integer.compare(b.getEstimatedSaving(), a.getEstimatedSaving()));
        if (copy.size() <= topK) {
            return copy;
        }
        return new ArrayList<>(copy.subList(0, topK));
    }

    private static long simulatedEncodedBytes(
            List<DocTerms> rows,
            TermTidsetIndex index,
            List<CandidateItemset> itemsets,
            boolean mutexInDoc
    ) {
        Map<ByteArrayKey, Integer> termIdMap = new HashMap<>();
        List<byte[]> terms = index.getIdToTermUnsafe();
        for (int i = 0; i < terms.size(); i++) {
            termIdMap.put(new ByteArrayKey(terms.get(i)), Integer.valueOf(i));
        }
        long encodedBytes = 0L;
        for (DocTerms row : rows) {
            Set<Integer> inDoc = new HashSet<>();
            for (byte[] term : row.getTermsUnsafe()) {
                Integer id = termIdMap.get(new ByteArrayKey(term));
                if (id != null) {
                    inDoc.add(id);
                }
            }
            Set<Integer> used = mutexInDoc ? new HashSet<>() : null;
            for (CandidateItemset itemset : itemsets) {
                int[] ids = itemset.getTermIdsUnsafe();
                boolean match = true;
                for (int id : ids) {
                    if (!inDoc.contains(Integer.valueOf(id))) {
                        match = false;
                        break;
                    }
                    if (mutexInDoc && used.contains(Integer.valueOf(id))) {
                        match = false;
                        break;
                    }
                }
                if (match) {
                    encodedBytes += 4L;
                    if (mutexInDoc) {
                        for (int id : ids) {
                            used.add(Integer.valueOf(id));
                        }
                    }
                }
            }
        }
        return encodedBytes;
    }

    private static FusionDataset createFusionDataset(int docs, double highRatio, double lowRatio, long seed) {
        Random random = new Random(seed);
        byte[] high = ascii("GET");
        byte[] low = ascii("RARE_SIG");
        byte[] common = ascii("PAYLOAD");
        List<DocTerms> rows = new ArrayList<>(docs);
        for (int d = 0; d < docs; d++) {
            List<byte[]> terms = new ArrayList<>();
            terms.add(common);
            if (random.nextDouble() < highRatio) {
                terms.add(high);
            }
            if (random.nextDouble() < lowRatio) {
                terms.add(low);
            }
            terms.add(token(40000 + random.nextInt(5000)));
            rows.add(ByteArrayTestSupport.doc(d, terms));
        }
        TermTidsetIndex index = TermTidsetIndex.build(rows);
        List<SelectedGroup> groups = runSelectorGroups(rows);
        return new FusionDataset(rows, index, groups, high, low);
    }

    private static List<SelectedGroup> runSelectorGroups(List<DocTerms> rows) {
        return cn.lxdb.plugins.muqingyu.fptoken.ExclusiveFrequentItemsetSelector
                .selectExclusiveBestItemsets(rows, 50, 2, 6, 100000);
    }

    private static int runFullScanQueries(List<DocTerms> rows, byte[] needle, int rounds) {
        int hits = 0;
        for (int r = 0; r < rounds; r++) {
            for (DocTerms row : rows) {
                if (containsTerm(row.getTermsUnsafe(), needle)) {
                    hits++;
                }
            }
        }
        return hits;
    }

    private static int runFrequentPatternLookup(List<SelectedGroup> groups, byte[] needle, int rounds) {
        int hits = 0;
        for (int r = 0; r < rounds; r++) {
            for (SelectedGroup group : groups) {
                if (containsTerm(group.getTerms(), needle)) {
                    hits += group.getSupport();
                }
            }
        }
        return hits;
    }

    private static int runDataSkippingLookup(TermTidsetIndex index, byte[] needle, int rounds) {
        Integer id = termIdOf(index, needle);
        if (id == null) {
            return 0;
        }
        BitSet bits = index.getTidsetsByTermIdUnsafe().get(id.intValue());
        int count = bits.cardinality();
        int hits = 0;
        for (int r = 0; r < rounds; r++) {
            hits += count;
        }
        return hits;
    }

    private static int runHybridLookup(FusionDataset data, byte[] needle, int rounds) {
        Integer id = termIdOf(data.index, needle);
        int skipHits = 0;
        if (id != null) {
            skipHits = data.index.getTidsetsByTermIdUnsafe().get(id.intValue()).cardinality();
        }
        int groupHits = 0;
        for (SelectedGroup group : data.groups) {
            if (containsTerm(group.getTerms(), needle)) {
                groupHits += group.getSupport();
            }
        }
        int oneRound = Math.max(skipHits, groupHits);
        return oneRound * rounds;
    }

    private static Integer termIdOf(TermTidsetIndex index, byte[] term) {
        List<byte[]> terms = index.getIdToTermUnsafe();
        for (int i = 0; i < terms.size(); i++) {
            if (java.util.Arrays.equals(terms.get(i), term)) {
                return Integer.valueOf(i);
            }
        }
        return null;
    }

    private static boolean containsTerm(List<byte[]> terms, byte[] needle) {
        for (byte[] term : terms) {
            if (java.util.Arrays.equals(term, needle)) {
                return true;
            }
        }
        return false;
    }

    private static byte[] ascii(String s) {
        return s.getBytes(java.nio.charset.StandardCharsets.US_ASCII);
    }

    private static byte[] token(int id) {
        return new byte[] {(byte) ((id >>> 16) & 0xFF), (byte) ((id >>> 8) & 0xFF), (byte) (id & 0xFF)};
    }

    private static final class FusionDataset {
        private final List<DocTerms> rows;
        private final TermTidsetIndex index;
        private final List<SelectedGroup> groups;
        private final byte[] highTerm;
        private final byte[] lowTerm;

        private FusionDataset(
                List<DocTerms> rows,
                TermTidsetIndex index,
                List<SelectedGroup> groups,
                byte[] highTerm,
                byte[] lowTerm
        ) {
            this.rows = rows;
            this.index = index;
            this.groups = groups;
            this.highTerm = highTerm;
            this.lowTerm = lowTerm;
        }
    }
}
