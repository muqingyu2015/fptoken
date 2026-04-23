package cn.lxdb.plugins.muqingyu.fptoken.tests.performance;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import cn.lxdb.plugins.muqingyu.fptoken.api.ExclusiveFpRowsProcessingApi;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.DocTerms;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.util.ByteArrayKey;
import cn.lxdb.plugins.muqingyu.fptoken.runner.result.LineFileProcessingResult;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

/**
 * 面向“检索路径”的性能对比测试：
 * 1) 1-byte 倒排 bitset 检索 vs 原始行扫描；
 * 2) low-hit skip-bitset 候选过滤+精确校验 vs 全量倒排引用扫描。
 */
@Tag("performance")
@EnabledIfSystemProperty(named = "fptoken.runPerfTests", matches = "true")
@Timeout(value = 120, unit = TimeUnit.SECONDS)
class DualIndexRetrievalComparisonPerformanceTest {

    @Test
    void PERF_RETRIEVE_001_oneByteBitset_shouldNotSlowerThanNaiveScan() {
        int docs = PerfTestSupport.intProp("fptoken.perf.retrieve.onebyte.docs", 10000);
        int rounds = PerfTestSupport.intProp("fptoken.perf.retrieve.onebyte.rounds", 16);
        double maxRatio = Double.parseDouble(
                System.getProperty("fptoken.perf.retrieve.onebyte.maxRatio", "1.00"));

        List<DocTerms> rawRows = buildRawRowsForOneByte(docs, 64);
        LineFileProcessingResult processing = ExclusiveFpRowsProcessingApi.processRowsWithNgram(
                rawRows, 2, 4, docs + 1, 2, docs + 1);

        List<BitSet> oneByteBuckets = processing.getOneByteDocidBitsetIndex().getBuckets();
        int[] queryBytes = buildHotQueryBytes();

        // 预热，减小首次 JIT 噪声。
        executeOneByteBitsetLookup(oneByteBuckets, queryBytes, 4);
        executeOneByteNaiveLookup(rawRows, queryBytes, 2);

        long bitsetMs = PerfTestSupport.elapsedMillis(() ->
                executeOneByteBitsetLookup(oneByteBuckets, queryBytes, rounds));
        long naiveMs = PerfTestSupport.elapsedMillis(() ->
                executeOneByteNaiveLookup(rawRows, queryBytes, rounds));

        int bitsetHits = executeOneByteBitsetLookup(oneByteBuckets, queryBytes, 1);
        int naiveHits = executeOneByteNaiveLookup(rawRows, queryBytes, 1);
        assertEquals(naiveHits, bitsetHits, "one-byte 检索命中数不一致");

        double ratio = ((double) bitsetMs) / Math.max(1.0d, naiveMs);
        System.out.println("[perf-compare one-byte] docs=" + docs
                + ", rounds=" + rounds
                + ", bitsetMs=" + bitsetMs
                + ", naiveMs=" + naiveMs
                + ", ratio=" + ratio
                + ", maxRatio=" + maxRatio);
        assertTrue(ratio <= maxRatio, "ratio=" + ratio + ", maxRatio=" + maxRatio);
    }

    @Test
    void PERF_RETRIEVE_002_lowHitSkipBitset_shouldNotSlowerThanNaiveRefScan() {
        int docs = PerfTestSupport.intProp("fptoken.perf.retrieve.lowhit.docs", 5000);
        int queryCount = PerfTestSupport.intProp("fptoken.perf.retrieve.lowhit.queryCount", 120);
        int rounds = PerfTestSupport.intProp("fptoken.perf.retrieve.lowhit.rounds", 6);
        double maxRatio = Double.parseDouble(
                System.getProperty("fptoken.perf.retrieve.lowhit.maxRatio", "1.00"));

        List<DocTerms> rawRows = buildRawRowsForLowHit(docs, 64, 20260422L);
        LineFileProcessingResult processing = ExclusiveFpRowsProcessingApi.processRowsWithNgram(
                rawRows, 2, 4, docs + 1, 2, docs + 1);
        LineFileProcessingResult.FinalIndexData finalData = processing.getFinalIndexData();

        List<DocTerms> lowHitRows = finalData.getLowHitForwardRows();
        List<LineFileProcessingResult.TermsPostingIndexRef> lowHitRefs = finalData.getLowHitTermToIndexes();
        assertTrue(!lowHitRows.isEmpty(), "lowHitForwardRows should not be empty");
        assertTrue(!lowHitRefs.isEmpty(), "lowHitTermToIndexes should not be empty");

        PreparedSkipIndex skipIndex = prepareSkipIndex(finalData.getLowHitTermSkipBitsetIndex());
        List<byte[]> queryTerms = sampleSingleTerms(lowHitRefs, queryCount);
        assertTrue(!queryTerms.isEmpty(), "queryTerms should not be empty");
        Map<Integer, DocTerms> rowsByDocId = mapRowsByDocId(lowHitRows);

        // 预热，减小首次 JIT 噪声。
        executeLowHitSkipLookup(skipIndex, rowsByDocId, queryTerms, 2);
        executeLowHitNaiveLookup(lowHitRows, queryTerms, 1);

        long skipMs = PerfTestSupport.elapsedMillis(() ->
                executeLowHitSkipLookup(skipIndex, rowsByDocId, queryTerms, rounds));
        long naiveMs = PerfTestSupport.elapsedMillis(() ->
                executeLowHitNaiveLookup(lowHitRows, queryTerms, rounds));

        int skipHits = executeLowHitSkipLookup(skipIndex, rowsByDocId, queryTerms, 1);
        int naiveHits = executeLowHitNaiveLookup(lowHitRows, queryTerms, 1);
        assertEquals(naiveHits, skipHits, "low-hit 检索命中数不一致");

        double ratio = ((double) skipMs) / Math.max(1.0d, naiveMs);
        System.out.println("[perf-compare low-hit] docs=" + docs
                + ", refs=" + lowHitRefs.size()
                + ", queryTerms=" + queryTerms.size()
                + ", rounds=" + rounds
                + ", skipMs=" + skipMs
                + ", naiveMs=" + naiveMs
                + ", ratio=" + ratio
                + ", maxRatio=" + maxRatio);
        assertTrue(ratio <= maxRatio, "ratio=" + ratio + ", maxRatio=" + maxRatio);
    }

    private static int executeOneByteBitsetLookup(List<BitSet> oneByteBuckets, int[] queryBytes, int rounds) {
        int total = 0;
        for (int r = 0; r < rounds; r++) {
            for (int value : queryBytes) {
                total += oneByteBuckets.get(value & 0xFF).cardinality();
            }
        }
        return total;
    }

    private static int executeOneByteNaiveLookup(List<DocTerms> rawRows, int[] queryBytes, int rounds) {
        int total = 0;
        for (int r = 0; r < rounds; r++) {
            for (int value : queryBytes) {
                for (DocTerms row : rawRows) {
                    if (rowContainsUnsignedByte(row, value & 0xFF)) {
                        total++;
                    }
                }
            }
        }
        return total;
    }

    private static int executeLowHitSkipLookup(
            PreparedSkipIndex skipIndex,
            Map<Integer, DocTerms> rowsByDocId,
            List<byte[]> queryTerms,
            int rounds
    ) {
        int total = 0;
        for (int r = 0; r < rounds; r++) {
            for (byte[] queryTerm : queryTerms) {
                BitSet candidates = candidatePostings(skipIndex, queryTerm);
                for (int postingIndex = candidates.nextSetBit(0);
                     postingIndex >= 0;
                     postingIndex = candidates.nextSetBit(postingIndex + 1)) {
                    DocTerms row = rowsByDocId.get(Integer.valueOf(postingIndex));
                    if (row != null && rowContainsTerm(row, queryTerm)) {
                        total++;
                    }
                }
            }
        }
        return total;
    }

    private static int executeLowHitNaiveLookup(
            List<DocTerms> lowHitRows,
            List<byte[]> queryTerms,
            int rounds
    ) {
        int total = 0;
        for (int r = 0; r < rounds; r++) {
            for (byte[] queryTerm : queryTerms) {
                for (DocTerms row : lowHitRows) {
                    if (rowContainsTerm(row, queryTerm)) {
                        total++;
                    }
                }
            }
        }
        return total;
    }

    private static BitSet candidatePostings(PreparedSkipIndex skipIndex, byte[] term) {
        BitSet allLevelsCandidate = null;
        for (PreparedHashLevel level : skipIndex.levels) {
            int gramLength = level.gramLength;
            if (term.length < gramLength) {
                continue;
            }
            BitSet levelCandidate = null;
            for (int start = 0; start <= term.length - gramLength; start++) {
                int bucket = hashWindowToBucket(term, start, gramLength);
                BitSet bucketBits = level.buckets.get(bucket);
                if (levelCandidate == null) {
                    levelCandidate = (BitSet) bucketBits.clone();
                } else {
                    levelCandidate.and(bucketBits);
                }
                if (levelCandidate.isEmpty()) {
                    break;
                }
            }
            if (levelCandidate == null) {
                continue;
            }
            if (allLevelsCandidate == null) {
                allLevelsCandidate = levelCandidate;
            } else {
                allLevelsCandidate.and(levelCandidate);
            }
            if (allLevelsCandidate.isEmpty()) {
                return allLevelsCandidate;
            }
        }
        return allLevelsCandidate == null ? new BitSet() : allLevelsCandidate;
    }

    private static PreparedSkipIndex prepareSkipIndex(LineFileProcessingResult.TermBlockSkipBitsetIndex skipIndex) {
        List<PreparedHashLevel> levels = new ArrayList<PreparedHashLevel>();
        for (LineFileProcessingResult.HashLevelBitsets level : skipIndex.getHashLevels()) {
            levels.add(new PreparedHashLevel(level.getGramLength(), level.getBuckets()));
        }
        return new PreparedSkipIndex(levels);
    }

    private static List<byte[]> sampleSingleTerms(
            List<LineFileProcessingResult.TermsPostingIndexRef> refs, int queryCount
    ) {
        List<byte[]> out = new ArrayList<byte[]>();
        Set<ByteArrayKey> seen = new HashSet<ByteArrayKey>();
        for (LineFileProcessingResult.TermsPostingIndexRef ref : refs) {
            List<byte[]> terms = ref.getTerms();
            if (terms.isEmpty()) {
                continue;
            }
            byte[] term = terms.get(0);
            ByteArrayKey key = new ByteArrayKey(term);
            if (seen.add(key)) {
                out.add(Arrays.copyOf(term, term.length));
                if (out.size() >= queryCount) {
                    break;
                }
            }
        }
        return out;
    }

    private static boolean rowContainsUnsignedByte(DocTerms row, int unsignedByte) {
        for (byte[] arr : row.getTermsUnsafe()) {
            for (int i = 0; i < arr.length; i++) {
                if ((arr[i] & 0xFF) == unsignedByte) {
                    return true;
                }
            }
        }
        return false;
    }

    private static boolean rowContainsTerm(DocTerms row, byte[] term) {
        for (byte[] value : row.getTermsUnsafe()) {
            if (Arrays.equals(value, term)) {
                return true;
            }
        }
        return false;
    }

    private static Map<Integer, DocTerms> mapRowsByDocId(List<DocTerms> rows) {
        Map<Integer, DocTerms> out = new LinkedHashMap<Integer, DocTerms>(rows.size() * 2);
        for (DocTerms row : rows) {
            out.put(Integer.valueOf(row.getDocId()), row);
        }
        return out;
    }

    private static int hashWindowToBucket(byte[] arr, int start, int len) {
        int h = 1;
        for (int i = 0; i < len; i++) {
            h = 31 * h + (arr[start + i] & 0xFF);
        }
        return h & 0xFF;
    }

    private static int[] buildHotQueryBytes() {
        return new int[] {
                0x30, 0x31, 0x32, 0x33, 0x34,
                0x35, 0x36, 0x37, 0x38, 0x39,
                0x41, 0x42, 0x43, 0x44, 0x45,
                0x46, 0x5f, 0x61, 0x62, 0x63
        };
    }

    private static List<DocTerms> buildRawRowsForOneByte(int docs, int lineLen) {
        List<DocTerms> rows = new ArrayList<DocTerms>(docs);
        for (int i = 0; i < docs; i++) {
            String line = fixedLenAscii("ONEBYTE_" + (i % 64) + "_", lineLen, i + 11);
            rows.add(new DocTerms(i, Arrays.asList(line.getBytes(StandardCharsets.UTF_8))));
        }
        return rows;
    }

    private static List<DocTerms> buildRawRowsForLowHit(int docs, int lineLen, long seed) {
        Random random = new Random(seed);
        List<DocTerms> rows = new ArrayList<DocTerms>(docs);
        for (int i = 0; i < docs; i++) {
            StringBuilder sb = new StringBuilder(lineLen);
            sb.append("LH_").append(i).append("_");
            while (sb.length() < lineLen) {
                int v = random.nextInt(62);
                char c;
                if (v < 10) {
                    c = (char) ('0' + v);
                } else if (v < 36) {
                    c = (char) ('A' + (v - 10));
                } else {
                    c = (char) ('a' + (v - 36));
                }
                sb.append(c);
            }
            if (sb.length() > lineLen) {
                sb.setLength(lineLen);
            }
            rows.add(new DocTerms(i, Arrays.asList(sb.toString().getBytes(StandardCharsets.UTF_8))));
        }
        return rows;
    }

    private static String fixedLenAscii(String seed, int len, int salt) {
        StringBuilder sb = new StringBuilder(len);
        sb.append(seed);
        int cursor = salt;
        while (sb.length() < len) {
            int v = (cursor * 131 + 17) & 63;
            char c;
            if (v < 10) {
                c = (char) ('0' + v);
            } else if (v < 36) {
                c = (char) ('A' + (v - 10));
            } else {
                c = (char) ('a' + (v - 36));
            }
            sb.append(c);
            cursor++;
        }
        if (sb.length() > len) {
            sb.setLength(len);
        }
        return sb.toString();
    }

    private static final class PreparedSkipIndex {
        private final List<PreparedHashLevel> levels;

        private PreparedSkipIndex(List<PreparedHashLevel> levels) {
            this.levels = levels;
        }
    }

    private static final class PreparedHashLevel {
        private final int gramLength;
        private final List<BitSet> buckets;

        private PreparedHashLevel(int gramLength, List<BitSet> buckets) {
            this.gramLength = gramLength;
            this.buckets = buckets;
        }
    }
}
