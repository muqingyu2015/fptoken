package cn.lxdb.plugins.muqingyu.fptoken.tests.unit;

import static org.junit.jupiter.api.Assertions.assertEquals;

import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.DocTerms;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.SelectedGroup;
import cn.lxdb.plugins.muqingyu.fptoken.runner.result.LineFileProcessingResult;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * 最终索引 bitset 精确性测试：
 * 对每个 bucket 的每一位做独立真值对账，避免“看起来能跑”但位图错位。
 */
class FinalIndexBitsetAccuracyUnitTest {

    @Test
    void finalIndexData_bitsets_shouldMatchIndependentGroundTruthPerBit() {
        List<DocTerms> loadedRows = new ArrayList<DocTerms>();
        loadedRows.add(new DocTerms(2, Arrays.asList(bytes("ABX"), bytes("Q"))));
        loadedRows.add(new DocTerms(5, Arrays.asList(bytes("BC"), bytes("XA"))));
        loadedRows.add(new DocTerms(9, Arrays.asList(bytes("AY"))));

        List<SelectedGroup> groups = new ArrayList<SelectedGroup>();
        groups.add(new SelectedGroup(
                Arrays.asList(bytes("AB"), bytes("BC")),
                Arrays.asList(2, 5),
                2,
                0
        ));
        groups.add(new SelectedGroup(
                Arrays.asList(bytes("XA"), bytes("AY")),
                Arrays.asList(5, 9),
                2,
                0
        ));

        List<LineFileProcessingResult.HotTermDocList> hotTerms =
                new ArrayList<LineFileProcessingResult.HotTermDocList>();
        hotTerms.add(new LineFileProcessingResult.HotTermDocList(bytes("Q"), Arrays.asList(2)));
        hotTerms.add(new LineFileProcessingResult.HotTermDocList(bytes("AY"), Arrays.asList(9)));

        List<DocTerms> lowHitRows = new ArrayList<DocTerms>();
        lowHitRows.add(new DocTerms(2, Arrays.asList(bytes("ABX"), bytes("Q"))));
        lowHitRows.add(new DocTerms(5, Arrays.asList(bytes("BC"), bytes("XA"))));
        lowHitRows.add(new DocTerms(9, Arrays.asList(bytes("AY"))));

        int skipMinGram = 2;
        int skipMaxGram = 3;
        LineFileProcessingResult.FinalIndexData finalData = new LineFileProcessingResult.FinalIndexData(
                loadedRows,
                groups,
                hotTerms,
                lowHitRows,
                0,
                skipMinGram,
                skipMaxGram
        );

        assertSkipBitsetEqualsGroundTruth(
                finalData.getHighFreqMutexGroupSkipBitsetIndex(),
                finalData.getHighFreqMutexGroupTermsToIndex(),
                skipMinGram,
                skipMaxGram
        );
        assertSkipBitsetEqualsGroundTruth(
                finalData.getHighFreqSingleTermSkipBitsetIndex(),
                finalData.getHighFreqSingleTermToIndex(),
                skipMinGram,
                skipMaxGram
        );
        assertSkipBitsetEqualsGroundTruth(
                finalData.getLowHitTermSkipBitsetIndex(),
                finalData.getLowHitTermToIndexes(),
                skipMinGram,
                skipMaxGram
        );
        assertOneByteBitsetEqualsGroundTruth(finalData.getOneByteDocidBitsetIndex(), loadedRows);
    }

    private static void assertSkipBitsetEqualsGroundTruth(
            LineFileProcessingResult.TermBlockSkipBitsetIndex actual,
            List<LineFileProcessingResult.TermsPostingIndexRef> refs,
            int minGram,
            int maxGram
    ) {
        int expectedMaxPostingIndex = -1;
        int maxTermLen = 0;
        Map<Integer, List<BitSet>> expected = new HashMap<Integer, List<BitSet>>();
        for (int gram = minGram; gram <= maxGram; gram++) {
            expected.put(Integer.valueOf(gram), fresh256Buckets());
        }

        for (LineFileProcessingResult.TermsPostingIndexRef ref : refs) {
            int postingIndex = ref.getPostingIndex();
            if (postingIndex > expectedMaxPostingIndex) {
                expectedMaxPostingIndex = postingIndex;
            }
            for (byte[] term : ref.getTerms()) {
                if (term.length > maxTermLen) {
                    maxTermLen = term.length;
                }
                for (int gram = minGram; gram <= maxGram; gram++) {
                    if (term.length < gram) {
                        continue;
                    }
                    List<BitSet> buckets = expected.get(Integer.valueOf(gram));
                    for (int start = 0; start <= term.length - gram; start++) {
                        int bucket = hashWindowToBucket(term, start, gram);
                        buckets.get(bucket).set(postingIndex);
                    }
                }
            }
        }

        assertEquals(expectedMaxPostingIndex, actual.getMaxPostingIndex());
        Map<Integer, LineFileProcessingResult.HashLevelBitsets> actualByGram =
                toActualByGram(actual.getHashLevels());
        for (int gram = minGram; gram <= maxGram; gram++) {
            LineFileProcessingResult.HashLevelBitsets level = actualByGram.get(Integer.valueOf(gram));
            if (maxTermLen < gram) {
                assertEquals(null, level, "gram=" + gram + " should not exist");
                continue;
            }
            List<BitSet> expectedBuckets = expected.get(Integer.valueOf(gram));
            List<BitSet> actualBuckets = level.getBuckets();
            assertEquals(256, actualBuckets.size(), "gram=" + gram);
            for (int b = 0; b < 256; b++) {
                assertEquals(expectedBuckets.get(b), actualBuckets.get(b),
                        "gram=" + gram + ", bucket=" + b);
            }
        }
    }

    private static void assertOneByteBitsetEqualsGroundTruth(
            LineFileProcessingResult.OneByteDocidBitsetIndex actual,
            List<DocTerms> loadedRows
    ) {
        List<BitSet> expected = fresh256Buckets();
        int maxDocId = -1;
        for (DocTerms row : loadedRows) {
            int docId = row.getDocId();
            if (docId > maxDocId) {
                maxDocId = docId;
            }
            for (byte[] term : row.getTerms()) {
                for (int i = 0; i < term.length; i++) {
                    expected.get(term[i] & 0xFF).set(docId);
                }
            }
        }

        assertEquals(maxDocId, actual.getMaxDocId());
        List<BitSet> actualBuckets = actual.getBuckets();
        assertEquals(256, actualBuckets.size());
        for (int b = 0; b < 256; b++) {
            assertEquals(expected.get(b), actualBuckets.get(b), "byteBucket=" + b);
        }
    }

    private static Map<Integer, LineFileProcessingResult.HashLevelBitsets> toActualByGram(
            List<LineFileProcessingResult.HashLevelBitsets> levels
    ) {
        Map<Integer, LineFileProcessingResult.HashLevelBitsets> out =
                new HashMap<Integer, LineFileProcessingResult.HashLevelBitsets>();
        for (LineFileProcessingResult.HashLevelBitsets level : levels) {
            out.put(Integer.valueOf(level.getGramLength()), level);
        }
        return out;
    }

    private static List<BitSet> fresh256Buckets() {
        List<BitSet> out = new ArrayList<BitSet>(256);
        for (int i = 0; i < 256; i++) {
            out.add(new BitSet());
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

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}

