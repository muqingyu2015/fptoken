package cn.lxdb.plugins.muqingyu.fptoken.tests.unit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.DocTerms;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.SelectedGroup;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.util.ByteArrayUtils;
import cn.lxdb.plugins.muqingyu.fptoken.runner.result.LineFileProcessingResult;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * FinalIndexData 中间步骤构建单元测试。
 */
class FinalIndexDataIntermediateStepsUnitTest {

    @Test
    void intermediateBuilders_shouldBuildAllIndexRefsWithExpectedSemantics() {
        List<SelectedGroup> groups = new ArrayList<SelectedGroup>();
        groups.add(new SelectedGroup(
                Arrays.asList(bytes("AB"), bytes("BC")),
                Arrays.asList(10, 20),
                2,
                0));

        List<LineFileProcessingResult.HotTermDocList> hotTerms = new ArrayList<LineFileProcessingResult.HotTermDocList>();
        hotTerms.add(new LineFileProcessingResult.HotTermDocList(bytes("CD"), Arrays.asList(10, 30)));

        List<DocTerms> lowHitRows = new ArrayList<DocTerms>();
        lowHitRows.add(new DocTerms(7, Arrays.asList(bytes("EF"), bytes("FG"))));
        lowHitRows.add(new DocTerms(9, Arrays.asList(bytes("GH"))));

        List<LineFileProcessingResult.TermsPostingIndexRef> groupRefs =
                LineFileProcessingResult.FinalIndexData.IntermediateSteps
                        .buildHighFreqMutexGroupTermsToIndex(groups);
        List<LineFileProcessingResult.TermsPostingIndexRef> hotRefs =
                LineFileProcessingResult.FinalIndexData.IntermediateSteps
                        .buildHighFreqSingleTermToIndex(hotTerms);
        List<LineFileProcessingResult.TermsPostingIndexRef> lowHitRefs =
                LineFileProcessingResult.FinalIndexData.IntermediateSteps
                        .buildLowHitTermToIndexes(lowHitRows);

        assertEquals(1, groupRefs.size());
        assertEquals(0, groupRefs.get(0).getPostingIndex());
        assertEquals(2, groupRefs.get(0).getTerms().size());

        assertEquals(1, hotRefs.size());
        assertEquals(0, hotRefs.get(0).getPostingIndex());
        assertEquals(1, hotRefs.get(0).getTerms().size());
        assertEquals(hex("CD"), ByteArrayUtils.toHex(hotRefs.get(0).getTerms().get(0)));

        // 低命中展开必须使用 docId 作为 postingIndex（不是 rowIndex）
        assertEquals(3, lowHitRefs.size());
        assertTrue(containsRef(lowHitRefs, "EF", 7));
        assertTrue(containsRef(lowHitRefs, "FG", 7));
        assertTrue(containsRef(lowHitRefs, "GH", 9));
    }

    @Test
    void intermediateBuilders_shouldBuildSkipBitsetAndOneByteBitset() {
        List<LineFileProcessingResult.TermsPostingIndexRef> refs = new ArrayList<LineFileProcessingResult.TermsPostingIndexRef>();
        refs.add(new LineFileProcessingResult.TermsPostingIndexRef(Arrays.asList(bytes("ABCD")), 3));
        refs.add(new LineFileProcessingResult.TermsPostingIndexRef(Arrays.asList(bytes("BCDE")), 5));

        LineFileProcessingResult.TermBlockSkipBitsetIndex skipIndex =
                LineFileProcessingResult.FinalIndexData.IntermediateSteps
                        .buildTermBlockSkipBitsetIndex(refs, 2, 3);
        assertEquals(5, skipIndex.getMaxPostingIndex());
        assertEquals(2, skipIndex.getHashLevels().size());
        LineFileProcessingResult.HashLevelBitsets gram2 = findLevel(skipIndex, 2);
        int bucketAB = hashWindowToBucket(bytes("AB"), 0, 2);
        assertTrue(gram2.getBuckets().get(bucketAB).get(3));
        assertFalse(gram2.getBuckets().get(bucketAB).get(5));

        List<DocTerms> loadedRows = new ArrayList<DocTerms>();
        loadedRows.add(new DocTerms(3, Arrays.asList(bytes("AX"))));
        loadedRows.add(new DocTerms(5, Arrays.asList(bytes("XB"))));
        LineFileProcessingResult.OneByteDocidBitsetIndex oneByte =
                LineFileProcessingResult.FinalIndexData.IntermediateSteps
                        .buildOneByteDocidBitsetIndex(loadedRows);
        assertEquals(5, oneByte.getMaxDocId());
        assertTrue(oneByte.getDocIdBitset('A').get(3));
        assertTrue(oneByte.getDocIdBitset('X').get(3));
        assertTrue(oneByte.getDocIdBitset('X').get(5));
        assertTrue(oneByte.getDocIdBitset('B').get(5));
    }

    @Test
    void termsPostingIndexRef_singleTermConstructor_shouldBeDefensive() {
        byte[] src = bytes("AB");
        LineFileProcessingResult.TermsPostingIndexRef ref =
                new LineFileProcessingResult.TermsPostingIndexRef(src, 11);

        src[0] = 'Z';
        assertEquals(1, ref.getTerms().size());
        assertEquals(hex("AB"), ByteArrayUtils.toHex(ref.getTerms().get(0)));
        assertEquals(11, ref.getPostingIndex());
        List<byte[]> firstRead = ref.getTerms();
        firstRead.clear();
        assertEquals(1, ref.getTerms().size());
    }

    private static boolean containsRef(
            List<LineFileProcessingResult.TermsPostingIndexRef> refs,
            String term,
            int postingIndex
    ) {
        String termHex = hex(term);
        for (LineFileProcessingResult.TermsPostingIndexRef ref : refs) {
            if (ref.getPostingIndex() == postingIndex
                    && ref.getTerms().size() == 1
                    && termHex.equals(ByteArrayUtils.toHex(ref.getTerms().get(0)))) {
                return true;
            }
        }
        return false;
    }

    private static LineFileProcessingResult.HashLevelBitsets findLevel(
            LineFileProcessingResult.TermBlockSkipBitsetIndex index,
            int gramLength
    ) {
        for (LineFileProcessingResult.HashLevelBitsets level : index.getHashLevels()) {
            if (level.getGramLength() == gramLength) {
                return level;
            }
        }
        throw new AssertionError("missing gram level: " + gramLength);
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

    private static String hex(String value) {
        return ByteArrayUtils.toHex(bytes(value));
    }
}

