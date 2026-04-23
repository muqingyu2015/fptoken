package cn.lxdb.plugins.muqingyu.fptoken.tests.unit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import cn.lxdb.plugins.muqingyu.fptoken.api.ExclusiveFpRowsProcessingApi;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.DocTerms;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.SelectedGroup;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.util.ByteArrayUtils;
import cn.lxdb.plugins.muqingyu.fptoken.runner.result.LineFileProcessingResult;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * {@link ExclusiveFpRowsProcessingApi#processRows(List, int, int, int)} 接口级契约测试。
 */
class ExclusiveFpRowsProcessingApiProcessRowsTest {

    @Test
    void processRows_shouldReturnEmptyStructures_whenRowsEmpty() {
        LineFileProcessingResult result = ExclusiveFpRowsProcessingApi.processRows(
                new ArrayList<DocTerms>(), 1, 2, 1);

        assertTrue(result.getLoadedRows().isEmpty());
        assertTrue(result.getSelectionResult().getGroups().isEmpty());
        assertTrue(result.getFinalIndexData().getHighFreqMutexGroupPostings().isEmpty());
        assertTrue(result.getFinalIndexData().getHighFreqSingleTermPostings().isEmpty());
        assertTrue(result.getFinalIndexData().getLowHitForwardRows().isEmpty());
    }

    @Test
    void processRows_shouldRejectInvalidArguments() {
        List<DocTerms> rows = new ArrayList<DocTerms>();
        rows.add(new DocTerms(0, Arrays.asList(bytes("AA"), bytes("AB"))));

        assertThrows(IllegalArgumentException.class, () ->
                ExclusiveFpRowsProcessingApi.processRows(rows, 0, 2, 1));
        assertThrows(IllegalArgumentException.class, () ->
                ExclusiveFpRowsProcessingApi.processRows(rows, 1, 0, 1));
    }

    @Test
    void processRows_shouldUseStrictGreaterThan_forHotTermThreshold() {
        // A 出现 2 次、B 出现 3 次；阈值=2 时应只保留 B（严格 >2）
        List<DocTerms> rows = new ArrayList<DocTerms>();
        rows.add(new DocTerms(0, Arrays.asList(bytes("A"), bytes("B"))));
        rows.add(new DocTerms(1, Arrays.asList(bytes("A"), bytes("B"))));
        rows.add(new DocTerms(2, Arrays.asList(bytes("B"), bytes("C"))));

        // minSupport 设大，避免产生互斥组合，聚焦 hotTermThreshold 语义
        LineFileProcessingResult result = ExclusiveFpRowsProcessingApi.processRows(rows, 100, 2, 2);
        Map<String, List<Integer>> hotMap = toHotMap(result.getFinalIndexData().getHighFreqSingleTermPostings());

        assertFalse(hotMap.containsKey(hex("A")));
        assertTrue(hotMap.containsKey(hex("B")));
        assertEquals(Arrays.asList(0, 1, 2), hotMap.get(hex("B")));
    }

    @Test
    void processRows_shouldDefensivelyCopyInputRows() {
        byte[] mutable = bytes("AB");
        List<DocTerms> rows = new ArrayList<DocTerms>();
        rows.add(new DocTerms(0, Arrays.asList(mutable, bytes("BC"))));
        rows.add(new DocTerms(1, Arrays.asList(bytes("AB"), bytes("CD"))));

        LineFileProcessingResult result = ExclusiveFpRowsProcessingApi.processRows(rows, 1, 2, 1);

        // 调用后篡改调用方原始数组，不应影响 result 内部快照
        mutable[0] = 'Z';
        mutable[1] = 'Z';
        rows.clear();

        String loadedFingerprint = rowsFingerprint(result.getLoadedRows());
        assertTrue(loadedFingerprint.contains(hex("AB")));
        assertFalse(loadedFingerprint.contains(hex("ZZ")));
    }

    @Test
    void processRows_shouldKeepNewAndLegacyAccessorsConsistent() {
        List<DocTerms> rows = new ArrayList<DocTerms>();
        rows.add(new DocTerms(0, Arrays.asList(bytes("AA"), bytes("AB"), bytes("AC"))));
        rows.add(new DocTerms(1, Arrays.asList(bytes("AA"), bytes("AB"))));
        rows.add(new DocTerms(2, Arrays.asList(bytes("AA"), bytes("AD"))));

        LineFileProcessingResult result = ExclusiveFpRowsProcessingApi.processRows(rows, 2, 2, 1);
        LineFileProcessingResult.FinalIndexData finalData = result.getFinalIndexData();

        // 新命名访问器 vs 兼容访问器必须一致
        assertEquals(groupsFingerprint(finalData.getHighFreqMutexGroupPostings()),
                groupsFingerprint(finalData.getGroups()));
        assertEquals(hotTermsFingerprint(finalData.getHighFreqSingleTermPostings()),
                hotTermsFingerprint(finalData.getHotTerms()));
        assertEquals(rowsFingerprint(finalData.getLowHitForwardRows()),
                rowsFingerprint(finalData.getCutRes()));

        // 顶层便捷访问器也应与 FinalIndexData 一致
        assertEquals(groupsFingerprint(result.getGroups()),
                groupsFingerprint(finalData.getHighFreqMutexGroupPostings()));
        assertEquals(hotTermsFingerprint(result.getHotTerms()),
                hotTermsFingerprint(finalData.getHighFreqSingleTermPostings()));
        assertEquals(rowsFingerprint(result.getCutRes()),
                rowsFingerprint(finalData.getLowHitForwardRows()));
    }

    @Test
    void processRows_shouldBuildIndexBasedInvertedViews_withoutChangingOriginalStructures() {
        List<DocTerms> rows = new ArrayList<DocTerms>();
        rows.add(new DocTerms(0, Arrays.asList(bytes("AA"), bytes("AB"), bytes("LX"))));
        rows.add(new DocTerms(1, Arrays.asList(bytes("AA"), bytes("AB"), bytes("LY"))));
        rows.add(new DocTerms(2, Arrays.asList(bytes("AA"), bytes("AZ"), bytes("LY"))));

        LineFileProcessingResult result = ExclusiveFpRowsProcessingApi.processRows(rows, 2, 2, 1);
        LineFileProcessingResult.FinalIndexData finalData = result.getFinalIndexData();

        List<SelectedGroup> groups = finalData.getHighFreqMutexGroupPostings();
        List<LineFileProcessingResult.TermsPostingIndexRef> groupIndexRefs =
                finalData.getHighFreqMutexGroupTermsToIndex();
        assertEquals(groups.size(), groupIndexRefs.size());
        for (int i = 0; i < groupIndexRefs.size(); i++) {
            assertEquals(i, groupIndexRefs.get(i).getPostingIndex());
            assertEquals(termsFingerprint(groups.get(i).getTerms()),
                    termsFingerprint(groupIndexRefs.get(i).getTerms()));
        }

        List<LineFileProcessingResult.HotTermDocList> hotTerms = finalData.getHighFreqSingleTermPostings();
        List<LineFileProcessingResult.TermsPostingIndexRef> hotTermIndexRefs =
                finalData.getHighFreqSingleTermToIndex();
        assertEquals(hotTerms.size(), hotTermIndexRefs.size());
        for (int i = 0; i < hotTermIndexRefs.size(); i++) {
            assertEquals(i, hotTermIndexRefs.get(i).getPostingIndex());
            assertEquals(1, hotTermIndexRefs.get(i).getTerms().size());
            assertEquals(ByteArrayUtils.toHex(hotTerms.get(i).getTerm()),
                    ByteArrayUtils.toHex(hotTermIndexRefs.get(i).getTerms().get(0)));
        }

        List<String> expectedLowHit = buildLowHitExpandedRefs(finalData.getLowHitForwardRows());
        List<String> actualLowHit = toLowHitExpandedRefs(finalData.getLowHitTermToIndexes());
        Collections.sort(expectedLowHit);
        Collections.sort(actualLowHit);
        assertEquals(expectedLowHit, actualLowHit);
    }

    private static Map<String, List<Integer>> toHotMap(
            List<LineFileProcessingResult.HotTermDocList> hotTerms
    ) {
        Map<String, List<Integer>> out = new LinkedHashMap<String, List<Integer>>();
        for (LineFileProcessingResult.HotTermDocList hotTerm : hotTerms) {
            out.put(ByteArrayUtils.toHex(hotTerm.getTerm()), hotTerm.getDocIds());
        }
        return out;
    }

    private static String groupsFingerprint(List<SelectedGroup> groups) {
        StringBuilder sb = new StringBuilder();
        for (SelectedGroup group : groups) {
            sb.append('{');
            for (byte[] term : group.getTerms()) {
                sb.append(ByteArrayUtils.toHex(term)).append(',');
            }
            sb.append('|');
            sb.append(group.getSupport()).append('|');
            sb.append(group.getDocIds().size()).append('}');
        }
        return sb.toString();
    }

    private static String hotTermsFingerprint(List<LineFileProcessingResult.HotTermDocList> hotTerms) {
        StringBuilder sb = new StringBuilder();
        for (LineFileProcessingResult.HotTermDocList hotTerm : hotTerms) {
            sb.append(ByteArrayUtils.toHex(hotTerm.getTerm())).append(':');
            for (Integer docId : hotTerm.getDocIds()) {
                sb.append(docId).append(',');
            }
            sb.append(';');
        }
        return sb.toString();
    }

    private static String rowsFingerprint(List<DocTerms> rows) {
        StringBuilder sb = new StringBuilder();
        for (DocTerms row : rows) {
            sb.append(row.getDocId()).append(':');
            for (byte[] term : row.getTermsUnsafe()) {
                sb.append(ByteArrayUtils.toHex(term)).append(',');
            }
            sb.append(';');
        }
        return sb.toString();
    }

    private static String termsFingerprint(List<byte[]> terms) {
        StringBuilder sb = new StringBuilder();
        for (byte[] term : terms) {
            sb.append(ByteArrayUtils.toHex(term)).append(',');
        }
        return sb.toString();
    }

    private static List<String> buildLowHitExpandedRefs(List<DocTerms> rows) {
        List<String> out = new ArrayList<String>();
        for (int rowIndex = 0; rowIndex < rows.size(); rowIndex++) {
            int docId = rows.get(rowIndex).getDocId();
            for (byte[] term : rows.get(rowIndex).getTermsUnsafe()) {
                out.add(ByteArrayUtils.toHex(term) + "@" + docId);
            }
        }
        return out;
    }

    private static List<String> toLowHitExpandedRefs(
            List<LineFileProcessingResult.TermsPostingIndexRef> refs
    ) {
        List<String> out = new ArrayList<String>();
        for (LineFileProcessingResult.TermsPostingIndexRef ref : refs) {
            assertEquals(1, ref.getTerms().size());
            out.add(ByteArrayUtils.toHex(ref.getTerms().get(0)) + "@" + ref.getPostingIndex());
        }
        return out;
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static String hex(String value) {
        return ByteArrayUtils.toHex(bytes(value));
    }
}
