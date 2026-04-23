package cn.lxdb.plugins.muqingyu.fptoken.tests.unit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import cn.lxdb.plugins.muqingyu.fptoken.api.ExclusiveFpRowsProcessingApi;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.DocTerms;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.ExclusiveSelectionResult;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.SelectedGroup;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.util.ByteArrayKey;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.util.ByteArrayUtils;
import cn.lxdb.plugins.muqingyu.fptoken.runner.result.LineFileProcessingResult;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * ExclusiveFpRowsProcessingApi 中间步骤单元测试。
 */
class ExclusiveFpRowsProcessingApiIntermediateStepsUnitTest {

    @Test
    void copyRows_shouldBeDefensiveAgainstInputMutation() {
        byte[] raw = bytes("ABCD");
        List<DocTerms> rows = new ArrayList<DocTerms>();
        rows.add(new DocTerms(0, Arrays.asList(raw)));

        List<DocTerms> copied = ExclusiveFpRowsProcessingApi.IntermediateSteps.copyRows(rows);
        raw[0] = 'Z';
        rows.clear();

        assertEquals(1, copied.size());
        assertEquals(hex("ABCD"), ByteArrayUtils.toHex(copied.get(0).getTermsUnsafe().get(0)));
    }

    @Test
    void tokenizeRowsForMining_shouldTokenizeRawAndKeepPreTokenizedRows() {
        List<DocTerms> rows = new ArrayList<DocTerms>();
        rows.add(new DocTerms(0, Arrays.asList(bytes("ABCD")))); // raw
        rows.add(new DocTerms(1, Arrays.asList(bytes("XY"), bytes("YZ")))); // pre-tokenized

        List<DocTerms> tokenized = ExclusiveFpRowsProcessingApi.IntermediateSteps
                .tokenizeRowsForMining(rows, 2, 2);

        assertEquals(2, tokenized.size());
        String firstTerms = termsHex(tokenized.get(0).getTermsUnsafe());
        assertTrue(firstTerms.contains(hex("AB")));
        assertTrue(firstTerms.contains(hex("BC")));
        assertTrue(firstTerms.contains(hex("CD")));
        assertFalse(firstTerms.contains(hex("ABCD")));

        assertEquals(termsHex(rows.get(1).getTermsUnsafe()), termsHex(tokenized.get(1).getTermsUnsafe()));
    }

    @Test
    void buildHotTerms_andRemoveSelectedSteps_shouldMatchThresholdAndExclusion() {
        List<DocTerms> tokenizedRows = new ArrayList<DocTerms>();
        tokenizedRows.add(new DocTerms(0, Arrays.asList(bytes("AA"), bytes("BB"))));
        tokenizedRows.add(new DocTerms(1, Arrays.asList(bytes("AA"), bytes("CC"))));
        tokenizedRows.add(new DocTerms(2, Arrays.asList(bytes("AA"), bytes("DD"))));

        List<LineFileProcessingResult.HotTermDocList> hotTerms =
                ExclusiveFpRowsProcessingApi.IntermediateSteps.buildHotTerms(tokenizedRows, 2);
        Map<String, List<Integer>> hotMap = toHotMap(hotTerms);
        assertTrue(hotMap.containsKey(hex("AA")));
        assertFalse(hotMap.containsKey(hex("BB")));

        ExclusiveSelectionResult selectionResult = selectionWithTerm("AA");
        Set<ByteArrayKey> selected = ExclusiveFpRowsProcessingApi.IntermediateSteps
                .collectSelectedTerms(selectionResult);
        List<DocTerms> cutResAfterRemove = ExclusiveFpRowsProcessingApi.IntermediateSteps
                .removeSelectedTermsFromCutRes(tokenizedRows, selected);
        List<LineFileProcessingResult.HotTermDocList> hotAfterRemove = ExclusiveFpRowsProcessingApi.IntermediateSteps
                .removeSelectedTermsFromHotTerms(hotTerms, selected);

        assertFalse(toHotMap(hotAfterRemove).containsKey(hex("AA")));
        for (DocTerms row : cutResAfterRemove) {
            for (byte[] term : row.getTermsUnsafe()) {
                assertFalse(Arrays.equals(term, bytes("AA")));
            }
        }
    }

    private static ExclusiveSelectionResult selectionWithTerm(String term) {
        List<byte[]> terms = new ArrayList<byte[]>();
        terms.add(bytes(term));
        List<Integer> docIds = new ArrayList<Integer>();
        docIds.add(0);
        SelectedGroup g = new SelectedGroup(terms, docIds, 1, 0);
        List<SelectedGroup> groups = new ArrayList<SelectedGroup>();
        groups.add(g);
        return new ExclusiveSelectionResult(groups, 0, 0, 0, 0, false);
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

    private static String termsHex(List<byte[]> terms) {
        StringBuilder sb = new StringBuilder();
        for (byte[] term : terms) {
            sb.append(ByteArrayUtils.toHex(term)).append('|');
        }
        return sb.toString();
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static String hex(String value) {
        return ByteArrayUtils.toHex(bytes(value));
    }
}

