package cn.lxdb.plugins.muqingyu.fptoken.tests.functional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import cn.lxdb.plugins.muqingyu.fptoken.api.ExclusiveFpRowsProcessingApi;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.DocTerms;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.ExclusiveSelectionResult;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.SelectedGroup;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.util.ByteArrayUtils;
import cn.lxdb.plugins.muqingyu.fptoken.runner.dataset.LineRecordDatasetLoader;
import cn.lxdb.plugins.muqingyu.fptoken.runner.result.LineFileProcessingResult;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class DerivedDataBuildFromFilesFunctionalTest {

    @Test
    void buildDerivedData_thresholdBoundaryAndSelectedTerms_shouldBeCorrect() throws Exception {
        Path dir = Files.createTempDirectory("fptoken-derived-boundary-");
        Path file = dir.resolve("boundary.txt");
        Files.write(file, Arrays.asList("ABCD", "ABCE", "ZZZZ", "ABCD"), StandardCharsets.UTF_8);

        List<DocTerms> rows = LineRecordDatasetLoader.loadSingleFile(file, 2, 2).getRows();
        ExclusiveSelectionResult result = resultWithSelectedTerms("AB");

        LineFileProcessingResult.DerivedData derived =
                ExclusiveFpRowsProcessingApi.buildDerivedData(rows, result, 2);

        Map<String, List<Integer>> hotTerms = toHotTermMap(derived.getHotTerms());
        assertEquals(1, hotTerms.size());
        assertEquals(Arrays.asList(0, 1, 3), hotTerms.get(hex("BC")));
        assertFalse(hotTerms.containsKey(hex("AB")));

        List<DocTerms> cutRes = derived.getCutRes();
        assertEquals(rows.size(), cutRes.size());
        for (DocTerms row : cutRes) {
            assertFalse(containsTerm(row, "AB"));
        }
        assertTrue(containsTerm(cutRes.get(0), "BC"));
    }

    @Test
    void buildDerivedData_emptySelection_shouldKeepCutResAndHotTerms() throws Exception {
        Path dir = Files.createTempDirectory("fptoken-derived-empty-selection-");
        Path file = dir.resolve("no-selected.txt");
        Files.write(file, Arrays.asList("XYAB", "ABAC", "ABAD", "XYXY"), StandardCharsets.UTF_8);

        List<DocTerms> rows = LineRecordDatasetLoader.loadSingleFile(file, 2, 2).getRows();
        ExclusiveSelectionResult emptyResult =
                new ExclusiveSelectionResult(Collections.<SelectedGroup>emptyList(), 0, 0, 0, 0, false);

        LineFileProcessingResult.DerivedData derived =
                ExclusiveFpRowsProcessingApi.buildDerivedData(rows, emptyResult, 1);

        assertEquals(rowsFingerprint(rows), rowsFingerprint(derived.getCutRes()));
        Map<String, List<Integer>> hotTerms = toHotTermMap(derived.getHotTerms());
        assertEquals(Arrays.asList(0, 1, 2), hotTerms.get(hex("AB")));
        assertEquals(Arrays.asList(0, 3), hotTerms.get(hex("XY")));
    }

    @Test
    void buildDerivedData_hotTermsShouldSortByCountThenUnsignedTermOrder() throws Exception {
        Path dir = Files.createTempDirectory("fptoken-derived-sort-");
        Path file = dir.resolve("sort.txt");
        Files.write(file, Arrays.asList(
                "ADZ",
                "ADY",
                "ADX",
                "ABQ",
                "ABW",
                "ACQ",
                "ACW"
        ), StandardCharsets.UTF_8);

        List<DocTerms> rows = LineRecordDatasetLoader.loadSingleFile(file, 2, 2).getRows();
        ExclusiveSelectionResult emptyResult =
                new ExclusiveSelectionResult(Collections.<SelectedGroup>emptyList(), 0, 0, 0, 0, false);

        LineFileProcessingResult.DerivedData derived =
                ExclusiveFpRowsProcessingApi.buildDerivedData(rows, emptyResult, 1);

        List<LineFileProcessingResult.HotTermDocList> hotTerms = derived.getHotTerms();
        assertEquals(3, hotTerms.size());
        assertEquals(hex("AD"), ByteArrayUtils.toHex(hotTerms.get(0).getTerm()));
        assertEquals(hex("AB"), ByteArrayUtils.toHex(hotTerms.get(1).getTerm()));
        assertEquals(hex("AC"), ByteArrayUtils.toHex(hotTerms.get(2).getTerm()));
        assertEquals(3, hotTerms.get(0).getCount());
        assertEquals(2, hotTerms.get(1).getCount());
        assertEquals(2, hotTerms.get(2).getCount());
    }

    private static ExclusiveSelectionResult resultWithSelectedTerms(String... selectedTerms) {
        List<SelectedGroup> groups = new ArrayList<SelectedGroup>();
        for (String selectedTerm : selectedTerms) {
            groups.add(new SelectedGroup(
                    Collections.singletonList(selectedTerm.getBytes(StandardCharsets.UTF_8)),
                    Collections.singletonList(0),
                    1,
                    1
            ));
        }
        return new ExclusiveSelectionResult(groups, 0, 0, 0, 0, false);
    }

    private static Map<String, List<Integer>> toHotTermMap(List<LineFileProcessingResult.HotTermDocList> hotTerms) {
        Map<String, List<Integer>> out = new LinkedHashMap<String, List<Integer>>();
        for (LineFileProcessingResult.HotTermDocList hotTerm : hotTerms) {
            out.put(ByteArrayUtils.toHex(hotTerm.getTerm()), hotTerm.getDocIds());
        }
        return out;
    }

    private static boolean containsTerm(DocTerms row, String value) {
        byte[] target = value.getBytes(StandardCharsets.UTF_8);
        for (byte[] t : row.getTermsUnsafe()) {
            if (Arrays.equals(target, t)) {
                return true;
            }
        }
        return false;
    }

    private static String rowsFingerprint(List<DocTerms> rows) {
        StringBuilder sb = new StringBuilder();
        for (DocTerms row : rows) {
            sb.append(row.getDocId()).append(':');
            for (byte[] t : row.getTermsUnsafe()) {
                sb.append(ByteArrayUtils.toHex(t)).append(',');
            }
            sb.append(';');
        }
        return sb.toString();
    }

    private static String hex(String value) {
        return ByteArrayUtils.toHex(value.getBytes(StandardCharsets.UTF_8));
    }
}
