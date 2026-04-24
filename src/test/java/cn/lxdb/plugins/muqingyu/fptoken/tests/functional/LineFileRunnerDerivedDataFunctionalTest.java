package cn.lxdb.plugins.muqingyu.fptoken.tests.functional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import cn.lxdb.plugins.muqingyu.fptoken.ExclusiveFrequentItemsetSelector;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.DocTerms;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.ExclusiveSelectionResult;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.SelectedGroup;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.util.ByteArrayKey;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.util.ByteArrayUtils;
import cn.lxdb.plugins.muqingyu.fptoken.runner.dataset.LineRecordDatasetLoader;
import cn.lxdb.plugins.muqingyu.fptoken.runner.entry.FptokenLineFileRunnerMain;
import cn.lxdb.plugins.muqingyu.fptoken.runner.ngram.ByteNgramTokenizer;
import cn.lxdb.plugins.muqingyu.fptoken.runner.result.LineFileProcessingResult;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

class LineFileRunnerDerivedDataFunctionalTest {

    @Test
    void runPipeline_shouldProduceAccurateLoadedResultAndDerivedData() throws Exception {
        Path dir = Files.createTempDirectory("fptoken-line-runner-derived-");
        Path input = dir.resolve("input.txt");
        List<String> lines = Arrays.asList(
                "ABCD",
                "ABCD",
                "ABEF",
                "ABEF",
                "XYAB"
        );
        Files.write(input, lines, StandardCharsets.UTF_8);

        LineFileProcessingResult processing = FptokenLineFileRunnerMain.runPipeline(
                dir, input, 2, 2, 2, 2, 1);

        // 1) loaded rows 必须保留原始加载结果
        List<DocTerms> loadedRows = processing.getLoadedRows();
        assertEquals(lines.size(), loadedRows.size());
        LineRecordDatasetLoader.LoadOutcome loaded = LineRecordDatasetLoader.loadSingleFileWithStats(input, 2, 2);
        assertEquals(lines.size(), loaded.getStats().getDocCount());
        assertEquals(lines.size(), loaded.getStats().getTotalLines());

        // 2) result 必须等价于同参数独立执行的处理结果（先在测试侧做同样切词）
        List<DocTerms> tokenizedRows = tokenizeRows(loadedRows, 2, 2);
        ExclusiveSelectionResult direct = ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(
                tokenizedRows, 2, 2, 6, 200000);
        assertEquals(groupsFingerprint(direct.getGroups()), groupsFingerprint(processing.getSelectionResult().getGroups()));
        assertEquals(direct.getIntersectionCount(), processing.getSelectionResult().getIntersectionCount());

        // 3) derived 数据必须是“先统计/再剔除 result 词项”的结果
        Set<ByteArrayKey> selectedTerms = collectSelectedTerms(processing.getSelectionResult());
        // 新命名语义：highFreq* 是高频倒排层，lowHitForwardRows 是低频正排层（skip index 源）。
        LineFileProcessingResult.FinalIndexData finalIndexData = processing.getFinalIndexData();

        List<DocTerms> expectedCutRes = expectedCutRes(tokenizedRows, selectedTerms);
        List<DocTerms> actualCutRes = finalIndexData.getLowHitForwardRows();
        assertEquals(rowsFingerprint(expectedCutRes), rowsFingerprint(actualCutRes));

        Map<String, List<Integer>> expectedHotTerms = expectedHotTerms(tokenizedRows, 1, selectedTerms);
        Map<String, List<Integer>> actualHotTerms = hotTermsMap(finalIndexData.getHighFreqSingleTermPostings());
        assertEquals(expectedHotTerms, actualHotTerms);

        // selected term 必须从 cut_res 与 hot_terms 同时剔除
        for (ByteArrayKey selected : selectedTerms) {
            String hex = ByteArrayUtils.toHex(selected.bytes());
            assertFalse(actualHotTerms.containsKey(hex), "selected term should not exist in hot_terms");
            assertFalse(cutResContainsTerm(actualCutRes, selected.bytes()),
                    "selected term should not exist in cut_res");
        }
    }

    private static Set<ByteArrayKey> collectSelectedTerms(ExclusiveSelectionResult result) {
        Set<ByteArrayKey> out = new LinkedHashSet<ByteArrayKey>();
        for (SelectedGroup g : result.getGroups()) {
            for (byte[] term : g.getTerms()) {
                out.add(new ByteArrayKey(term));
            }
        }
        return out;
    }

    private static List<DocTerms> expectedCutRes(List<DocTerms> loadedRows, Set<ByteArrayKey> selectedTerms) {
        List<DocTerms> out = new ArrayList<DocTerms>(loadedRows.size());
        for (DocTerms row : loadedRows) {
            List<byte[]> filtered = new ArrayList<byte[]>();
            for (byte[] t : row.getTermsUnsafe()) {
                if (!selectedTerms.contains(new ByteArrayKey(t))) {
                    filtered.add(t);
                }
            }
            out.add(new DocTerms(row.getDocId(), filtered));
        }
        return out;
    }

    private static Map<String, List<Integer>> expectedHotTerms(
            List<DocTerms> loadedRows,
            int thresholdExclusive,
            Set<ByteArrayKey> selectedTerms
    ) {
        Map<ByteArrayKey, LinkedHashSet<Integer>> termToDocs = new LinkedHashMap<ByteArrayKey, LinkedHashSet<Integer>>();
        for (DocTerms row : loadedRows) {
            for (byte[] t : row.getTermsUnsafe()) {
                ByteArrayKey key = new ByteArrayKey(t);
                LinkedHashSet<Integer> docs = termToDocs.get(key);
                if (docs == null) {
                    docs = new LinkedHashSet<Integer>();
                    termToDocs.put(key, docs);
                }
                docs.add(row.getDocId());
            }
        }

        Map<String, List<Integer>> out = new LinkedHashMap<String, List<Integer>>();
        for (Map.Entry<ByteArrayKey, LinkedHashSet<Integer>> e : termToDocs.entrySet()) {
            if (e.getValue().size() <= thresholdExclusive) {
                continue;
            }
            if (selectedTerms.contains(e.getKey())) {
                continue;
            }
            out.put(ByteArrayUtils.toHex(e.getKey().bytes()), new ArrayList<Integer>(e.getValue()));
        }
        return out;
    }

    private static Map<String, List<Integer>> hotTermsMap(
            List<LineFileProcessingResult.HotTermDocList> hotTerms
    ) {
        Map<String, List<Integer>> out = new LinkedHashMap<String, List<Integer>>();
        for (LineFileProcessingResult.HotTermDocList item : hotTerms) {
            out.put(ByteArrayUtils.toHex(item.getTerm()), item.getDocIds());
        }
        return out;
    }

    private static boolean cutResContainsTerm(List<DocTerms> rows, byte[] term) {
        for (DocTerms row : rows) {
            for (byte[] t : row.getTermsUnsafe()) {
                if (Arrays.equals(t, term)) {
                    return true;
                }
            }
        }
        return false;
    }

    private static String groupsFingerprint(List<SelectedGroup> groups) {
        StringBuilder sb = new StringBuilder();
        for (SelectedGroup g : groups) {
            sb.append('{');
            for (byte[] term : g.getTerms()) {
                sb.append(ByteArrayUtils.toHex(term)).append(',');
            }
            sb.append('|').append(g.getSupport()).append('}');
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

    private static List<DocTerms> tokenizeRows(List<DocTerms> rawRows, int ngramStart, int ngramEnd) {
        List<DocTerms> out = new ArrayList<DocTerms>(rawRows.size());
        for (DocTerms row : rawRows) {
            List<byte[]> terms = row.getTermsUnsafe();
            byte[] raw = terms.isEmpty() ? new byte[0] : terms.get(0);
            out.add(new DocTerms(row.getDocId(), ByteNgramTokenizer.tokenize(raw, ngramStart, ngramEnd)));
        }
        return out;
    }
}
