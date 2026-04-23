package cn.lxdb.plugins.muqingyu.fptoken.tests.functional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import cn.lxdb.plugins.muqingyu.fptoken.api.ExclusiveFpRowsProcessingApi;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.DocTerms;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.ExclusiveSelectionResult;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.SelectedGroup;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.util.ByteArrayKey;
import cn.lxdb.plugins.muqingyu.fptoken.runner.dataset.LineRecordDatasetLoader;
import cn.lxdb.plugins.muqingyu.fptoken.runner.result.LineFileProcessingResult;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

class LxdbDualIndexBuildFunctionalTest {

    @Test
    void derivedAndSelection_shouldBuildDisjointAndCompleteDualIndexes() throws Exception {
        Path dir = Files.createTempDirectory("fptoken-lxdb-dual-functional-");
        List<Scenario> scenarios = new ArrayList<Scenario>();
        scenarios.add(new Scenario("repeat.txt", 1400, DataProfile.HIGH_REPEAT, 0));
        scenarios.add(new Scenario("unique.txt", 1400, DataProfile.HIGH_UNIQUE, 0));
        scenarios.add(new Scenario("mixed.txt", 1800, DataProfile.MIXED_EMPTY_SHORT, 20));

        for (Scenario scenario : scenarios) {
            Path file = dir.resolve(scenario.name);
            writeProfiledFile(file, scenario.lineCount, 64, scenario.overflowExtra, scenario.profile, 23);

            List<DocTerms> rows = LineRecordDatasetLoader.loadSingleFile(file, 2, 4).getRows();
            LineFileProcessingResult processing = ExclusiveFpRowsProcessingApi.processRows(rows, 80, 2, 16);
            LineFileProcessingResult.FinalIndexData finalIndexData = processing.getFinalIndexData();

            // 高频层倒排：互斥组合倒排 + 单词倒排。
            Map<ByteArrayKey, LinkedHashSet<Integer>> compressedIndex =
                    buildCompressedIndex(
                            finalIndexData.getHighFreqMutexGroupPostings(),
                            finalIndexData.getHighFreqSingleTermPostings());
            // 低频层正排：使用 lowHitForwardRows 构建 skip index。
            Map<ByteArrayKey, LinkedHashSet<Integer>> skipIndex =
                    buildSkipIndex(finalIndexData.getLowHitForwardRows(), compressedIndex.keySet());
            Map<ByteArrayKey, LinkedHashSet<Integer>> sourceIndex = buildSkipIndex(processing.getLoadedRows());
            Set<ByteArrayKey> selectedTerms = collectSelectedTerms(processing.getSelectionResult());

            // 1) 主压缩索引与 skip 索引应按 term 空间互斥。
            Set<ByteArrayKey> overlap = new LinkedHashSet<ByteArrayKey>(compressedIndex.keySet());
            overlap.retainAll(skipIndex.keySet());
            assertTrue(overlap.isEmpty(), "compressed and skip index should be disjoint: " + scenario.name);

            // 2) 两层索引联合后应覆盖原始 rows 的词项集合。
            Set<ByteArrayKey> union = new LinkedHashSet<ByteArrayKey>(compressedIndex.keySet());
            union.addAll(skipIndex.keySet());
            assertEquals(sourceIndex.keySet(), union, "union term set mismatch: " + scenario.name);

            // 3) 对于每个词项，联合索引里的 docId 集合必须与原始 rows 一致。
            for (Map.Entry<ByteArrayKey, LinkedHashSet<Integer>> entry : sourceIndex.entrySet()) {
                ByteArrayKey term = entry.getKey();
                LinkedHashSet<Integer> docsInUnion = compressedIndex.containsKey(term)
                        ? compressedIndex.get(term)
                        : skipIndex.get(term);
                assertTrue(docsInUnion != null, "missing term in dual indexes: " + scenario.name);
                if (selectedTerms.contains(term)) {
                    // selected group 的 docIds 是“模式命中集合”，不是该 term 的完整倒排，因此只校验子集关系。
                    assertTrue(entry.getValue().containsAll(docsInUnion),
                            "selected term docs should be subset of source docs: " + scenario.name);
                } else {
                    assertEquals(entry.getValue(), docsInUnion, "docId set mismatch: " + scenario.name);
                }
            }

            // 4) selectionResult 中的词项必须落在主压缩索引，不应再出现在 skip index。
            assertSelectedTermsInCompressedOnly(processing.getSelectionResult(), compressedIndex, skipIndex);
            // 5) hotTerms 也必须落在主压缩索引，不应再出现在 skip index。
            assertHotTermsInCompressedOnly(finalIndexData.getHighFreqSingleTermPostings(), compressedIndex, skipIndex);
        }
    }

    private static void assertSelectedTermsInCompressedOnly(
            ExclusiveSelectionResult result,
            Map<ByteArrayKey, LinkedHashSet<Integer>> compressedIndex,
            Map<ByteArrayKey, LinkedHashSet<Integer>> skipIndex
    ) {
        for (SelectedGroup group : result.getGroups()) {
            for (byte[] termBytes : group.getTerms()) {
                ByteArrayKey term = new ByteArrayKey(termBytes);
                assertTrue(compressedIndex.containsKey(term), "selected term missing in compressed");
                assertFalse(skipIndex.containsKey(term), "selected term should not exist in skip");
            }
        }
    }

    private static Set<ByteArrayKey> collectSelectedTerms(ExclusiveSelectionResult result) {
        Set<ByteArrayKey> out = new LinkedHashSet<ByteArrayKey>();
        for (SelectedGroup group : result.getGroups()) {
            for (byte[] termBytes : group.getTerms()) {
                out.add(new ByteArrayKey(termBytes));
            }
        }
        return out;
    }

    private static void assertHotTermsInCompressedOnly(
            List<LineFileProcessingResult.HotTermDocList> hotTerms,
            Map<ByteArrayKey, LinkedHashSet<Integer>> compressedIndex,
            Map<ByteArrayKey, LinkedHashSet<Integer>> skipIndex
    ) {
        for (LineFileProcessingResult.HotTermDocList hotTerm : hotTerms) {
            ByteArrayKey term = new ByteArrayKey(hotTerm.getTerm());
            assertTrue(compressedIndex.containsKey(term), "hot term missing in compressed");
            assertFalse(skipIndex.containsKey(term), "hot term should not exist in skip");
        }
    }

    private static Map<ByteArrayKey, LinkedHashSet<Integer>> buildCompressedIndex(
            List<SelectedGroup> mutexGroups,
            List<LineFileProcessingResult.HotTermDocList> hotTerms
    ) {
        Map<ByteArrayKey, LinkedHashSet<Integer>> out = new LinkedHashMap<ByteArrayKey, LinkedHashSet<Integer>>();
        for (SelectedGroup group : mutexGroups) {
            LinkedHashSet<Integer> groupDocs = new LinkedHashSet<Integer>(group.getDocIds());
            for (byte[] termBytes : group.getTerms()) {
                ByteArrayKey term = new ByteArrayKey(termBytes);
                LinkedHashSet<Integer> docs = out.get(term);
                if (docs == null) {
                    docs = new LinkedHashSet<Integer>();
                    out.put(term, docs);
                }
                docs.addAll(groupDocs);
            }
        }
        for (LineFileProcessingResult.HotTermDocList hotTerm : hotTerms) {
            ByteArrayKey term = new ByteArrayKey(hotTerm.getTerm());
            LinkedHashSet<Integer> docs = out.get(term);
            if (docs == null) {
                docs = new LinkedHashSet<Integer>();
                out.put(term, docs);
            }
            docs.addAll(hotTerm.getDocIds());
        }
        return out;
    }

    private static Map<ByteArrayKey, LinkedHashSet<Integer>> buildSkipIndex(List<DocTerms> rows) {
        return buildSkipIndex(rows, java.util.Collections.<ByteArrayKey>emptySet());
    }

    private static Map<ByteArrayKey, LinkedHashSet<Integer>> buildSkipIndex(
            List<DocTerms> rows,
            Set<ByteArrayKey> excludedTerms
    ) {
        Map<ByteArrayKey, LinkedHashSet<Integer>> out = new LinkedHashMap<ByteArrayKey, LinkedHashSet<Integer>>();
        for (DocTerms row : rows) {
            int docId = row.getDocId();
            for (byte[] termBytes : row.getTermsUnsafe()) {
                ByteArrayKey term = new ByteArrayKey(termBytes);
                if (excludedTerms.contains(term)) {
                    continue;
                }
                LinkedHashSet<Integer> docs = out.get(term);
                if (docs == null) {
                    docs = new LinkedHashSet<Integer>();
                    out.put(term, docs);
                }
                docs.add(docId);
            }
        }
        return out;
    }

    private static void writeProfiledFile(
            Path file,
            int lineCount,
            int lineLen,
            int overflowExtra,
            DataProfile profile,
            int seed
    ) throws Exception {
        Files.createDirectories(file.getParent());
        List<String> lines = new ArrayList<String>(lineCount);
        for (int i = 0; i < lineCount; i++) {
            int targetLen = lineLen + overflowExtra;
            lines.add(buildProfiledLine(profile, i, targetLen, seed));
        }
        Files.write(file, lines, StandardCharsets.UTF_8);
    }

    private static String buildProfiledLine(DataProfile profile, int index, int targetLen, int seed) {
        if (profile == DataProfile.MIXED_EMPTY_SHORT) {
            if (index % 53 == 0) {
                return "";
            }
            if (index % 7 == 0) {
                int shortLen = Math.max(4, Math.min(20, targetLen / 3));
                return fixedLenAscii("MIX_" + (index % 5) + "_", shortLen, seed + index);
            }
        }
        if (profile == DataProfile.HIGH_REPEAT) {
            return fixedLenAscii("REP_" + (index % 8) + "_", targetLen, seed + index);
        }
        if (profile == DataProfile.HIGH_UNIQUE) {
            return fixedLenAscii("UNQ_" + index + "_", targetLen, seed + index * 5);
        }
        return fixedLenAscii("DFT_" + index + "_", targetLen, seed + index);
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

    private static final class Scenario {
        private final String name;
        private final int lineCount;
        private final DataProfile profile;
        private final int overflowExtra;

        private Scenario(String name, int lineCount, DataProfile profile, int overflowExtra) {
            this.name = name;
            this.lineCount = lineCount;
            this.profile = profile;
            this.overflowExtra = overflowExtra;
        }
    }

    private enum DataProfile {
        HIGH_REPEAT,
        HIGH_UNIQUE,
        MIXED_EMPTY_SHORT
    }
}
