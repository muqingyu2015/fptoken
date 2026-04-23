package cn.lxdb.plugins.muqingyu.fptoken.tests.performance;

import static org.junit.jupiter.api.Assertions.assertTrue;

import cn.lxdb.plugins.muqingyu.fptoken.api.ExclusiveFpRowsProcessingApi;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.DocTerms;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.ExclusiveSelectionResult;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.SelectedGroup;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.util.ByteArrayKey;
import cn.lxdb.plugins.muqingyu.fptoken.runner.result.LineFileProcessingResult;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

@Tag("performance")
@EnabledIfSystemProperty(named = "fptoken.runPerfTests", matches = "true")
@Timeout(value = 90, unit = TimeUnit.SECONDS)
class LxdbDualIndexBuildPerformanceTest {

    @Test
    void PERF_LXDB_IDX_001_dualIndexBuild_shouldStayWithinBudget_forDiverseProfiles() {
        List<ProfileCase> cases = new ArrayList<ProfileCase>();
        cases.add(new ProfileCase("repeat", 12000, DataProfile.HIGH_REPEAT));
        cases.add(new ProfileCase("unique", 12000, DataProfile.HIGH_UNIQUE));
        cases.add(new ProfileCase("bursty", 12000, DataProfile.BURSTY_HOTSPOT));
        cases.add(new ProfileCase("mixed", 12000, DataProfile.MIXED_EMPTY_SHORT));

        long processTotalMs = 0L;
        long indexBuildTotalMs = 0L;
        for (ProfileCase profileCase : cases) {
            List<DocTerms> rows = buildRows(profileCase.lineCount, profileCase.profile, 64, 17);
            final Ref<LineFileProcessingResult> holder = new Ref<LineFileProcessingResult>();
            long processMs = PerfTestSupport.elapsedMillis(() ->
                    holder.value = ExclusiveFpRowsProcessingApi.processRows(rows, 160, 2, 20));
            processTotalMs += processMs;

            final Ref<Map<ByteArrayKey, LinkedHashSet<Integer>>> compressedHolder =
                    new Ref<Map<ByteArrayKey, LinkedHashSet<Integer>>>();
            final Ref<Map<ByteArrayKey, LinkedHashSet<Integer>>> skipHolder =
                    new Ref<Map<ByteArrayKey, LinkedHashSet<Integer>>>();
            long buildMs = PerfTestSupport.elapsedMillis(() -> {
                LineFileProcessingResult.FinalIndexData finalIndexData = holder.value.getFinalIndexData();
                compressedHolder.value = buildCompressedIndex(
                        finalIndexData.getHighFreqMutexGroupPostings(),
                        finalIndexData.getHighFreqSingleTermPostings());
                skipHolder.value = buildSkipIndex(
                        finalIndexData.getLowHitForwardRows(),
                        compressedHolder.value.keySet());
            });
            indexBuildTotalMs += buildMs;

            assertTrue(!compressedHolder.value.isEmpty() || !skipHolder.value.isEmpty(),
                    "both indexes are empty for profile: " + profileCase.name);
        }

        long processBudgetMs = PerfTestSupport.longProp("fptoken.perf.lxdb.idx.processBudgetMs", 60000L);
        long buildBudgetMs = PerfTestSupport.longProp("fptoken.perf.lxdb.idx.buildBudgetMs", 35000L);
        assertTrue(processTotalMs <= processBudgetMs,
                "processTotalMs=" + processTotalMs + ", budget=" + processBudgetMs);
        assertTrue(indexBuildTotalMs <= buildBudgetMs,
                "indexBuildTotalMs=" + indexBuildTotalMs + ", budget=" + buildBudgetMs);
    }

    @Test
    @EnabledIfSystemProperty(named = "fptoken.runScaleTests", matches = "true")
    void PERF_LXDB_IDX_002_dualIndexBuild_scaleMatrix_shouldStayWithinBudget() {
        int[] scales = new int[] {8000, 16000, 24000, 32000};
        long totalMs = 0L;
        for (int scale : scales) {
            List<DocTerms> rows = buildRows(scale, DataProfile.BURSTY_HOTSPOT, 64, 31);
            final Ref<LineFileProcessingResult> holder = new Ref<LineFileProcessingResult>();
            long elapsedMs = PerfTestSupport.elapsedMillis(() -> {
                holder.value = ExclusiveFpRowsProcessingApi.processRows(rows, 220, 2, 24);
                LineFileProcessingResult.FinalIndexData finalIndexData = holder.value.getFinalIndexData();
                Map<ByteArrayKey, LinkedHashSet<Integer>> compressed =
                        buildCompressedIndex(
                                finalIndexData.getHighFreqMutexGroupPostings(),
                                finalIndexData.getHighFreqSingleTermPostings());
                buildSkipIndex(finalIndexData.getLowHitForwardRows(), compressed.keySet());
            });
            totalMs += elapsedMs;
        }

        long budgetMs = PerfTestSupport.longProp("fptoken.perf.lxdb.idx.scaleBudgetMs", 140000L);
        assertTrue(totalMs <= budgetMs, "totalMs=" + totalMs + ", budget=" + budgetMs);
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
            java.util.Set<ByteArrayKey> excludedTerms
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

    private static List<DocTerms> buildRows(int lineCount, DataProfile profile, int lineLen, int seed) {
        List<DocTerms> rows = new ArrayList<DocTerms>(lineCount);
        for (int i = 0; i < lineCount; i++) {
            String line = buildProfiledLine(profile, i, lineLen, seed);
            byte[] bytes = line.getBytes(StandardCharsets.UTF_8);
            rows.add(new DocTerms(i, cn.lxdb.plugins.muqingyu.fptoken.runner.ngram.ByteNgramTokenizer.tokenize(bytes, 2, 4)));
        }
        return rows;
    }

    private static String buildProfiledLine(DataProfile profile, int index, int targetLen, int seed) {
        if (profile == DataProfile.MIXED_EMPTY_SHORT) {
            if (index % 113 == 0) {
                return "";
            }
            if (index % 9 == 0) {
                int shortLen = Math.max(4, Math.min(28, targetLen / 2));
                return fixedLenAscii("MIX_" + (index % 7) + "_", shortLen, seed + index);
            }
        }
        if (profile == DataProfile.HIGH_REPEAT) {
            return fixedLenAscii("REP_" + (index % 6) + "_", targetLen, seed + index);
        }
        if (profile == DataProfile.HIGH_UNIQUE) {
            return fixedLenAscii("UNQ_" + index + "_", targetLen, seed + index * 11);
        }
        if (profile == DataProfile.BURSTY_HOTSPOT) {
            int hot = ((index / 192) % 4 == 0) ? (index % 4) : (index % 32);
            return fixedLenAscii("BURST_" + hot + "_", targetLen, seed + index);
        }
        return fixedLenAscii("DEF_" + index + "_", targetLen, seed + index);
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

    private static final class ProfileCase {
        private final String name;
        private final int lineCount;
        private final DataProfile profile;

        private ProfileCase(String name, int lineCount, DataProfile profile) {
            this.name = name;
            this.lineCount = lineCount;
            this.profile = profile;
        }
    }

    private enum DataProfile {
        HIGH_REPEAT,
        HIGH_UNIQUE,
        BURSTY_HOTSPOT,
        MIXED_EMPTY_SHORT
    }

    private static final class Ref<T> {
        private T value;
    }
}
