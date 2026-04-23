package cn.lxdb.plugins.muqingyu.fptoken.tests.performance;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import cn.lxdb.plugins.muqingyu.fptoken.ExclusiveFrequentItemsetSelector;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.DocTerms;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.ExclusiveSelectionResult;
import cn.lxdb.plugins.muqingyu.fptoken.runner.dataset.LineRecordDatasetLoader;
import java.io.BufferedWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

/**
 * 上限文件（64B/行 + 32000 行上限）性能基准。
 */
@Tag("performance")
@EnabledIfSystemProperty(named = "fptoken.runPerfTests", matches = "true")
@Timeout(value = 90, unit = TimeUnit.SECONDS)
class MaxLengthFilePerformanceTest {

    @Test
    void PERF_FILE_MAXLEN_001_sampledPipeline_on64ByteRows_shouldStayWithinBudget() throws Exception {
        Path dir = Files.createTempDirectory("fptoken-perf-maxlen-001-");
        Path file = dir.resolve("max64.txt");
        int lineCount = PerfTestSupport.intProp("fptoken.perf.maxlen.case1.lines", 12000);
        writeLengthControlledFile(file, lineCount, 64, 0, 16);

        final LineRecordDatasetLoader.LoadedDataset[] holder = new LineRecordDatasetLoader.LoadedDataset[1];
        long loadMs = PerfTestSupport.elapsedMillis(() ->
                holder[0] = uncheckedLoad(file, 2, 4));
        List<DocTerms> rows = holder[0].getRows();
        assertEquals(lineCount, rows.size());

        long mineMs = runSelector(rows, 0.30d, 120, 0.0d, 260, 2);
        long loadBudgetMs = PerfTestSupport.longProp("fptoken.perf.maxlen.case1.loadBudgetMs", 5000L);
        long mineBudgetMs = PerfTestSupport.longProp("fptoken.perf.maxlen.case1.mineBudgetMs", 12000L);
        assertTrue(loadMs <= loadBudgetMs, () -> "loadMs=" + loadMs + ", budget=" + loadBudgetMs);
        assertTrue(mineMs <= mineBudgetMs, () -> "mineMs=" + mineMs + ", budget=" + mineBudgetMs);
    }

    @Test
    void PERF_FILE_MAXLEN_002_sampledVsFull_on64ByteRows_shouldNotRegressTooMuch() throws Exception {
        Path dir = Files.createTempDirectory("fptoken-perf-maxlen-002-");
        Path file = dir.resolve("max64-compare.txt");
        int lineCount = PerfTestSupport.intProp("fptoken.perf.maxlen.case2.lines", 8000);
        writeLengthControlledFile(file, lineCount, 64, 0, 16);

        List<DocTerms> rows = LineRecordDatasetLoader.loadSingleFile(file, 2, 4).getRows();
        assertEquals(lineCount, rows.size());

        long sampledMs = runSelector(rows, 0.35d, 100, 0.0d, 220, 2);
        long baselineMs = runSelector(rows, 1.0d, 1, 1.0d, 220, 2);

        long sampledBudgetMs = PerfTestSupport.longProp("fptoken.perf.maxlen.case2.sampledBudgetMs", 12000L);
        long baselineBudgetMs = PerfTestSupport.longProp("fptoken.perf.maxlen.case2.fullBudgetMs", 15000L);
        long slackMs = PerfTestSupport.longProp("fptoken.perf.maxlen.case2.slackMs", 4000L);
        assertTrue(sampledMs <= sampledBudgetMs, () -> "sampledMs=" + sampledMs + ", budget=" + sampledBudgetMs);
        assertTrue(baselineMs <= baselineBudgetMs, () -> "baselineMs=" + baselineMs + ", budget=" + baselineBudgetMs);
        assertTrue(sampledMs <= baselineMs + slackMs,
                () -> "sampled=" + sampledMs + ", baseline=" + baselineMs + ", slack=" + slackMs);
    }

    @Test
    @EnabledIfSystemProperty(named = "fptoken.runScaleTests", matches = "true")
    void PERF_FILE_MAXLEN_003_capAndTruncateBoundary_shouldStayHealthy() throws Exception {
        Path dir = Files.createTempDirectory("fptoken-perf-maxlen-003-");
        Path file = dir.resolve("overflow-cap.txt");
        writeLengthControlledFile(file, 32100, 64, 20, 24);

        LineRecordDatasetLoader.LoadOutcome outcome = LineRecordDatasetLoader.loadSingleFileWithStats(file, 2, 4);
        assertEquals(LineRecordDatasetLoader.MAX_LINES_PER_FILE, outcome.getLoadedDataset().getRows().size());
        assertTrue(outcome.getStats().getTruncatedLines() > 0);
        assertTrue(outcome.getStats().getDroppedByCap() > 0);

        long sampledMs = runSelector(
                outcome.getLoadedDataset().getRows(), 0.30d, 180, 0.0d, 360, 2);
        long budgetMs = PerfTestSupport.longProp("fptoken.perf.maxlen.case3.mineBudgetMs", 30000L);
        assertTrue(sampledMs <= budgetMs, () -> "sampledMs=" + sampledMs + ", budget=" + budgetMs);
    }

    @Test
    @EnabledIfSystemProperty(named = "fptoken.runScaleTests", matches = "true")
    void PERF_FILE_MAXLEN_004_lengthAndLineCap_matrix_shouldStayWithinBudget() throws Exception {
        Path dir = Files.createTempDirectory("fptoken-perf-maxlen-004-");
        int nearCapLines = PerfTestSupport.intProp("fptoken.perf.maxlen.case4.nearCapLines", 30000);
        int overCapLines = PerfTestSupport.intProp("fptoken.perf.maxlen.case4.overCapLines", 32100);
        MatrixCase[] cases = new MatrixCase[] {
                new MatrixCase("exact64_nearCap", nearCapLines, 64, 0),
                new MatrixCase("exact64_overCap", overCapLines, 64, 0),
                new MatrixCase("over64_nearCap", nearCapLines, 64, 20),
                new MatrixCase("over64_overCap", overCapLines, 64, 20)
        };

        long loadTotalMs = 0L;
        long sampledTotalMs = 0L;
        long baselineTotalMs = 0L;
        for (MatrixCase matrixCase : cases) {
            Path file = dir.resolve(matrixCase.name + ".txt");
            writeLengthControlledFile(file, matrixCase.lineCount, matrixCase.baseLen, matrixCase.overflowExtra, 32);

            final LineRecordDatasetLoader.LoadOutcome[] holder = new LineRecordDatasetLoader.LoadOutcome[1];
            long loadMs = PerfTestSupport.elapsedMillis(() ->
                    holder[0] = uncheckedLoadWithStats(file, 2, 4));
            loadTotalMs += loadMs;

            LineRecordDatasetLoader.LoadOutcome outcome = holder[0];
            int expectedDocs = Math.min(matrixCase.lineCount, LineRecordDatasetLoader.MAX_LINES_PER_FILE);
            assertEquals(expectedDocs, outcome.getLoadedDataset().getRows().size());
            if (matrixCase.lineCount > LineRecordDatasetLoader.MAX_LINES_PER_FILE) {
                assertTrue(outcome.getStats().getDroppedByCap() > 0L, matrixCase.name);
            }
            if (matrixCase.overflowExtra > 0) {
                assertTrue(outcome.getStats().getTruncatedLines() > 0L, matrixCase.name);
            }

            long sampledMs = runSelector(
                    outcome.getLoadedDataset().getRows(), 0.30d, 160, 0.0d, 320, 2);
            long baselineMs = runSelector(
                    outcome.getLoadedDataset().getRows(), 1.0d, 1, 1.0d, 320, 2);
            sampledTotalMs += sampledMs;
            baselineTotalMs += baselineMs;
        }

        long loadBudgetMs = PerfTestSupport.longProp("fptoken.perf.maxlen.case4.loadBudgetMs", 40000L);
        long sampledBudgetMs = PerfTestSupport.longProp("fptoken.perf.maxlen.case4.sampledBudgetMs", 70000L);
        long baselineBudgetMs = PerfTestSupport.longProp("fptoken.perf.maxlen.case4.fullBudgetMs", 90000L);
        long slackMs = PerfTestSupport.longProp("fptoken.perf.maxlen.case4.slackMs", 12000L);
        assertTrue(loadTotalMs <= loadBudgetMs, "loadTotalMs=" + loadTotalMs + ", budget=" + loadBudgetMs);
        assertTrue(sampledTotalMs <= sampledBudgetMs,
                "sampledTotalMs=" + sampledTotalMs + ", budget=" + sampledBudgetMs);
        assertTrue(baselineTotalMs <= baselineBudgetMs,
                "baselineTotalMs=" + baselineTotalMs + ", budget=" + baselineBudgetMs);
        assertTrue(sampledTotalMs <= baselineTotalMs + slackMs,
                "sampledTotalMs=" + sampledTotalMs + ", baselineTotalMs=" + baselineTotalMs + ", slack=" + slackMs);
    }

    @Test
    @EnabledIfSystemProperty(named = "fptoken.runScaleTests", matches = "true")
    void PERF_FILE_MAXLEN_005_featureDiverse_matrix_shouldStayWithinBudget() throws Exception {
        Path dir = Files.createTempDirectory("fptoken-perf-maxlen-005-");
        int nearCapLines = PerfTestSupport.intProp("fptoken.perf.maxlen.case5.nearCapLines", 22000);
        int overCapLines = PerfTestSupport.intProp("fptoken.perf.maxlen.case5.overCapLines", 32500);
        FeatureCase[] cases = new FeatureCase[] {
                new FeatureCase("repeat_exact64_nearCap", nearCapLines, 64, 0, DataProfile.HIGH_REPEAT, 13),
                new FeatureCase("repeat_over64_overCap", overCapLines, 64, 20, DataProfile.HIGH_REPEAT, 13),
                new FeatureCase("unique_exact64_nearCap", nearCapLines, 64, 0, DataProfile.HIGH_UNIQUE, 29),
                new FeatureCase("unique_over64_overCap", overCapLines, 64, 20, DataProfile.HIGH_UNIQUE, 29),
                new FeatureCase("bursty_exact64_nearCap", nearCapLines, 64, 0, DataProfile.BURSTY_HOTSPOT, 17),
                new FeatureCase("bursty_over64_overCap", overCapLines, 64, 20, DataProfile.BURSTY_HOTSPOT, 17),
                new FeatureCase("mixed_exact64_nearCap", nearCapLines, 64, 0, DataProfile.MIXED_EMPTY_SHORT, 31),
                new FeatureCase("mixed_over64_overCap", overCapLines, 64, 20, DataProfile.MIXED_EMPTY_SHORT, 31)
        };

        long loadTotalMs = 0L;
        long sampledTotalMs = 0L;
        long baselineTotalMs = 0L;
        for (FeatureCase featureCase : cases) {
            Path file = dir.resolve(featureCase.name + ".txt");
            writeProfiledFile(
                    file,
                    featureCase.lineCount,
                    featureCase.baseLen,
                    featureCase.overflowExtra,
                    featureCase.profile,
                    featureCase.seed
            );

            final LineRecordDatasetLoader.LoadOutcome[] holder = new LineRecordDatasetLoader.LoadOutcome[1];
            long loadMs = PerfTestSupport.elapsedMillis(() ->
                    holder[0] = uncheckedLoadWithStats(file, 2, 4));
            loadTotalMs += loadMs;

            LineRecordDatasetLoader.LoadOutcome outcome = holder[0];
            int expectedDocs = Math.min(featureCase.lineCount, LineRecordDatasetLoader.MAX_LINES_PER_FILE);
            assertEquals(expectedDocs, outcome.getLoadedDataset().getRows().size(), featureCase.name);
            if (featureCase.lineCount > LineRecordDatasetLoader.MAX_LINES_PER_FILE) {
                assertTrue(outcome.getStats().getDroppedByCap() > 0L, featureCase.name);
            }
            if (featureCase.overflowExtra > 0) {
                assertTrue(outcome.getStats().getTruncatedLines() > 0L, featureCase.name);
            }

            long sampledMs = runSelector(
                    outcome.getLoadedDataset().getRows(), 0.30d, 180, 0.0d, 320, 2);
            long baselineMs = runSelector(
                    outcome.getLoadedDataset().getRows(), 1.0d, 1, 1.0d, 320, 2);
            sampledTotalMs += sampledMs;
            baselineTotalMs += baselineMs;
        }

        long loadBudgetMs = PerfTestSupport.longProp("fptoken.perf.maxlen.case5.loadBudgetMs", 90000L);
        long sampledBudgetMs = PerfTestSupport.longProp("fptoken.perf.maxlen.case5.sampledBudgetMs", 160000L);
        long baselineBudgetMs = PerfTestSupport.longProp("fptoken.perf.maxlen.case5.fullBudgetMs", 220000L);
        long slackMs = PerfTestSupport.longProp("fptoken.perf.maxlen.case5.slackMs", 30000L);
        assertTrue(loadTotalMs <= loadBudgetMs, "loadTotalMs=" + loadTotalMs + ", budget=" + loadBudgetMs);
        assertTrue(sampledTotalMs <= sampledBudgetMs,
                "sampledTotalMs=" + sampledTotalMs + ", budget=" + sampledBudgetMs);
        assertTrue(baselineTotalMs <= baselineBudgetMs,
                "baselineTotalMs=" + baselineTotalMs + ", budget=" + baselineBudgetMs);
        assertTrue(sampledTotalMs <= baselineTotalMs + slackMs,
                "sampledTotalMs=" + sampledTotalMs + ", baselineTotalMs=" + baselineTotalMs + ", slack=" + slackMs);
    }

    private static long runSelector(
            List<DocTerms> rows,
            double sampleRatio,
            int minSampleCount,
            double supportScale,
            int minSupport,
            int minItemsetSize
    ) {
        double oldRatio = ExclusiveFrequentItemsetSelector.getSampleRatio();
        int oldMin = ExclusiveFrequentItemsetSelector.getMinSampleCount();
        double oldScale = ExclusiveFrequentItemsetSelector.getSamplingSupportScale();
        try {
            ExclusiveFrequentItemsetSelector.setSampleRatio(sampleRatio);
            ExclusiveFrequentItemsetSelector.setMinSampleCount(minSampleCount);
            ExclusiveFrequentItemsetSelector.setSamplingSupportScale(supportScale);

            final ExclusiveSelectionResult[] holder = new ExclusiveSelectionResult[1];
            long elapsedMs = PerfTestSupport.elapsedMillis(() -> holder[0] =
                    ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(
                            rows, minSupport, minItemsetSize, 4, 160_000));
            assertTrue(holder[0] != null);
            assertTrue(holder[0].getCandidateCount() >= 0);
            return elapsedMs;
        } finally {
            ExclusiveFrequentItemsetSelector.setSampleRatio(oldRatio);
            ExclusiveFrequentItemsetSelector.setMinSampleCount(oldMin);
            ExclusiveFrequentItemsetSelector.setSamplingSupportScale(oldScale);
        }
    }

    private static void writeLengthControlledFile(
            Path file,
            int lineCount,
            int lineLen,
            int overflowExtra,
            int groups
    ) throws Exception {
        Files.createDirectories(file.getParent());
        try (BufferedWriter writer = Files.newBufferedWriter(file, StandardCharsets.UTF_8)) {
            for (int i = 0; i < lineCount; i++) {
                int group = i % Math.max(1, groups);
                int targetLen = lineLen + overflowExtra;
                String seed = "COMMON_GROUP_" + group + "_";
                writer.write(fixedLenAscii(seed, targetLen, i));
                writer.newLine();
            }
        }
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

    private static void writeProfiledFile(
            Path file,
            int lineCount,
            int lineLen,
            int overflowExtra,
            DataProfile profile,
            int seed
    ) throws Exception {
        Files.createDirectories(file.getParent());
        try (BufferedWriter writer = Files.newBufferedWriter(file, StandardCharsets.UTF_8)) {
            for (int i = 0; i < lineCount; i++) {
                int targetLen = lineLen + overflowExtra;
                String line = buildProfiledLine(profile, i, targetLen, seed);
                writer.write(line);
                writer.newLine();
            }
        }
    }

    private static String buildProfiledLine(DataProfile profile, int index, int targetLen, int seed) {
        if (profile == DataProfile.MIXED_EMPTY_SHORT) {
            if (index % 97 == 0) {
                return "";
            }
            if (index % 11 == 0) {
                int shortLen = Math.max(4, Math.min(32, targetLen / 2));
                return fixedLenAscii("MIX_SHORT_" + (index % 7) + "_", shortLen, seed + index);
            }
        }
        if (profile == DataProfile.HIGH_REPEAT) {
            return fixedLenAscii("REP_G" + (index % 6) + "_", targetLen, seed + (index % 13));
        }
        if (profile == DataProfile.HIGH_UNIQUE) {
            return fixedLenAscii("UNQ_" + index + "_", targetLen, seed + index * 3);
        }
        if (profile == DataProfile.BURSTY_HOTSPOT) {
            int burstGroup = ((index / 256) % 3 == 0) ? (index % 4) : (index % 32);
            return fixedLenAscii("BURST_G" + burstGroup + "_", targetLen, seed + index);
        }
        return fixedLenAscii("DFT_" + index + "_", targetLen, seed + index);
    }

    private static LineRecordDatasetLoader.LoadedDataset uncheckedLoad(Path file, int ngramStart, int ngramEnd) {
        try {
            return LineRecordDatasetLoader.loadSingleFile(file, ngramStart, ngramEnd);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static LineRecordDatasetLoader.LoadOutcome uncheckedLoadWithStats(
            Path file, int ngramStart, int ngramEnd
    ) {
        try {
            return LineRecordDatasetLoader.loadSingleFileWithStats(file, ngramStart, ngramEnd);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static final class MatrixCase {
        private final String name;
        private final int lineCount;
        private final int baseLen;
        private final int overflowExtra;

        private MatrixCase(String name, int lineCount, int baseLen, int overflowExtra) {
            this.name = name;
            this.lineCount = lineCount;
            this.baseLen = baseLen;
            this.overflowExtra = overflowExtra;
        }
    }

    private static final class FeatureCase {
        private final String name;
        private final int lineCount;
        private final int baseLen;
        private final int overflowExtra;
        private final DataProfile profile;
        private final int seed;

        private FeatureCase(
                String name,
                int lineCount,
                int baseLen,
                int overflowExtra,
                DataProfile profile,
                int seed
        ) {
            this.name = name;
            this.lineCount = lineCount;
            this.baseLen = baseLen;
            this.overflowExtra = overflowExtra;
            this.profile = profile;
            this.seed = seed;
        }
    }

    private enum DataProfile {
        HIGH_REPEAT,
        HIGH_UNIQUE,
        BURSTY_HOTSPOT,
        MIXED_EMPTY_SHORT
    }
}
