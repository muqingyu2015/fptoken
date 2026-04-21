package cn.lxdb.plugins.muqingyu.fptoken.tests.performance;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import cn.lxdb.plugins.muqingyu.fptoken.ExclusiveFrequentItemsetSelector;
import cn.lxdb.plugins.muqingyu.fptoken.model.DocTerms;
import cn.lxdb.plugins.muqingyu.fptoken.model.ExclusiveSelectionResult;
import cn.lxdb.plugins.muqingyu.fptoken.tests.ByteArrayTestSupport;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

@Tag("performance")
@EnabledIfSystemProperty(named = "fptoken.runPerfTests", matches = "true")
@Timeout(value = 10, unit = TimeUnit.SECONDS)
class PerformanceExecutionReliabilityTest {

    @Test
    void executionQuality_environmentSnapshot_shouldContainCoreRuntimeFields() {
        Map<String, String> env = environmentSnapshot();
        assertTrue(!env.get("java.version").isBlank());
        assertTrue(!env.get("java.vm.name").isBlank());
        assertTrue(!env.get("os.name").isBlank());
        assertTrue(!env.get("os.arch").isBlank());
        assertTrue(!env.get("available.processors").isBlank());
        assertTrue(!env.get("max.memory.mb").isBlank());
    }

    @Test
    void executionQuality_warmupAndRepeatedRuns_shouldProduceStableStats() {
        List<DocTerms> rows = PerfTestSupport.protocolMixRows(900, 20260426L);

        // warm-up
        for (int i = 0; i < 3; i++) {
            runFacade(rows, 8, 2, 6, 200_000);
        }

        // measurement
        List<Long> samples = new ArrayList<>();
        for (int i = 0; i < 8; i++) {
            samples.add(PerfTestSupport.elapsedMillis(() -> runFacade(rows, 8, 2, 6, 200_000)));
        }

        long p50 = PerfTestSupport.median(samples);
        long p95 = PerfTestSupport.percentile(samples, 0.95d);
        double stddev = PerfTestSupport.stddevMillis(samples);

        assertTrue(p50 >= 0L);
        assertTrue(p95 >= p50);
        assertTrue(stddev >= 0d);
        assertTrue(p95 <= p50 * 3L + 1L);
    }

    @Test
    void executionQuality_resultArchive_shouldWriteStandardizedCsvAndMarkdown() throws IOException {
        List<DocTerms> rows = PerfTestSupport.protocolMixRows(700, 20260427L);
        List<Long> samples = new ArrayList<>();
        ExclusiveSelectionResult latest = null;
        for (int i = 0; i < 6; i++) {
            final int seed = i;
            long ms = PerfTestSupport.elapsedMillis(() -> runFacade(
                    PerfTestSupport.protocolMixRows(700, 20260427L + seed), 8, 2, 6, 200_000));
            samples.add(ms);
            latest = runFacade(rows, 8, 2, 6, 200_000);
        }
        assertTrue(latest != null);

        long p50 = PerfTestSupport.median(samples);
        long p99 = PerfTestSupport.percentile(samples, 0.99d);
        double stddev = PerfTestSupport.stddevMillis(samples);
        Map<String, String> env = environmentSnapshot();

        Path csv = Files.createTempFile("fptoken-perf-exec-", ".csv");
        Path md = Files.createTempFile("fptoken-perf-exec-", ".md");
        try {
            List<String> csvLines = Arrays.asList(
                    "time,p50_ms,p99_ms,stddev_ms,candidate_count,group_count,java_version,os_name",
                    Instant.now() + "," + p50 + "," + p99 + "," + String.format("%.3f", stddev) + ","
                            + latest.getCandidateCount() + "," + latest.getGroups().size() + ","
                            + env.get("java.version") + "," + env.get("os.name"));
            Files.write(csv, csvLines, StandardCharsets.UTF_8);

            List<String> mdLines = Arrays.asList(
                    "# Performance Execution Report",
                    "",
                    "- timestamp: " + Instant.now(),
                    "- p50_ms: " + p50,
                    "- p99_ms: " + p99,
                    "- stddev_ms: " + String.format("%.3f", stddev),
                    "- candidate_count: " + latest.getCandidateCount(),
                    "- group_count: " + latest.getGroups().size(),
                    "- java_version: " + env.get("java.version"),
                    "- os_name: " + env.get("os.name"));
            Files.write(md, mdLines, StandardCharsets.UTF_8);

            assertTrue(Files.size(csv) > 0L);
            assertTrue(Files.size(md) > 0L);
        } finally {
            Files.deleteIfExists(csv);
            Files.deleteIfExists(md);
        }
    }

    @Test
    void executionQuality_failureIsolation_shouldContinueCollectingAfterSingleCaseFailure() {
        List<Runnable> tasks = new ArrayList<>();
        tasks.add(() -> runFacade(PerfTestSupport.protocolMixRows(350, 1L), 8, 2, 6, 80_000));
        tasks.add(() -> {
            throw new IllegalStateException("injected failure");
        });
        tasks.add(() -> runFacade(PerfTestSupport.protocolMixRows(350, 2L), 8, 2, 6, 80_000));

        int ok = 0;
        int failed = 0;
        for (Runnable task : tasks) {
            try {
                task.run();
                ok++;
            } catch (RuntimeException ex) {
                failed++;
            }
        }

        assertEquals(2, ok);
        assertEquals(1, failed);
    }

    @Test
    void executionQuality_perfRun_shouldAlsoValidateResultCorrectnessInvariants() {
        List<DocTerms> rows = ByteArrayTestSupport.pcapLikeBatch(35, 96, 16, 8);
        ExclusiveSelectionResult lo =
                runFacade(rows, 6, 2, 6, 80_000);
        ExclusiveSelectionResult hi =
                runFacade(rows, 10, 2, 6, 80_000);

        assertTrue(ByteArrayTestSupport.pairwiseTermsDisjoint(lo.getGroups()));
        assertTrue(ByteArrayTestSupport.pairwiseTermsDisjoint(hi.getGroups()));
        assertTrue(ByteArrayTestSupport.allGroupsTermCountInRange(lo.getGroups(), 2, 6));
        assertTrue(ByteArrayTestSupport.allGroupsTermCountInRange(hi.getGroups(), 2, 6));
        assertTrue(hi.getCandidateCount() <= lo.getCandidateCount() || lo.isTruncatedByCandidateLimit());
        assertFalse(lo.getMaxCandidateCount() <= 0);
    }

    private static ExclusiveSelectionResult runFacade(
            List<DocTerms> rows,
            int minSupport,
            int minItemsetSize,
            int maxItemsetSize,
            int maxCandidateCount
    ) {
        return ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(
                rows, minSupport, minItemsetSize, maxItemsetSize, maxCandidateCount);
    }

    private static Map<String, String> environmentSnapshot() {
        Map<String, String> out = new LinkedHashMap<>();
        Runtime rt = Runtime.getRuntime();
        out.put("java.version", System.getProperty("java.version", ""));
        out.put("java.vm.name", System.getProperty("java.vm.name", ""));
        out.put("os.name", System.getProperty("os.name", ""));
        out.put("os.arch", System.getProperty("os.arch", ""));
        out.put("available.processors", Integer.toString(rt.availableProcessors()));
        out.put("max.memory.mb", Long.toString(rt.maxMemory() / (1024L * 1024L)));
        out.put("input.arguments", ManagementFactory.getRuntimeMXBean().getInputArguments().toString());
        return out;
    }
}
