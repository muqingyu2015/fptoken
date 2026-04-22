package cn.lxdb.plugins.muqingyu.fptoken.tests.performance;

import static org.junit.jupiter.api.Assertions.assertTrue;

import cn.lxdb.plugins.muqingyu.fptoken.ExclusiveFrequentItemsetSelector;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.DocTerms;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.ExclusiveSelectionResult;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

@Tag("performance")
@EnabledIfSystemProperty(named = "fptoken.runPerfTests", matches = "true")
@Timeout(value = 30, unit = TimeUnit.SECONDS)
class PerformanceRegressionGateTest {

    @Test
    void baselineGuard_p50_p95_candidate_heap_truncate() throws Exception {
        List<Long> samples = new ArrayList<>();
        long maxHeapDeltaMb = 0L;
        int maxCandidates = 0;
        boolean anyTruncated = false;
        for (int i = 0; i < 6; i++) {
            final int seed = i;
            List<DocTerms> rows = PerfTestSupport.protocolMixRows(500, 20260500L + seed);
            Runtime rt = Runtime.getRuntime();
            rt.gc();
            long before = rt.totalMemory() - rt.freeMemory();
            final ExclusiveSelectionResult[] holder = new ExclusiveSelectionResult[1];
            long ms = PerfTestSupport.elapsedMillis(() -> {
                holder[0] = ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(
                        rows, 20, 2, 6, 120000);
            });
            long after = rt.totalMemory() - rt.freeMemory();
            long heapDelta = Math.max(0L, (after - before) / (1024L * 1024L));
            maxHeapDeltaMb = Math.max(maxHeapDeltaMb, heapDelta);
            samples.add(Long.valueOf(ms));
            ExclusiveSelectionResult snapshot = holder[0];
            maxCandidates = Math.max(maxCandidates, snapshot.getCandidateCount());
            anyTruncated = anyTruncated || snapshot.isTruncatedByCandidateLimit();
        }

        long p50 = PerfTestSupport.percentile(samples, 0.50d);
        long p95 = PerfTestSupport.percentile(samples, 0.95d);
        BaselineRow current = new BaselineRow(p50, p95, maxCandidates, maxHeapDeltaMb, anyTruncated ? 1 : 0);

        Path baselinePath = Path.of(System.getProperty(
                "fptoken.perf.regression.file",
                System.getProperty("java.io.tmpdir") + "/fptoken-perf-regression-gate.csv"
        ));
        if (!Files.exists(baselinePath)) {
            List<String> lines = new ArrayList<>();
            lines.add("timestamp,p50_ms,p95_ms,max_candidates,max_heap_delta_mb,any_truncated");
            lines.add(current.toCsvLine());
            Files.write(baselinePath, lines, StandardCharsets.UTF_8);
            assertTrue(true);
            return;
        }

        BaselineRow baseline = readLastRow(baselinePath);
        double latencyRatio = Double.parseDouble(System.getProperty("fptoken.perf.regression.maxLatencyRatio", "1.35"));
        double candidateRatio = Double.parseDouble(System.getProperty("fptoken.perf.regression.maxCandidateRatio", "1.40"));
        double heapRatio = Double.parseDouble(System.getProperty("fptoken.perf.regression.maxHeapRatio", "1.40"));

        assertTrue(current.p50Ms <= Math.max(1d, baseline.p50Ms) * latencyRatio,
                () -> "p50 regression: current=" + current.p50Ms + ", baseline=" + baseline.p50Ms);
        assertTrue(current.p95Ms <= Math.max(1d, baseline.p95Ms) * latencyRatio,
                () -> "p95 regression: current=" + current.p95Ms + ", baseline=" + baseline.p95Ms);
        assertTrue(current.maxCandidates <= Math.max(1d, baseline.maxCandidates) * candidateRatio,
                () -> "candidate regression: current=" + current.maxCandidates + ", baseline=" + baseline.maxCandidates);
        long heapAbsSlackMb = Long.parseLong(System.getProperty("fptoken.perf.regression.heapAbsSlackMb", "16"));
        double heapLimitByRatio = Math.max(1d, baseline.maxHeapDeltaMb) * heapRatio;
        double heapLimitBySlack = baseline.maxHeapDeltaMb + heapAbsSlackMb;
        double heapAllowed = Math.max(heapLimitByRatio, heapLimitBySlack);
        assertTrue(current.maxHeapDeltaMb <= heapAllowed,
                () -> "heap regression: current=" + current.maxHeapDeltaMb + ", baseline="
                        + baseline.maxHeapDeltaMb + ", heapAllowed=" + heapAllowed);

        appendRow(baselinePath, current);
    }

    private static BaselineRow readLastRow(Path baselinePath) throws Exception {
        List<String> lines = Files.readAllLines(baselinePath, StandardCharsets.UTF_8);
        if (lines.size() <= 1) {
            return new BaselineRow(1L, 1L, 1, 1L, 0);
        }
        String[] parts = lines.get(lines.size() - 1).split(",");
        return new BaselineRow(
                Long.parseLong(parts[1].trim()),
                Long.parseLong(parts[2].trim()),
                Integer.parseInt(parts[3].trim()),
                Long.parseLong(parts[4].trim()),
                Integer.parseInt(parts[5].trim())
        );
    }

    private static void appendRow(Path baselinePath, BaselineRow row) throws Exception {
        List<String> lines = new ArrayList<>();
        lines.add(row.toCsvLine());
        Files.write(baselinePath, lines, StandardCharsets.UTF_8, java.nio.file.StandardOpenOption.APPEND);
    }

    private static final class BaselineRow {
        private final long p50Ms;
        private final long p95Ms;
        private final int maxCandidates;
        private final long maxHeapDeltaMb;
        private final int anyTruncated;

        private BaselineRow(long p50Ms, long p95Ms, int maxCandidates, long maxHeapDeltaMb, int anyTruncated) {
            this.p50Ms = p50Ms;
            this.p95Ms = p95Ms;
            this.maxCandidates = maxCandidates;
            this.maxHeapDeltaMb = maxHeapDeltaMb;
            this.anyTruncated = anyTruncated;
        }

        private String toCsvLine() {
            return String.format(
                    Locale.ROOT,
                    "%s,%d,%d,%d,%d,%d",
                    Instant.now(),
                    p50Ms,
                    p95Ms,
                    maxCandidates,
                    maxHeapDeltaMb,
                    anyTruncated
            );
        }
    }
}
