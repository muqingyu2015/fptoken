package cn.lxdb.plugins.muqingyu.fptoken.tests.performance;

import static org.junit.jupiter.api.Assertions.assertTrue;

import cn.lxdb.plugins.muqingyu.fptoken.api.ExclusiveFpRowsProcessingApi;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.DocTerms;
import cn.lxdb.plugins.muqingyu.fptoken.runner.result.LineFileProcessingResult;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

/**
 * 以“提升高频组合层占比”为目标的调优对比测试。
 *
 * <p>通过多个参数档位比较 {@code highFreqSingleTermMovedToMutexGroupPercent}，
 * 并同时约束低频残差占比，避免只靠“挤压阈值”得到失真结果。</p>
 */
@Tag("performance")
@EnabledIfSystemProperty(named = "fptoken.runPerfTests", matches = "true")
@Timeout(value = 30, unit = TimeUnit.SECONDS)
class MutexGroupRatioTuningPerformanceTest {

    @Test
    void tuningMatrix_shouldFindProfileWithBetterMutexAbsorbRatio() {
        List<DocTerms> rows = buildClusteredRows(
                PerfTestSupport.intProp("fptoken.perf.mutex.records", 1200));
        int minSupport = PerfTestSupport.intProp("fptoken.perf.mutex.minSupport", 2);
        int minItemsetSize = 2;
        int hotTermThreshold = PerfTestSupport.intProp("fptoken.perf.mutex.hotThreshold", 3);

        ExclusiveFpRowsProcessingApi.ProcessingOptions baseline = ExclusiveFpRowsProcessingApi.defaultOptions()
                .withMinSupport(minSupport)
                .withMinItemsetSize(minItemsetSize)
                .withHotTermThresholdExclusive(hotTermThreshold)
                .withMaxItemsetSize(4)
                .withMaxCandidateCount(80_000)
                .withSampleRatio(0.25d)
                .withPickerEstimatedBytesPerTerm(2)
                .withPickerCoverageRewardPerTerm(0);

        ExclusiveFpRowsProcessingApi.ProcessingOptions[] tunedProfiles =
                new ExclusiveFpRowsProcessingApi.ProcessingOptions[] {
                        ExclusiveFpRowsProcessingApi.compressionFocusedOptions()
                                .withMinSupport(minSupport)
                                .withMinItemsetSize(minItemsetSize)
                                .withHotTermThresholdExclusive(hotTermThreshold)
                                .withMaxItemsetSize(6)
                                .withMaxCandidateCount(180_000)
                                .withSampleRatio(0.45d)
                                .withPickerEstimatedBytesPerTerm(1)
                                .withPickerCoverageRewardPerTerm(2),
                        ExclusiveFpRowsProcessingApi.compressionFocusedOptions()
                                .withMinSupport(minSupport)
                                .withMinItemsetSize(minItemsetSize)
                                .withHotTermThresholdExclusive(hotTermThreshold)
                                .withMaxItemsetSize(8)
                                .withMaxCandidateCount(220_000)
                                .withSampleRatio(0.60d)
                                .withPickerEstimatedBytesPerTerm(1)
                                .withPickerCoverageRewardPerTerm(3),
                        ExclusiveFpRowsProcessingApi.compressionFocusedOptions()
                                .withMinSupport(minSupport)
                                .withMinItemsetSize(minItemsetSize)
                                .withHotTermThresholdExclusive(hotTermThreshold)
                                .withMaxItemsetSize(8)
                                .withMaxCandidateCount(260_000)
                                .withSampleRatio(0.70d)
                                .withMinSampleCount(256)
                                .withPickerEstimatedBytesPerTerm(1)
                                .withPickerCoverageRewardPerTerm(4)
                };

        RunMetrics baselineMetrics = run(rows, baseline);
        RunMetrics best = baselineMetrics;
        for (ExclusiveFpRowsProcessingApi.ProcessingOptions profile : tunedProfiles) {
            RunMetrics metrics = run(rows, profile);
            if (metrics.score > best.score) {
                best = metrics;
            }
        }
        RunMetrics bestMetrics = best;
        System.out.println(
                "[mutex-ratio-tuning] baselineAbsorb=" + baselineMetrics.mutexAbsorbPercent
                        + "%, bestAbsorb=" + bestMetrics.mutexAbsorbPercent
                        + "%, baselineLowHitRows=" + baselineMetrics.lowHitRowsPercent
                        + "%, bestLowHitRows=" + bestMetrics.lowHitRowsPercent
                        + "%, baselineMs=" + baselineMetrics.elapsedMs
                        + ", bestMs=" + bestMetrics.elapsedMs
        );

        // 目标1：互斥组合吸收率提升。
        assertTrue(
                bestMetrics.mutexAbsorbPercent >= baselineMetrics.mutexAbsorbPercent,
                () -> "best absorb ratio should be >= baseline, best=" + bestMetrics.mutexAbsorbPercent
                        + ", baseline=" + baselineMetrics.mutexAbsorbPercent
        );
        // 目标2：低频残差占比不要显著恶化（避免调优副作用）。
        assertTrue(
                bestMetrics.lowHitRowsPercent <= baselineMetrics.lowHitRowsPercent + 12.0d,
                () -> "best low-hit rows ratio should stay controlled, best=" + bestMetrics.lowHitRowsPercent
                        + ", baseline=" + baselineMetrics.lowHitRowsPercent
        );
    }

    private static RunMetrics run(
            List<DocTerms> rows,
            ExclusiveFpRowsProcessingApi.ProcessingOptions options
    ) {
        long t0 = System.nanoTime();
        LineFileProcessingResult result = ExclusiveFpRowsProcessingApi.processRows(rows, options);
        long elapsedMs = (System.nanoTime() - t0) / 1_000_000L;
        LineFileProcessingResult.ProcessingStats stats = result.getProcessingStats();
        double absorb = stats.getHighFreqSingleTermMovedToMutexGroupPercent();
        double lowHitRows = stats.getLowHitForwardRowsPercent();
        // 更高 absorb、更低 low-hit、更低耗时（弱权重）=> 分数更高。
        double score = absorb - (0.35d * lowHitRows) - (0.02d * elapsedMs);
        return new RunMetrics(absorb, lowHitRows, elapsedMs, score);
    }

    private static List<DocTerms> buildClusteredRows(int records) {
        List<DocTerms> rows = new ArrayList<>(records);
        int docId = 0;
        int kCount = 40;
        int vCount = 10;
        int pairSpace = kCount * vCount;
        for (int i = 0; i < records; i++) {
            int pairId = i % pairSpace;
            int k = pairId % kCount;
            int v = pairId / kCount;
            List<byte[]> terms = new ArrayList<>();
            terms.add(bytes("K" + k));
            terms.add(bytes("V" + v));
            // 加少量噪声词，模拟真实行数据差异。
            terms.add(bytes("NOISE_" + (i % 17)));
            if (i % 7 == 0) {
                terms.add(bytes("NOISE_X"));
            }
            if (i % 11 == 0) {
                terms.addAll(Arrays.asList(bytes("ANCHOR_A"), bytes("ANCHOR_B")));
            }
            rows.add(new DocTerms(docId++, terms));
        }
        return rows;
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static final class RunMetrics {
        private final double mutexAbsorbPercent;
        private final double lowHitRowsPercent;
        private final long elapsedMs;
        private final double score;

        private RunMetrics(
                double mutexAbsorbPercent,
                double lowHitRowsPercent,
                long elapsedMs,
                double score
        ) {
            this.mutexAbsorbPercent = mutexAbsorbPercent;
            this.lowHitRowsPercent = lowHitRowsPercent;
            this.elapsedMs = elapsedMs;
            this.score = score;
        }
    }
}
