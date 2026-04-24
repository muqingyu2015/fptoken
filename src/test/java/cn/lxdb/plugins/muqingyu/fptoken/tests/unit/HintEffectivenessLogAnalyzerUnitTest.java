package cn.lxdb.plugins.muqingyu.fptoken.tests.unit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import cn.lxdb.plugins.muqingyu.fptoken.runner.analysis.HintEffectivenessLogAnalyzer;
import cn.lxdb.plugins.muqingyu.fptoken.runner.analysis.HintEffectivenessLogAnalyzer.AnalysisSummary;
import cn.lxdb.plugins.muqingyu.fptoken.runner.analysis.HintEffectivenessLogAnalyzer.Decision;
import cn.lxdb.plugins.muqingyu.fptoken.runner.analysis.HintEffectivenessLogAnalyzer.DecisionPolicy;
import cn.lxdb.plugins.muqingyu.fptoken.runner.analysis.HintEffectivenessLogAnalyzer.EffectClass;
import cn.lxdb.plugins.muqingyu.fptoken.runner.analysis.HintEffectivenessLogAnalyzer.Observation;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

class HintEffectivenessLogAnalyzerUnitTest {

    @Test
    void parseObservationLine_shouldExtractModeAndImprovement() {
        Observation obs = HintEffectivenessLogAnalyzer.parseObservationLine(
                "[real-lucene-merge-hint] mode=mutex-only, improveMedianPercent=-11.32, positiveRounds=0/7",
                "x.log",
                5
        );
        assertNotNull(obs);
        assertEquals("mutex-only", obs.mode);
        assertEquals(-11.32d, obs.improvementPercent, 0.0001d);
        assertEquals("x.log:5", obs.source);
    }

    @Test
    void analyzeFiles_shouldComputeRatiosAndDecision() throws Exception {
        Path log = Files.createTempFile("hint-effectiveness", ".log");
        List<String> lines = Arrays.asList(
                "[real-lucene-merge-hint] mode=mutex-only, improveMedianPercent=12.5, positiveRounds=6/7",
                "[real-lucene-merge-hint] mode=mutex-only, improveMedianPercent=-15.0, positiveRounds=1/7",
                "[merge-hint-perf] baselineMedianMs=100, hintedMedianMs=102, improvementPercent=-2.0",
                "[merge-hint-perf] baselineMedianMs=100, hintedMedianMs=80, improvementPercent=20.0",
                "this is unrelated line"
        );
        Files.write(log, lines, StandardCharsets.UTF_8);
        try {
            AnalysisSummary summary = HintEffectivenessLogAnalyzer.analyzeFiles(
                    Arrays.asList(log),
                    DecisionPolicy.defaults()
            );
            assertEquals(4, summary.totalObservations);
            assertEquals(2, summary.totalCounts.get(EffectClass.EFFECTIVE).intValue());
            assertEquals(1, summary.totalCounts.get(EffectClass.NEUTRAL).intValue());
            assertEquals(1, summary.totalCounts.get(EffectClass.NEGATIVE).intValue());
            assertEquals(0.5d, summary.effectiveRatio, 0.0001d);
            assertEquals(0.25d, summary.negativeRatio, 0.0001d);
            assertEquals(Decision.ENABLE_CONDITIONALLY_WITH_GUARDRAIL, summary.decision);
        } finally {
            Files.deleteIfExists(log);
        }
    }
}
