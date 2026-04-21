package cn.lxdb.plugins.muqingyu.fptoken.tests.performance;

import static org.junit.jupiter.api.Assertions.assertTrue;

import cn.lxdb.plugins.muqingyu.fptoken.ExclusiveFrequentItemsetSelector;
import cn.lxdb.plugins.muqingyu.fptoken.model.DocTerms;
import cn.lxdb.plugins.muqingyu.fptoken.model.ExclusiveSelectionResult;
import cn.lxdb.plugins.muqingyu.fptoken.tests.ByteArrayTestSupport;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

@Tag("performance")
@EnabledIfSystemProperty(named = "fptoken.runPerfTests", matches = "true")
@Timeout(value = 10, unit = TimeUnit.SECONDS)
class CandidateLimitPressurePerformanceTest {

    @Test
    void denseInput_lowCandidateLimit_truncatesQuickly() {
        List<DocTerms> rows = new ArrayList<>();
        // 构造“每条文档都包含同一批高频词”的高密场景，确保候选数会迅速超过上限。
        List<byte[]> denseTerms = new ArrayList<>();
        for (int t = 0; t < 20; t++) {
            denseTerms.add(new byte[] {(byte) t});
        }
        for (int i = 0; i < 600; i++) {
            rows.add(ByteArrayTestSupport.doc(i, denseTerms));
        }

        long startNs = System.nanoTime();
        ExclusiveSelectionResult result = ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(
                rows, 40, 2, 5, 80);
        long elapsedMs = (System.nanoTime() - startNs) / 1_000_000L;

        assertTrue(result.getCandidateCount() <= 80);
        assertTrue(
                result.isTruncatedByCandidateLimit(),
                () -> "expected truncation under dense input, candidateCount=" + result.getCandidateCount());
        assertTrue(elapsedMs >= 0);
        assertTrue(ByteArrayTestSupport.pairwiseTermsDisjoint(result.getGroups()));
    }
}
