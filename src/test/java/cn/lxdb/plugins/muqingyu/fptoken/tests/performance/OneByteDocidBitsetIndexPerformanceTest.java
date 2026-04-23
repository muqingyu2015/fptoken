package cn.lxdb.plugins.muqingyu.fptoken.tests.performance;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
 * one-byte 倒排 bitset 构建路径性能测试（仅 Perf 开关开启时执行）。
 */
@Tag("performance")
@EnabledIfSystemProperty(named = "fptoken.runPerfTests", matches = "true")
@Timeout(value = 60, unit = TimeUnit.SECONDS)
class OneByteDocidBitsetIndexPerformanceTest {

    @Test
    void PERF_ONEBYTE_001_repeatedProcessRows_shouldStayWithinBudget() {
        int docs = PerfTestSupport.intProp("fptoken.perf.onebyte.docs", 12000);
        int rounds = PerfTestSupport.intProp("fptoken.perf.onebyte.rounds", 8);
        long budgetMs = PerfTestSupport.longProp("fptoken.perf.onebyte.totalBudgetMs", 12000L);

        List<DocTerms> rows = buildRawRows(docs);
        final LineFileProcessingResult[] holder = new LineFileProcessingResult[1];
        long elapsedMs = PerfTestSupport.elapsedMillis(() -> {
            for (int i = 0; i < rounds; i++) {
                holder[0] = ExclusiveFpRowsProcessingApi.processRowsWithNgram(
                        rows, 2, 4, docs + 1, 2, docs + 1);
            }
        });

        assertTrue(holder[0] != null);
        assertEquals(docs - 1, holder[0].getOneByteDocidBitsetIndex().getMaxDocId());
        assertTrue(elapsedMs <= budgetMs, () -> "elapsedMs=" + elapsedMs + ", budgetMs=" + budgetMs);
    }

    private static List<DocTerms> buildRawRows(int docs) {
        List<DocTerms> out = new ArrayList<DocTerms>(docs);
        for (int i = 0; i < docs; i++) {
            // 固定 64B 左右，保持与线上样本接近；每条只保留一段 raw bytes，触发 API 内部切词路径。
            String line = "FLOW_" + (i % 256) + "_ABCD_ZYXW_" + i + "_0123456789abcdef0123456789abcdef";
            out.add(new DocTerms(i, Arrays.asList(line.getBytes(StandardCharsets.UTF_8))));
        }
        return out;
    }
}

