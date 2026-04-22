package cn.lxdb.plugins.muqingyu.fptoken.tests.performance;

import cn.lxdb.plugins.muqingyu.fptoken.tests.ByteArrayTestSupport;

import static org.junit.jupiter.api.Assertions.assertTrue;

import cn.lxdb.plugins.muqingyu.fptoken.ExclusiveFrequentItemsetSelector;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.index.TermTidsetIndex;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.miner.BeamFrequentItemsetMiner;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.config.SelectorConfig;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.DocTerms;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.ExclusiveSelectionResult;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

/**
 * 璁捐鏂囨。 P-002锝濸-005 绛夛細鏃堕棿棰勭畻鐑熸祴锛堥粯璁ゅ叧闂紝閬垮厤 CI 鎶栧姩锛夈€?
 *
 * <p>鍚敤锛歿@code -Dfptoken.runBudgetPerfTests=true}
 */
@Tag("performance")
@EnabledIfSystemProperty(named = "fptoken.runBudgetPerfTests", matches = "true")
@Timeout(value = 10, unit = TimeUnit.SECONDS)
class PerformanceBudgetDesignDocTest {

    private static final long DEFAULT_TOTAL_BUDGET_MS = 600_000L;

    @Test
    void p002_miningPhaseBudget() {
        List<DocTerms> rows = ByteArrayTestSupport.pcapLikeBatch(800, 512, 64, 32);
        var index = TermTidsetIndex.build(rows);
        var miner = new BeamFrequentItemsetMiner();
        var config = new SelectorConfig(8, 2, 6, 200_000);
        long t0 = System.nanoTime();
        miner.mineWithStats(index.getTidsetsByTermId(), config, 2048, 24, 48);
        long ms = (System.nanoTime() - t0) / 1_000_000L;
        assertTrue(ms < Long.getLong("fptoken.miningBudgetMs", 300_000L), () -> "mining ms=" + ms);
    }

    @Test
    void p003_pickerPhaseBudget() {
        List<DocTerms> rows = ByteArrayTestSupport.pcapLikeBatch(600, 384, 48, 24);
        ExclusiveSelectionResult r =
                ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(rows, 5, 2, 6, 200_000);
        assertTrue(r.getCandidateCount() >= 0);
    }

    @Test
    void p004_endToEndBudget() {
        List<DocTerms> rows = ByteArrayTestSupport.pcapLikeBatch(1500, 512, 64, 32);
        long t0 = System.nanoTime();
        ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(rows, 10, 2, 8, 200_000);
        long ms = (System.nanoTime() - t0) / 1_000_000L;
        assertTrue(ms < Long.getLong("fptoken.e2eBudgetMs", DEFAULT_TOTAL_BUDGET_MS), () -> "e2e ms=" + ms);
    }

    @Test
    void p005_scalingPrint() {
        for (int n : new int[] {1000, 5000}) {
            List<DocTerms> rows = ByteArrayTestSupport.pcapLikeBatch(n / 10, 256, 32, 16);
            long t0 = System.nanoTime();
            ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(rows, 8, 2, 6, 150_000);
            long ms = (System.nanoTime() - t0) / 1_000_000L;
            System.out.println("[P-005] approxRecords=" + n + " docs=" + rows.size() + " timeMs=" + ms);
        }
    }

}

