package cn.lxdb.plugins.muqingyu.fptoken.tests.performance;

import cn.lxdb.plugins.muqingyu.fptoken.tests.ByteArrayTestSupport;

import static org.junit.jupiter.api.Assertions.assertTrue;

import cn.lxdb.plugins.muqingyu.fptoken.ExclusiveFrequentItemsetSelector;
import cn.lxdb.plugins.muqingyu.fptoken.model.DocTerms;
import cn.lxdb.plugins.muqingyu.fptoken.model.ExclusiveSelectionResult;
import java.util.List;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

/**
 * 鍏稿瀷璐熻浇璁℃椂锛堥潪 JMH锛夛細妯℃嫙銆屼竾绾у寘 脳 澶氭粦鍔ㄧ獥 脳 1/2/3-byte items銆嶉噺绾ф椂鍙墜鍔ㄦ墦寮€銆?
 *
 * <p>杩愯绀轰緥锛圥owerShell锛夛細
 * <pre>
 *   java -Dfptoken.runPerfTests=true -jar junit-platform-console-standalone-....jar ...
 * </pre>
 * 鎴栦娇鐢ㄤ粨搴撴牴鐩綍 {@code scripts/run-fptoken-tests.ps1 -Perf}銆?
 */
@Tag("performance")
@EnabledIfSystemProperty(named = "fptoken.runPerfTests", matches = "true")
class ExclusiveFrequentItemsetSelectorPerformanceTest {

    @Test
    void benchmark_pcapLikeBatch_defaultScale() {
        int records = Integer.getInteger("fptoken.perf.records", 2000);
        int recordLen = Integer.getInteger("fptoken.perf.recordLen", 256);
        int windowLen = Integer.getInteger("fptoken.perf.windowLen", 32);
        int windowStep = Integer.getInteger("fptoken.perf.windowStep", 16);
        int minSupport = Integer.getInteger("fptoken.perf.minSupport", 50);
        int maxCandidateCount = Integer.getInteger("fptoken.perf.maxCandidateCount", 200_000);

        List<DocTerms> rows = ByteArrayTestSupport.pcapLikeBatch(records, recordLen, windowLen, windowStep);

        Runtime rt = Runtime.getRuntime();
        rt.gc();
        long memBefore = rt.totalMemory() - rt.freeMemory();
        long t0 = System.nanoTime();
        ExclusiveSelectionResult r = ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(
                rows, minSupport, 2, 6, maxCandidateCount);
        long ms = (System.nanoTime() - t0) / 1_000_000L;
        long memAfter = rt.totalMemory() - rt.freeMemory();

        System.out.println("[fptoken perf] docs=" + rows.size()
                + " records=" + records
                + " timeMs=" + ms
                + " groups=" + r.getGroups().size()
                + " candidates=" + r.getCandidateCount()
                + " frequentTerms=" + r.getFrequentTermCount()
                + " truncated=" + r.isTruncatedByCandidateLimit()
                + " heapDeltaMb=" + ((memAfter - memBefore) / (1024 * 1024)));

        assertTrue(ms >= 0);
        assertTrue(ByteArrayTestSupport.pairwiseTermsDisjoint(r.getGroups()));
    }
}

