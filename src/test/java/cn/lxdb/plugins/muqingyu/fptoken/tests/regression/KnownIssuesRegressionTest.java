package cn.lxdb.plugins.muqingyu.fptoken.tests.regression;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import cn.lxdb.plugins.muqingyu.fptoken.ExclusiveFrequentItemsetSelector;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.miner.BeamFrequentItemsetMiner;
import cn.lxdb.plugins.muqingyu.fptoken.tests.ByteArrayTestSupport;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import org.junit.jupiter.api.Test;

class KnownIssuesRegressionTest {

    @Test
    void regression_customScoreAndDefaultScore_keepSearchStatsStableOnCanonicalData() {
        List<BitSet> tid = new ArrayList<>();
        BitSet t0 = new BitSet();
        t0.set(0);
        t0.set(1);
        t0.set(2);
        BitSet t1 = new BitSet();
        t1.set(0);
        t1.set(1);
        t1.set(2);
        tid.add(t0);
        tid.add(t1);

        var cfg = new cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.config.SelectorConfig(2, 2, 4, 5000);
        var d = new BeamFrequentItemsetMiner().mineWithStats(tid, cfg, 0, 16, 32);
        var c = new BeamFrequentItemsetMiner((len, sup) -> len * len * sup).mineWithStats(tid, cfg, 0, 16, 32);
        assertEquals(d.getGeneratedCandidateCount(), c.getGeneratedCandidateCount());
        assertEquals(d.getIntersectionCount(), c.getIntersectionCount());
    }

    @Test
    void regression_sparseDocIds_keptInOutputAfterFullPipeline() {
        List<cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.DocTerms> rows = new ArrayList<>();
        byte[] x = ByteArrayTestSupport.hex("DEAD");
        rows.add(ByteArrayTestSupport.doc(4, x));
        rows.add(ByteArrayTestSupport.doc(10, x));
        rows.add(ByteArrayTestSupport.doc(21, x));

        var result = ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(rows, 2, 1, 3, 2000);
        assertTrue(!result.getGroups().isEmpty());
        assertEquals(List.of(4, 10, 21), result.getGroups().get(0).getDocIds());
    }
}
