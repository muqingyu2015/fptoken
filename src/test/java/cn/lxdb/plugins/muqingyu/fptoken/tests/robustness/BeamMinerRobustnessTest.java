package cn.lxdb.plugins.muqingyu.fptoken.tests.robustness;

import static org.junit.jupiter.api.Assertions.assertTrue;

import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.config.SelectorConfig;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.miner.BeamFrequentItemsetMiner;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import org.junit.jupiter.api.Test;

class BeamMinerRobustnessTest {

    @Test
    void timeoutAndIdleStop_doNotCrashAndReturnConsistentStats() {
        List<BitSet> tid = denseTidsets(10, 200);
        SelectorConfig cfg = new SelectorConfig(20, 2, 6, 50_000);
        BeamFrequentItemsetMiner miner = new BeamFrequentItemsetMiner();

        var result = miner.mineWithStats(tid, cfg, 0, 32, 64, 1, 1L);

        assertTrue(result.getFrequentTermCount() >= 0);
        assertTrue(result.getGeneratedCandidateCount() >= 0);
        assertTrue(result.getIntersectionCount() >= 0);
        assertTrue(result.getCandidates().size() <= cfg.getMaxCandidateCount());
    }

    @Test
    void malformedTermIdInPickerPath_shouldBeRejectedByComponent() {
        var picker = new cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.picker.TwoPhaseExclusiveItemsetPicker();
        var bad = new cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.CandidateItemset(new int[] {-1}, new BitSet());
        List<cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.CandidateItemset> candidates = List.of(bad);
        boolean threw = false;
        try {
            picker.pick(candidates, 1, 1);
        } catch (IllegalArgumentException ex) {
            threw = true;
        }
        assertTrue(threw);
    }

    private static List<BitSet> denseTidsets(int termCount, int docCount) {
        List<BitSet> out = new ArrayList<>();
        for (int t = 0; t < termCount; t++) {
            BitSet b = new BitSet();
            for (int d = 0; d < docCount; d++) {
                if ((d + t) % 2 == 0 || d % 3 == 0) {
                    b.set(d);
                }
            }
            out.add(b);
        }
        return out;
    }
}
