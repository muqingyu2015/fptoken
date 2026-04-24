package cn.lxdb.plugins.muqingyu.fptoken.tests.unit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import cn.lxdb.plugins.muqingyu.fptoken.ExclusiveFrequentItemsetSelector;
import cn.lxdb.plugins.muqingyu.fptoken.tests.CandidateFixture;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.config.EngineTuningConfig;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.CandidateItemset;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import org.junit.jupiter.api.Test;

class ExclusiveFrequentItemsetSelectorAdaptiveConfigTest {

    @Test
    void adaptiveBeamWidth_byVocabularySize() throws Exception {
        Method m = ExclusiveFrequentItemsetSelector.class.getDeclaredMethod("adaptiveBeamWidth", int.class);
        m.setAccessible(true);
        assertEquals(24, ((Integer) m.invoke(null, 100)).intValue());
        assertEquals(32, ((Integer) m.invoke(null, 1500)).intValue());
        assertEquals(48, ((Integer) m.invoke(null, 3000)).intValue());
        assertEquals(64, ((Integer) m.invoke(null, 9000)).intValue());
    }

    @Test
    void adaptiveMaxFrequentTermCount_byVocabularySize() throws Exception {
        Method m = ExclusiveFrequentItemsetSelector.class.getDeclaredMethod("adaptiveMaxFrequentTermCount", int.class);
        m.setAccessible(true);
        assertEquals(500, ((Integer) m.invoke(null, 100)).intValue());
        assertEquals(EngineTuningConfig.DEFAULT_MAX_FREQUENT_TERM_COUNT, ((Integer) m.invoke(null, 1500)).intValue());
        assertEquals(2000, ((Integer) m.invoke(null, 3000)).intValue());
        assertEquals(3000, ((Integer) m.invoke(null, 9000)).intValue());
    }

    @Test
    void adaptiveMaxSwapTrials_byCandidateCount() throws Exception {
        Method m = ExclusiveFrequentItemsetSelector.class.getDeclaredMethod("adaptiveMaxSwapTrials", int.class);
        m.setAccessible(true);
        assertEquals(30, ((Integer) m.invoke(null, 100)).intValue());
        assertEquals(100, ((Integer) m.invoke(null, 1000)).intValue());
        assertEquals(150, ((Integer) m.invoke(null, 3000)).intValue());
        assertEquals(300, ((Integer) m.invoke(null, 12000)).intValue());
    }

    @Test
    void recomputeOnFullData_shouldPruneSupersetByKnownLowSupportPrefix() throws Exception {
        Method m = ExclusiveFrequentItemsetSelector.class.getDeclaredMethod(
                "recomputeOnFullDataWithPrefixPruningStats",
                List.class,
                List.class,
                int.class
        );
        m.setAccessible(true);

        List<CandidateItemset> candidates = new ArrayList<>();
        candidates.add(CandidateFixture.itemset(new int[] {0, 1}, 0));
        candidates.add(CandidateFixture.itemset(new int[] {0, 1, 2}, 0, 1, 2, 3));

        List<BitSet> tidsets = new ArrayList<>();
        BitSet t0 = new BitSet();
        t0.set(0);
        t0.set(1);
        t0.set(2);
        t0.set(3);
        BitSet t1 = new BitSet();
        t1.set(0);
        BitSet t2 = new BitSet();
        t2.set(0);
        t2.set(1);
        t2.set(2);
        t2.set(3);
        tidsets.add(t0);
        tidsets.add(t1);
        tidsets.add(t2);

        Object result = m.invoke(null, candidates, tidsets, 2);
        Field recomputedField = result.getClass().getDeclaredField("recomputedCandidates");
        recomputedField.setAccessible(true);
        @SuppressWarnings("unchecked")
        List<CandidateItemset> recomputed = (List<CandidateItemset>) recomputedField.get(result);
        Field prunedField = result.getClass().getDeclaredField("prefixPrunedCandidates");
        prunedField.setAccessible(true);
        int pruned = ((Integer) prunedField.get(result)).intValue();

        assertTrue(recomputed.isEmpty());
        assertEquals(1, pruned);
    }
}
