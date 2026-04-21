package cn.lxdb.plugins.muqingyu.fptoken.tests.unit;

import static org.junit.jupiter.api.Assertions.assertEquals;

import cn.lxdb.plugins.muqingyu.fptoken.ExclusiveFrequentItemsetSelector;
import java.lang.reflect.Method;
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
        assertEquals(1500, ((Integer) m.invoke(null, 1500)).intValue());
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
}
