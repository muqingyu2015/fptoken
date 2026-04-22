package cn.lxdb.plugins.muqingyu.fptoken.tests.unit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.config.SelectorConfig;
import org.junit.jupiter.api.Test;

class SelectorConfigTest {

    @Test
    void validConfig_getters() {
        SelectorConfig c = new SelectorConfig(5, 2, 8, 1000);
        assertEquals(5, c.getMinSupport());
        assertEquals(2, c.getMinItemsetSize());
        assertEquals(8, c.getMaxItemsetSize());
        assertEquals(1000, c.getMaxCandidateCount());
    }

    @Test
    void minSupportNonPositive_throws() {
        assertThrows(IllegalArgumentException.class, () -> new SelectorConfig(0, 1, 3, 1));
        assertThrows(IllegalArgumentException.class, () -> new SelectorConfig(-1, 1, 3, 1));
    }

    @Test
    void minItemsetNonPositive_throws() {
        assertThrows(IllegalArgumentException.class, () -> new SelectorConfig(1, 0, 3, 1));
        assertThrows(IllegalArgumentException.class, () -> new SelectorConfig(1, -2, 3, 1));
    }

    @Test
    void maxLessThanMin_throws() {
        assertThrows(IllegalArgumentException.class, () -> new SelectorConfig(1, 4, 3, 1));
    }

    @Test
    void maxCandidateNonPositive_throws() {
        assertThrows(IllegalArgumentException.class, () -> new SelectorConfig(1, 1, 3, 0));
        assertThrows(IllegalArgumentException.class, () -> new SelectorConfig(1, 1, 3, -8));
    }

    @Test
    void maxItemsetSizeLessThanOne_throws() {
        assertThrows(IllegalArgumentException.class, () -> new SelectorConfig(1, 1, 0, 10));
    }

    @Test
    void minEqualsMaxItemsetSize_allowed() {
        SelectorConfig c = new SelectorConfig(3, 2, 2, 100);
        assertEquals(2, c.getMinItemsetSize());
        assertEquals(2, c.getMaxItemsetSize());
    }

    @Test
    void maxCandidateCount_one_allowed() {
        SelectorConfig c = new SelectorConfig(1, 1, 4, 1);
        assertEquals(1, c.getMaxCandidateCount());
    }

    @Test
    void largeButValidValues_roundTrip() {
        SelectorConfig c = new SelectorConfig(Integer.MAX_VALUE / 4, 1, 64, Integer.MAX_VALUE / 2);
        assertTrue(c.getMinSupport() > 0);
        assertTrue(c.getMaxCandidateCount() > 0);
    }
}

