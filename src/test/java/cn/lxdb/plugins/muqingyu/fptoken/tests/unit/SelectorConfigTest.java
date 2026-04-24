package cn.lxdb.plugins.muqingyu.fptoken.tests.unit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.config.SelectorConfig;
import java.util.stream.Stream;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
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

    @TestFactory
    Stream<DynamicTest> invalidConfigs_shouldThrow() {
        int[][] cases = new int[][]{
                {0, 1, 3, 1},
                {-1, 1, 3, 1},
                {1, 0, 3, 1},
                {1, -2, 3, 1},
                {1, 4, 3, 1},
                {1, 1, 3, 0},
                {1, 1, 3, -8},
                {1, 1, 0, 10}
        };
        return Stream.of(cases).map(c -> DynamicTest.dynamicTest(
                "invalid(" + c[0] + "," + c[1] + "," + c[2] + "," + c[3] + ")",
                () -> assertThrows(IllegalArgumentException.class, () -> new SelectorConfig(c[0], c[1], c[2], c[3]))
        ));
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

    @Test
    void staticFactory_sameArgs_shouldReuseImmutableInstance() {
        SelectorConfig c1 = SelectorConfig.of(5, 2, 8, 1000);
        SelectorConfig c2 = SelectorConfig.of(5, 2, 8, 1000);
        assertSame(c1, c2);
    }

    @TestFactory
    Stream<DynamicTest> aliasGetters_shouldMatchPrimaryGetters() {
        SelectorConfig c = SelectorConfig.of(7, 3, 9, 123);
        return Stream.of(
                DynamicTest.dynamicTest(
                        "minimumRequiredSupport alias",
                        () -> assertEquals(c.getMinSupport(), c.getMinimumRequiredSupport())
                ),
                DynamicTest.dynamicTest(
                        "minimumPatternLength alias",
                        () -> assertEquals(c.getMinItemsetSize(), c.getMinimumPatternLength())
                ),
                DynamicTest.dynamicTest(
                        "maximumPatternLength alias",
                        () -> assertEquals(c.getMaxItemsetSize(), c.getMaximumPatternLength())
                ),
                DynamicTest.dynamicTest(
                        "maximumIntermediateResults alias",
                        () -> assertEquals(c.getMaxCandidateCount(), c.getMaximumIntermediateResults())
                )
        );
    }
}

