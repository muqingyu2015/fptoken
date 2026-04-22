package cn.lxdb.plugins.muqingyu.fptoken.tests.unit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.config.EngineTuningConfig;
import org.junit.jupiter.api.Test;

class EngineTuningConfigTest {

    @Test
    void facadeDefaults_shouldBePositiveAndCoherent() {
        assertTrue(EngineTuningConfig.DEFAULT_MAX_ITEMSET_SIZE > 0);
        assertTrue(EngineTuningConfig.DEFAULT_MAX_CANDIDATE_COUNT > 0);
        assertTrue(EngineTuningConfig.DEFAULT_MAX_FREQUENT_TERM_COUNT > 0);
        assertTrue(EngineTuningConfig.DEFAULT_MAX_BRANCHING_FACTOR > 0);
        assertTrue(EngineTuningConfig.FACADE_DEFAULT_BEAM_WIDTH > 0);
        assertTrue(EngineTuningConfig.DEFAULT_MAX_SWAP_TRIALS >= 0);
    }

    @Test
    void minerDefaults_shouldBePositiveAndCoherent() {
        assertTrue(EngineTuningConfig.MINER_FALLBACK_BEAM_WIDTH > 0);
        assertTrue(EngineTuningConfig.BEAM_WIDTH_DIVISOR_1 >= 1);
        assertTrue(EngineTuningConfig.BEAM_WIDTH_DIVISOR_2 >= EngineTuningConfig.BEAM_WIDTH_DIVISOR_1);
        assertTrue(EngineTuningConfig.BITS_PER_WORD == 64);
        assertTrue(EngineTuningConfig.MIN_BITSET_CAPACITY > 0);
        assertTrue(EngineTuningConfig.TIMEOUT_CHECK_INTERVAL > 0);
        assertTrue(EngineTuningConfig.MAX_INITIAL_CANDIDATE_CAPACITY > 0);
    }

    @Test
    void defaultScore_formulaShouldMatchExpected() {
        assertEquals(0, EngineTuningConfig.defaultScore(1, 99));
        assertEquals(12, EngineTuningConfig.defaultScore(4, 4));
        assertEquals(30, EngineTuningConfig.defaultScore(6, 6));
    }
}
