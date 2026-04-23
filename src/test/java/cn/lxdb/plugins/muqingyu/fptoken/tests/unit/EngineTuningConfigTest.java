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
        assertTrue(EngineTuningConfig.COMPRESSION_FOCUSED_MAX_ITEMSET_SIZE
                >= EngineTuningConfig.DEFAULT_MAX_ITEMSET_SIZE);
        assertTrue(EngineTuningConfig.COMPRESSION_FOCUSED_MAX_CANDIDATE_COUNT
                >= EngineTuningConfig.DEFAULT_MAX_CANDIDATE_COUNT);
        assertTrue(EngineTuningConfig.COMPRESSION_FOCUSED_SAMPLE_RATIO
                >= EngineTuningConfig.DEFAULT_SAMPLE_RATIO);
        assertTrue(EngineTuningConfig.COMPRESSION_FOCUSED_SAMPLE_RATIO <= 1.0d);
        assertTrue(EngineTuningConfig.COMPRESSION_FOCUSED_MIN_SAMPLE_COUNT
                >= EngineTuningConfig.DEFAULT_MIN_SAMPLE_COUNT);
        assertTrue(EngineTuningConfig.PICKER_ESTIMATED_BYTES_PER_TERM > 0);
        assertTrue(EngineTuningConfig.COMPRESSION_FOCUSED_PICKER_ESTIMATED_BYTES_PER_TERM > 0);
        assertTrue(EngineTuningConfig.COMPRESSION_FOCUSED_PICKER_ESTIMATED_BYTES_PER_TERM
                <= EngineTuningConfig.PICKER_ESTIMATED_BYTES_PER_TERM);
        assertTrue(EngineTuningConfig.COMPRESSION_FOCUSED_PICKER_MIN_NET_GAIN >= 0);
        assertTrue(EngineTuningConfig.PICKER_DEFAULT_COVERAGE_REWARD_PER_TERM >= 0);
        assertTrue(EngineTuningConfig.COMPRESSION_FOCUSED_PICKER_COVERAGE_REWARD_PER_TERM
                >= EngineTuningConfig.PICKER_DEFAULT_COVERAGE_REWARD_PER_TERM);
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
