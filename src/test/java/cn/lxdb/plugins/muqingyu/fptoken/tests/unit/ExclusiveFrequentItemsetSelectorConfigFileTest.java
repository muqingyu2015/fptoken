package cn.lxdb.plugins.muqingyu.fptoken.tests.unit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import cn.lxdb.plugins.muqingyu.fptoken.ExclusiveFrequentItemsetSelector;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.config.EngineTuningConfig;
import org.junit.jupiter.api.Test;

class ExclusiveFrequentItemsetSelectorConfigFileTest {

    @Test
    void codeConfig_setters_shouldTakeEffectImmediately() {
        double oldRatio = ExclusiveFrequentItemsetSelector.getSampleRatio();
        int oldMin = ExclusiveFrequentItemsetSelector.getMinSampleCount();
        double oldScale = ExclusiveFrequentItemsetSelector.getSamplingSupportScale();
        int oldMinNetGain = ExclusiveFrequentItemsetSelector.getPickerMinNetGain();
        int oldEstimatedBytes = ExclusiveFrequentItemsetSelector.getPickerEstimatedBytesPerTerm();
        int oldCoverageReward = ExclusiveFrequentItemsetSelector.getPickerCoverageRewardPerTerm();
        try {
            ExclusiveFrequentItemsetSelector.setSampleRatio(0.42d);
            ExclusiveFrequentItemsetSelector.setMinSampleCount(77);
            ExclusiveFrequentItemsetSelector.setSamplingSupportScale(0.88d);
            ExclusiveFrequentItemsetSelector.setPickerMinNetGain(2);
            ExclusiveFrequentItemsetSelector.setPickerEstimatedBytesPerTerm(1);
            ExclusiveFrequentItemsetSelector.setPickerCoverageRewardPerTerm(4);

            assertEquals(0.42d, ExclusiveFrequentItemsetSelector.getSampleRatio(), 1e-9);
            assertEquals(77, ExclusiveFrequentItemsetSelector.getMinSampleCount());
            assertEquals(0.88d, ExclusiveFrequentItemsetSelector.getSamplingSupportScale(), 1e-9);
            assertEquals(2, ExclusiveFrequentItemsetSelector.getPickerMinNetGain());
            assertEquals(1, ExclusiveFrequentItemsetSelector.getPickerEstimatedBytesPerTerm());
            assertEquals(4, ExclusiveFrequentItemsetSelector.getPickerCoverageRewardPerTerm());
        } finally {
            ExclusiveFrequentItemsetSelector.setSampleRatio(oldRatio);
            ExclusiveFrequentItemsetSelector.setMinSampleCount(oldMin);
            ExclusiveFrequentItemsetSelector.setSamplingSupportScale(oldScale);
            ExclusiveFrequentItemsetSelector.setPickerMinNetGain(oldMinNetGain);
            ExclusiveFrequentItemsetSelector.setPickerEstimatedBytesPerTerm(oldEstimatedBytes);
            ExclusiveFrequentItemsetSelector.setPickerCoverageRewardPerTerm(oldCoverageReward);
        }
    }

    @Test
    void codeConfig_invalidPickerTuning_shouldThrow() {
        assertThrows(IllegalArgumentException.class,
                () -> ExclusiveFrequentItemsetSelector.setPickerMinNetGain(-1));
        assertThrows(IllegalArgumentException.class,
                () -> ExclusiveFrequentItemsetSelector.setPickerEstimatedBytesPerTerm(0));
        assertThrows(IllegalArgumentException.class,
                () -> ExclusiveFrequentItemsetSelector.setPickerCoverageRewardPerTerm(-1));
    }

    @Test
    void codeConfig_invalidSampleRatio_shouldBeClampedToRange() {
        double oldRatio = ExclusiveFrequentItemsetSelector.getSampleRatio();
        try {
            ExclusiveFrequentItemsetSelector.setSampleRatio(-2.0d);
            assertEquals(0.0d, ExclusiveFrequentItemsetSelector.getSampleRatio(), 1e-12);

            ExclusiveFrequentItemsetSelector.setSampleRatio(5.0d);
            assertEquals(1.0d, ExclusiveFrequentItemsetSelector.getSampleRatio(), 1e-12);
        } finally {
            ExclusiveFrequentItemsetSelector.setSampleRatio(oldRatio);
        }
    }

    @Test
    void codeConfig_invalidMinSampleCount_shouldBeFlooredToOne() {
        int oldMin = ExclusiveFrequentItemsetSelector.getMinSampleCount();
        try {
            ExclusiveFrequentItemsetSelector.setMinSampleCount(0);
            assertEquals(1, ExclusiveFrequentItemsetSelector.getMinSampleCount());
        } finally {
            ExclusiveFrequentItemsetSelector.setMinSampleCount(oldMin);
        }
    }

    @Test
    void restoreDefaults_shouldWorkWithCodeConfig() {
        ExclusiveFrequentItemsetSelector.setSampleRatio(0.66d);
        ExclusiveFrequentItemsetSelector.setMinSampleCount(99);
        ExclusiveFrequentItemsetSelector.setSamplingSupportScale(0.77d);
        ExclusiveFrequentItemsetSelector.setPickerMinNetGain(3);
        ExclusiveFrequentItemsetSelector.setPickerEstimatedBytesPerTerm(1);
        ExclusiveFrequentItemsetSelector.setPickerCoverageRewardPerTerm(3);

        ExclusiveFrequentItemsetSelector.setSampleRatio(EngineTuningConfig.DEFAULT_SAMPLE_RATIO);
        ExclusiveFrequentItemsetSelector.setMinSampleCount(EngineTuningConfig.DEFAULT_MIN_SAMPLE_COUNT);
        ExclusiveFrequentItemsetSelector.setSamplingSupportScale(EngineTuningConfig.DEFAULT_SAMPLING_SUPPORT_SCALE);
        ExclusiveFrequentItemsetSelector.setPickerMinNetGain(EngineTuningConfig.PICKER_DEFAULT_MIN_NET_GAIN);
        ExclusiveFrequentItemsetSelector.setPickerEstimatedBytesPerTerm(
                EngineTuningConfig.PICKER_ESTIMATED_BYTES_PER_TERM);
        ExclusiveFrequentItemsetSelector.setPickerCoverageRewardPerTerm(
                EngineTuningConfig.PICKER_DEFAULT_COVERAGE_REWARD_PER_TERM);

        assertEquals(EngineTuningConfig.DEFAULT_SAMPLE_RATIO, ExclusiveFrequentItemsetSelector.getSampleRatio(), 1e-12);
        assertEquals(EngineTuningConfig.DEFAULT_MIN_SAMPLE_COUNT, ExclusiveFrequentItemsetSelector.getMinSampleCount());
        assertEquals(EngineTuningConfig.DEFAULT_SAMPLING_SUPPORT_SCALE,
                ExclusiveFrequentItemsetSelector.getSamplingSupportScale(), 1e-12);
        assertEquals(EngineTuningConfig.PICKER_DEFAULT_MIN_NET_GAIN,
                ExclusiveFrequentItemsetSelector.getPickerMinNetGain());
        assertEquals(EngineTuningConfig.PICKER_ESTIMATED_BYTES_PER_TERM,
                ExclusiveFrequentItemsetSelector.getPickerEstimatedBytesPerTerm());
        assertEquals(EngineTuningConfig.PICKER_DEFAULT_COVERAGE_REWARD_PER_TERM,
                ExclusiveFrequentItemsetSelector.getPickerCoverageRewardPerTerm());
    }

    @Test
    void computeSampledMinSupport_autoScale_shouldFollowSampleRatio() {
        int sampled = ExclusiveFrequentItemsetSelector.computeSampledMinSupport(100, 1000, 300, 0.0d);
        assertEquals(30, sampled);
    }

    @Test
    void computeSampledMinSupport_explicitScale_shouldOverrideAutoRatio() {
        int sampled = ExclusiveFrequentItemsetSelector.computeSampledMinSupport(100, 1000, 300, 0.5d);
        assertEquals(50, sampled);
    }

    @Test
    void computeSampledMinSupport_shouldClampToAtLeastOne() {
        int sampled = ExclusiveFrequentItemsetSelector.computeSampledMinSupport(1, 1000, 1, 0.0d);
        assertEquals(1, sampled);
    }

    @Test
    void computeSampledMinSupport_invalidArgs_shouldThrow() {
        assertThrows(IllegalArgumentException.class,
                () -> ExclusiveFrequentItemsetSelector.computeSampledMinSupport(0, 100, 10, 0.0d));
        assertThrows(IllegalArgumentException.class,
                () -> ExclusiveFrequentItemsetSelector.computeSampledMinSupport(10, 0, 10, 0.0d));
        assertThrows(IllegalArgumentException.class,
                () -> ExclusiveFrequentItemsetSelector.computeSampledMinSupport(10, 100, 101, 0.0d));
    }

    @Test
    void computeTargetSampleSize_shouldRespectRatioMinAndUpperBound() {
        assertEquals(300, ExclusiveFrequentItemsetSelector.computeTargetSampleSize(1000, 0.3d, 64));
        assertEquals(64, ExclusiveFrequentItemsetSelector.computeTargetSampleSize(1000, 0.0d, 64));
        assertEquals(1000, ExclusiveFrequentItemsetSelector.computeTargetSampleSize(1000, 1.0d, 64));
        assertEquals(1000, ExclusiveFrequentItemsetSelector.computeTargetSampleSize(1000, 0.2d, 5000));
    }

    @Test
    void computeTargetSampleSize_invalidArgs_shouldThrow() {
        assertThrows(IllegalArgumentException.class,
                () -> ExclusiveFrequentItemsetSelector.computeTargetSampleSize(0, 0.5d, 10));
    }
}
