package cn.lxdb.plugins.muqingyu.fptoken.tests.unit;

import static org.junit.jupiter.api.Assertions.assertEquals;

import cn.lxdb.plugins.muqingyu.fptoken.ExclusiveFrequentItemsetSelector;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.config.EngineTuningConfig;
import org.junit.jupiter.api.Test;

class ExclusiveFrequentItemsetSelectorConfigFileTest {

    @Test
    void codeConfig_setters_shouldTakeEffectImmediately() {
        double oldRatio = ExclusiveFrequentItemsetSelector.getSampleRatio();
        int oldMin = ExclusiveFrequentItemsetSelector.getMinSampleCount();
        double oldScale = ExclusiveFrequentItemsetSelector.getSamplingSupportScale();
        try {
            ExclusiveFrequentItemsetSelector.setSampleRatio(0.42d);
            ExclusiveFrequentItemsetSelector.setMinSampleCount(77);
            ExclusiveFrequentItemsetSelector.setSamplingSupportScale(0.88d);

            assertEquals(0.42d, ExclusiveFrequentItemsetSelector.getSampleRatio(), 1e-9);
            assertEquals(77, ExclusiveFrequentItemsetSelector.getMinSampleCount());
            assertEquals(0.88d, ExclusiveFrequentItemsetSelector.getSamplingSupportScale(), 1e-9);
        } finally {
            ExclusiveFrequentItemsetSelector.setSampleRatio(oldRatio);
            ExclusiveFrequentItemsetSelector.setMinSampleCount(oldMin);
            ExclusiveFrequentItemsetSelector.setSamplingSupportScale(oldScale);
        }
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

        ExclusiveFrequentItemsetSelector.setSampleRatio(EngineTuningConfig.DEFAULT_SAMPLE_RATIO);
        ExclusiveFrequentItemsetSelector.setMinSampleCount(EngineTuningConfig.DEFAULT_MIN_SAMPLE_COUNT);
        ExclusiveFrequentItemsetSelector.setSamplingSupportScale(EngineTuningConfig.DEFAULT_SAMPLING_SUPPORT_SCALE);

        assertEquals(EngineTuningConfig.DEFAULT_SAMPLE_RATIO, ExclusiveFrequentItemsetSelector.getSampleRatio(), 1e-12);
        assertEquals(EngineTuningConfig.DEFAULT_MIN_SAMPLE_COUNT, ExclusiveFrequentItemsetSelector.getMinSampleCount());
        assertEquals(EngineTuningConfig.DEFAULT_SAMPLING_SUPPORT_SCALE,
                ExclusiveFrequentItemsetSelector.getSamplingSupportScale(), 1e-12);
    }
}
