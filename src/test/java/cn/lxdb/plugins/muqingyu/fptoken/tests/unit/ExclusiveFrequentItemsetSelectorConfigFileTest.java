package cn.lxdb.plugins.muqingyu.fptoken.tests.unit;

import static org.junit.jupiter.api.Assertions.assertEquals;

import cn.lxdb.plugins.muqingyu.fptoken.ExclusiveFrequentItemsetSelector;
import cn.lxdb.plugins.muqingyu.fptoken.config.EngineTuningConfig;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

class ExclusiveFrequentItemsetSelectorConfigFileTest {

    @Test
    void reloadSamplingConfig_readsValuesFromPropertiesFile() throws Exception {
        Path temp = Files.createTempFile("fptoken-config-", ".properties");
        String content = ""
                + "fptoken.sampling.sampleRatio=0.42\n"
                + "fptoken.sampling.minSampleCount=77\n"
                + "fptoken.sampling.supportScale=0.88\n";
        Files.write(temp, content.getBytes(StandardCharsets.UTF_8));

        String oldPath = System.getProperty("fptoken.config.file");
        double oldRatio = ExclusiveFrequentItemsetSelector.getSampleRatio();
        int oldMin = ExclusiveFrequentItemsetSelector.getMinSampleCount();
        double oldScale = ExclusiveFrequentItemsetSelector.getSamplingSupportScale();
        try {
            System.setProperty("fptoken.config.file", temp.toString());
            ExclusiveFrequentItemsetSelector.reloadSamplingConfig();

            assertEquals(0.42d, ExclusiveFrequentItemsetSelector.getSampleRatio(), 1e-9);
            assertEquals(77, ExclusiveFrequentItemsetSelector.getMinSampleCount());
            assertEquals(0.88d, ExclusiveFrequentItemsetSelector.getSamplingSupportScale(), 1e-9);
        } finally {
            if (oldPath == null) {
                System.clearProperty("fptoken.config.file");
            } else {
                System.setProperty("fptoken.config.file", oldPath);
            }
            ExclusiveFrequentItemsetSelector.setSampleRatio(oldRatio);
            ExclusiveFrequentItemsetSelector.setMinSampleCount(oldMin);
            ExclusiveFrequentItemsetSelector.setSamplingSupportScale(oldScale);
            Files.deleteIfExists(temp);
        }
    }

    @Test
    void reloadSamplingConfig_missingFile_keepsCurrentValues() {
        String oldPath = System.getProperty("fptoken.config.file");
        double oldRatio = ExclusiveFrequentItemsetSelector.getSampleRatio();
        int oldMin = ExclusiveFrequentItemsetSelector.getMinSampleCount();
        double oldScale = ExclusiveFrequentItemsetSelector.getSamplingSupportScale();
        try {
            System.setProperty("fptoken.config.file", "__missing__/__missing__.properties");
            ExclusiveFrequentItemsetSelector.reloadSamplingConfig();

            assertEquals(oldRatio, ExclusiveFrequentItemsetSelector.getSampleRatio(), 1e-12);
            assertEquals(oldMin, ExclusiveFrequentItemsetSelector.getMinSampleCount());
            assertEquals(oldScale, ExclusiveFrequentItemsetSelector.getSamplingSupportScale(), 1e-12);
        } finally {
            if (oldPath == null) {
                System.clearProperty("fptoken.config.file");
            } else {
                System.setProperty("fptoken.config.file", oldPath);
            }
            ExclusiveFrequentItemsetSelector.setSampleRatio(EngineTuningConfig.DEFAULT_SAMPLE_RATIO);
            ExclusiveFrequentItemsetSelector.setMinSampleCount(EngineTuningConfig.DEFAULT_MIN_SAMPLE_COUNT);
            ExclusiveFrequentItemsetSelector.setSamplingSupportScale(EngineTuningConfig.DEFAULT_SAMPLING_SUPPORT_SCALE);
        }
    }

    @Test
    void reloadSamplingConfig_invalidValues_shouldKeepCurrentValues() throws Exception {
        Path temp = Files.createTempFile("fptoken-config-invalid-", ".properties");
        String content = ""
                + "fptoken.sampling.sampleRatio=not-a-number\n"
                + "fptoken.sampling.minSampleCount=bad-int\n"
                + "fptoken.sampling.supportScale=bad-double\n";
        Files.write(temp, content.getBytes(StandardCharsets.UTF_8));

        String oldPath = System.getProperty("fptoken.config.file");
        double oldRatio = ExclusiveFrequentItemsetSelector.getSampleRatio();
        int oldMin = ExclusiveFrequentItemsetSelector.getMinSampleCount();
        double oldScale = ExclusiveFrequentItemsetSelector.getSamplingSupportScale();
        try {
            System.setProperty("fptoken.config.file", temp.toString());
            ExclusiveFrequentItemsetSelector.reloadSamplingConfig();

            assertEquals(oldRatio, ExclusiveFrequentItemsetSelector.getSampleRatio(), 1e-12);
            assertEquals(oldMin, ExclusiveFrequentItemsetSelector.getMinSampleCount());
            assertEquals(oldScale, ExclusiveFrequentItemsetSelector.getSamplingSupportScale(), 1e-12);
        } finally {
            if (oldPath == null) {
                System.clearProperty("fptoken.config.file");
            } else {
                System.setProperty("fptoken.config.file", oldPath);
            }
            Files.deleteIfExists(temp);
        }
    }
}
