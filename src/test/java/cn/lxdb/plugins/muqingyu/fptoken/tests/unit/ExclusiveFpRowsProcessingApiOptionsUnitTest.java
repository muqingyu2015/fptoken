package cn.lxdb.plugins.muqingyu.fptoken.tests.unit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import cn.lxdb.plugins.muqingyu.fptoken.api.ExclusiveFpRowsProcessingApi;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.config.EngineTuningConfig;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.DocTerms;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

class ExclusiveFpRowsProcessingApiOptionsUnitTest {

    @Test
    void defaultOptions_shouldMatchEngineDefaults() {
        ExclusiveFpRowsProcessingApi.ProcessingOptions options = ExclusiveFpRowsProcessingApi.defaultOptions();

        assertEquals(EngineTuningConfig.DEFAULT_RUNNER_MIN_SUPPORT, options.getMinSupport());
        assertEquals(EngineTuningConfig.DEFAULT_RUNNER_MIN_ITEMSET_SIZE, options.getMinItemsetSize());
        assertEquals(EngineTuningConfig.DEFAULT_HOT_TERM_THRESHOLD_EXCLUSIVE, options.getHotTermThresholdExclusive());
        assertEquals(EngineTuningConfig.DEFAULT_NGRAM_START, options.getNgramStart());
        assertEquals(EngineTuningConfig.DEFAULT_NGRAM_END, options.getNgramEnd());
        assertEquals(EngineTuningConfig.DEFAULT_SKIP_HASH_MIN_GRAM, options.getSkipHashMinGram());
        assertEquals(EngineTuningConfig.DEFAULT_SKIP_HASH_MAX_GRAM, options.getSkipHashMaxGram());
    }

    @Test
    void optionsWithers_shouldKeepOriginalInstanceImmutable() {
        ExclusiveFpRowsProcessingApi.ProcessingOptions base = ExclusiveFpRowsProcessingApi.defaultOptions();
        ExclusiveFpRowsProcessingApi.ProcessingOptions changed = base
                .withMinSupport(99)
                .withMinItemsetSize(3)
                .withNgramRange(3, 5)
                .withSkipHashGramRange(3, 6);

        assertEquals(EngineTuningConfig.DEFAULT_RUNNER_MIN_SUPPORT, base.getMinSupport());
        assertEquals(EngineTuningConfig.DEFAULT_RUNNER_MIN_ITEMSET_SIZE, base.getMinItemsetSize());
        assertEquals(EngineTuningConfig.DEFAULT_NGRAM_START, base.getNgramStart());
        assertEquals(EngineTuningConfig.DEFAULT_SKIP_HASH_MIN_GRAM, base.getSkipHashMinGram());

        assertEquals(99, changed.getMinSupport());
        assertEquals(3, changed.getMinItemsetSize());
        assertEquals(3, changed.getNgramStart());
        assertEquals(5, changed.getNgramEnd());
        assertEquals(3, changed.getSkipHashMinGram());
        assertEquals(6, changed.getSkipHashMaxGram());
    }

    @Test
    void validateProcessingOptions_shouldRejectInvalidCombinations() {
        assertThrows(
                IllegalArgumentException.class,
                () -> ExclusiveFpRowsProcessingApi.IntermediateSteps.validateProcessingOptions(
                        ExclusiveFpRowsProcessingApi.defaultOptions().withMinSupport(0))
        );
        assertThrows(
                IllegalArgumentException.class,
                () -> ExclusiveFpRowsProcessingApi.IntermediateSteps.validateProcessingOptions(
                        ExclusiveFpRowsProcessingApi.defaultOptions().withMinItemsetSize(0))
        );
        assertThrows(
                IllegalArgumentException.class,
                () -> ExclusiveFpRowsProcessingApi.IntermediateSteps.validateProcessingOptions(
                        ExclusiveFpRowsProcessingApi.defaultOptions().withNgramRange(0, 2))
        );
        assertThrows(
                IllegalArgumentException.class,
                () -> ExclusiveFpRowsProcessingApi.IntermediateSteps.validateProcessingOptions(
                        ExclusiveFpRowsProcessingApi.defaultOptions().withSkipHashGramRange(1, 4))
        );
    }

    @Test
    void processRows_withInvalidOptions_shouldFailFast() {
        List<DocTerms> rows = new ArrayList<DocTerms>();
        rows.add(new DocTerms(0, Arrays.asList(bytes("ABCD"))));

        assertThrows(
                IllegalArgumentException.class,
                () -> ExclusiveFpRowsProcessingApi.processRows(
                        rows,
                        ExclusiveFpRowsProcessingApi.defaultOptions().withSkipHashGramRange(1, 1))
        );
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
