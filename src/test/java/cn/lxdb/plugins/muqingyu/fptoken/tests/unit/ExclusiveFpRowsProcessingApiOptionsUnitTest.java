package cn.lxdb.plugins.muqingyu.fptoken.tests.unit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import cn.lxdb.plugins.muqingyu.fptoken.api.ExclusiveFpRowsProcessingApi;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.ByteRef;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.config.EngineTuningConfig;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.DocTerms;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;

class ExclusiveFpRowsProcessingApiOptionsUnitTest {

    @Test
    void defaultOptions_shouldMatchEngineDefaults() {
        ExclusiveFpRowsProcessingApi.ProcessingOptions options = ExclusiveFpRowsProcessingApi.defaultOptions();

        assertEquals(EngineTuningConfig.DEFAULT_RUNNER_MIN_SUPPORT, options.getMinSupport());
        assertEquals(EngineTuningConfig.DEFAULT_RUNNER_MIN_ITEMSET_SIZE, options.getMinItemsetSize());
        assertEquals(EngineTuningConfig.DEFAULT_MAX_ITEMSET_SIZE, options.getMaxItemsetSize());
        assertEquals(EngineTuningConfig.DEFAULT_MAX_CANDIDATE_COUNT, options.getMaxCandidateCount());
        assertEquals(EngineTuningConfig.DEFAULT_HOT_TERM_THRESHOLD_EXCLUSIVE, options.getHotTermThresholdExclusive());
        assertEquals(EngineTuningConfig.DEFAULT_NGRAM_START, options.getNgramStart());
        assertEquals(EngineTuningConfig.DEFAULT_NGRAM_END, options.getNgramEnd());
        assertEquals(EngineTuningConfig.DEFAULT_SKIP_HASH_MIN_GRAM, options.getSkipHashMinGram());
        assertEquals(EngineTuningConfig.DEFAULT_SKIP_HASH_MAX_GRAM, options.getSkipHashMaxGram());
        assertEquals(EngineTuningConfig.DEFAULT_SAMPLE_RATIO, options.getSampleRatio());
        assertEquals(EngineTuningConfig.DEFAULT_MIN_SAMPLE_COUNT, options.getMinSampleCount());
        assertEquals(EngineTuningConfig.DEFAULT_SAMPLING_SUPPORT_SCALE, options.getSamplingSupportScale());
        assertEquals(EngineTuningConfig.PICKER_DEFAULT_MIN_NET_GAIN, options.getPickerMinNetGain());
        assertEquals(EngineTuningConfig.PICKER_ESTIMATED_BYTES_PER_TERM, options.getPickerEstimatedBytesPerTerm());
        assertEquals(EngineTuningConfig.PICKER_DEFAULT_COVERAGE_REWARD_PER_TERM,
                options.getPickerCoverageRewardPerTerm());
        assertEquals(0, options.getHintBoostWeight());
        assertEquals(ExclusiveFpRowsProcessingApi.HintValidationMode.FILTER_ONLY, options.getHintValidationMode());
        assertEquals(0, options.getPremergeMutexGroupHints().size());
        assertEquals(0, options.getPremergeSingleTermHints().size());
    }

    @Test
    void optionsWithers_shouldKeepOriginalInstanceImmutable() {
        ExclusiveFpRowsProcessingApi.ProcessingOptions base = ExclusiveFpRowsProcessingApi.defaultOptions();
        ExclusiveFpRowsProcessingApi.ProcessingOptions changed = base
                .withMinSupport(99)
                .withMinItemsetSize(3)
                .withMaxItemsetSize(7)
                .withMaxCandidateCount(190_000)
                .withNgramRange(3, 5)
                .withSkipHashGramRange(3, 6)
                .withSampleRatio(0.42d)
                .withMinSampleCount(88)
                .withSamplingSupportScale(0.7d)
                .withPickerMinNetGain(2)
                .withPickerEstimatedBytesPerTerm(1)
                .withPickerCoverageRewardPerTerm(5)
                .withHintBoostWeight(9)
                .withHintValidationMode(ExclusiveFpRowsProcessingApi.HintValidationMode.STRICT)
                .withPremergeSingleTermHints(Arrays.asList(
                        new ExclusiveFpRowsProcessingApi.PremergeHint(
                                Arrays.asList(new ByteRef(bytes("A"), 0, 1)))));

        assertEquals(EngineTuningConfig.DEFAULT_RUNNER_MIN_SUPPORT, base.getMinSupport());
        assertEquals(EngineTuningConfig.DEFAULT_RUNNER_MIN_ITEMSET_SIZE, base.getMinItemsetSize());
        assertEquals(EngineTuningConfig.DEFAULT_MAX_ITEMSET_SIZE, base.getMaxItemsetSize());
        assertEquals(EngineTuningConfig.DEFAULT_MAX_CANDIDATE_COUNT, base.getMaxCandidateCount());
        assertEquals(EngineTuningConfig.DEFAULT_NGRAM_START, base.getNgramStart());
        assertEquals(EngineTuningConfig.DEFAULT_SKIP_HASH_MIN_GRAM, base.getSkipHashMinGram());
        assertEquals(EngineTuningConfig.DEFAULT_SAMPLE_RATIO, base.getSampleRatio());
        assertEquals(EngineTuningConfig.DEFAULT_MIN_SAMPLE_COUNT, base.getMinSampleCount());
        assertEquals(EngineTuningConfig.PICKER_DEFAULT_MIN_NET_GAIN, base.getPickerMinNetGain());
        assertEquals(EngineTuningConfig.PICKER_ESTIMATED_BYTES_PER_TERM, base.getPickerEstimatedBytesPerTerm());
        assertEquals(EngineTuningConfig.PICKER_DEFAULT_COVERAGE_REWARD_PER_TERM,
                base.getPickerCoverageRewardPerTerm());
        assertEquals(0, base.getHintBoostWeight());
        assertEquals(ExclusiveFpRowsProcessingApi.HintValidationMode.FILTER_ONLY, base.getHintValidationMode());
        assertEquals(0, base.getPremergeMutexGroupHints().size());
        assertEquals(0, base.getPremergeSingleTermHints().size());

        assertEquals(99, changed.getMinSupport());
        assertEquals(3, changed.getMinItemsetSize());
        assertEquals(7, changed.getMaxItemsetSize());
        assertEquals(190_000, changed.getMaxCandidateCount());
        assertEquals(3, changed.getNgramStart());
        assertEquals(5, changed.getNgramEnd());
        assertEquals(3, changed.getSkipHashMinGram());
        assertEquals(6, changed.getSkipHashMaxGram());
        assertEquals(0.42d, changed.getSampleRatio());
        assertEquals(88, changed.getMinSampleCount());
        assertEquals(0.7d, changed.getSamplingSupportScale());
        assertEquals(2, changed.getPickerMinNetGain());
        assertEquals(1, changed.getPickerEstimatedBytesPerTerm());
        assertEquals(5, changed.getPickerCoverageRewardPerTerm());
        assertEquals(9, changed.getHintBoostWeight());
        assertEquals(ExclusiveFpRowsProcessingApi.HintValidationMode.STRICT, changed.getHintValidationMode());
        assertEquals(0, changed.getPremergeMutexGroupHints().size());
        assertEquals(1, changed.getPremergeSingleTermHints().size());
    }

    @Test
    void compressionFocusedOptions_shouldUseCompressionProfileDefaults() {
        ExclusiveFpRowsProcessingApi.ProcessingOptions options = ExclusiveFpRowsProcessingApi.compressionFocusedOptions();
        assertEquals(EngineTuningConfig.COMPRESSION_FOCUSED_MAX_ITEMSET_SIZE, options.getMaxItemsetSize());
        assertEquals(EngineTuningConfig.COMPRESSION_FOCUSED_MAX_CANDIDATE_COUNT, options.getMaxCandidateCount());
        assertEquals(EngineTuningConfig.COMPRESSION_FOCUSED_SAMPLE_RATIO, options.getSampleRatio());
        assertEquals(EngineTuningConfig.COMPRESSION_FOCUSED_MIN_SAMPLE_COUNT, options.getMinSampleCount());
        assertEquals(EngineTuningConfig.COMPRESSION_FOCUSED_SAMPLING_SUPPORT_SCALE, options.getSamplingSupportScale());
        assertEquals(EngineTuningConfig.COMPRESSION_FOCUSED_PICKER_MIN_NET_GAIN, options.getPickerMinNetGain());
        assertEquals(EngineTuningConfig.COMPRESSION_FOCUSED_PICKER_ESTIMATED_BYTES_PER_TERM,
                options.getPickerEstimatedBytesPerTerm());
        assertEquals(EngineTuningConfig.COMPRESSION_FOCUSED_PICKER_COVERAGE_REWARD_PER_TERM,
                options.getPickerCoverageRewardPerTerm());
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
        assertThrows(
                IllegalArgumentException.class,
                () -> ExclusiveFpRowsProcessingApi.IntermediateSteps.validateProcessingOptions(
                        ExclusiveFpRowsProcessingApi.defaultOptions().withMaxItemsetSize(0))
        );
        assertThrows(
                IllegalArgumentException.class,
                () -> ExclusiveFpRowsProcessingApi.IntermediateSteps.validateProcessingOptions(
                        ExclusiveFpRowsProcessingApi.defaultOptions().withMaxItemsetSize(1).withMinItemsetSize(2))
        );
        assertThrows(
                IllegalArgumentException.class,
                () -> ExclusiveFpRowsProcessingApi.IntermediateSteps.validateProcessingOptions(
                        ExclusiveFpRowsProcessingApi.defaultOptions().withMaxCandidateCount(0))
        );
        assertThrows(
                IllegalArgumentException.class,
                () -> ExclusiveFpRowsProcessingApi.IntermediateSteps.validateProcessingOptions(
                        ExclusiveFpRowsProcessingApi.defaultOptions().withSampleRatio(-0.01d))
        );
        assertThrows(
                IllegalArgumentException.class,
                () -> ExclusiveFpRowsProcessingApi.IntermediateSteps.validateProcessingOptions(
                        ExclusiveFpRowsProcessingApi.defaultOptions().withMinSampleCount(0))
        );
        assertThrows(
                IllegalArgumentException.class,
                () -> ExclusiveFpRowsProcessingApi.IntermediateSteps.validateProcessingOptions(
                        ExclusiveFpRowsProcessingApi.defaultOptions().withPickerMinNetGain(-1))
        );
        assertThrows(
                IllegalArgumentException.class,
                () -> ExclusiveFpRowsProcessingApi.IntermediateSteps.validateProcessingOptions(
                        ExclusiveFpRowsProcessingApi.defaultOptions().withPickerEstimatedBytesPerTerm(0))
        );
        assertThrows(
                IllegalArgumentException.class,
                () -> ExclusiveFpRowsProcessingApi.IntermediateSteps.validateProcessingOptions(
                        ExclusiveFpRowsProcessingApi.defaultOptions().withPickerCoverageRewardPerTerm(-1))
        );
        assertThrows(
                IllegalArgumentException.class,
                () -> ExclusiveFpRowsProcessingApi.IntermediateSteps.validateProcessingOptions(
                        ExclusiveFpRowsProcessingApi.defaultOptions().withHintBoostWeight(-1))
        );
        assertThrows(
                IllegalArgumentException.class,
                () -> ExclusiveFpRowsProcessingApi.IntermediateSteps.validateProcessingOptions(
                        ExclusiveFpRowsProcessingApi.defaultOptions()
                                .withHintValidationMode(ExclusiveFpRowsProcessingApi.HintValidationMode.STRICT)
                                .withPremergeMutexGroupHints(Arrays.asList(
                                        new ExclusiveFpRowsProcessingApi.PremergeHint(
                                                Collections.emptyList()))))
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
