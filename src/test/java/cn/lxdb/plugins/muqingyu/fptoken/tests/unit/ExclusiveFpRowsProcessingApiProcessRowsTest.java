package cn.lxdb.plugins.muqingyu.fptoken.tests.unit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import cn.lxdb.plugins.muqingyu.fptoken.ExclusiveFrequentItemsetSelector;
import cn.lxdb.plugins.muqingyu.fptoken.api.ExclusiveFpRowsProcessingApi;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.config.EngineTuningConfig;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.DocTerms;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.ExclusiveSelectionResult;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.SelectedGroup;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.util.ByteArrayUtils;
import cn.lxdb.plugins.muqingyu.fptoken.runner.result.LineFileProcessingResult;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * {@link ExclusiveFpRowsProcessingApi#processRows(List, int, int, int)} 接口级契约测试。
 */
class ExclusiveFpRowsProcessingApiProcessRowsTest {

    @Test
    void processRows_shouldNotMutateGlobalSelectorRuntimeTuning() {
        double oldRatio = ExclusiveFrequentItemsetSelector.getSampleRatio();
        int oldMinSample = ExclusiveFrequentItemsetSelector.getMinSampleCount();
        double oldScale = ExclusiveFrequentItemsetSelector.getSamplingSupportScale();
        int oldMinNetGain = ExclusiveFrequentItemsetSelector.getPickerMinNetGain();
        int oldEstimatedBytes = ExclusiveFrequentItemsetSelector.getPickerEstimatedBytesPerTerm();
        int oldCoverageReward = ExclusiveFrequentItemsetSelector.getPickerCoverageRewardPerTerm();
        try {
            List<DocTerms> rows = new ArrayList<DocTerms>();
            rows.add(new DocTerms(0, Arrays.asList(bytes("ABCD"))));
            rows.add(new DocTerms(1, Arrays.asList(bytes("ABCE"))));
            rows.add(new DocTerms(2, Arrays.asList(bytes("ABCF"))));
            ExclusiveFpRowsProcessingApi.ProcessingOptions options =
                    ExclusiveFpRowsProcessingApi.defaultOptions()
                            .withSampleRatio(0.17d)
                            .withMinSampleCount(7)
                            .withSamplingSupportScale(0.25d)
                            .withPickerMinNetGain(9)
                            .withPickerEstimatedBytesPerTerm(3)
                            .withPickerCoverageRewardPerTerm(11);
            ExclusiveFpRowsProcessingApi.processRows(rows, options);
            assertEquals(oldRatio, ExclusiveFrequentItemsetSelector.getSampleRatio(), 0.000001d);
            assertEquals(oldMinSample, ExclusiveFrequentItemsetSelector.getMinSampleCount());
            assertEquals(oldScale, ExclusiveFrequentItemsetSelector.getSamplingSupportScale(), 0.000001d);
            assertEquals(oldMinNetGain, ExclusiveFrequentItemsetSelector.getPickerMinNetGain());
            assertEquals(oldEstimatedBytes, ExclusiveFrequentItemsetSelector.getPickerEstimatedBytesPerTerm());
            assertEquals(oldCoverageReward, ExclusiveFrequentItemsetSelector.getPickerCoverageRewardPerTerm());
        } finally {
            ExclusiveFrequentItemsetSelector.setSampleRatio(oldRatio);
            ExclusiveFrequentItemsetSelector.setMinSampleCount(oldMinSample);
            ExclusiveFrequentItemsetSelector.setSamplingSupportScale(oldScale);
            ExclusiveFrequentItemsetSelector.setPickerMinNetGain(oldMinNetGain);
            ExclusiveFrequentItemsetSelector.setPickerEstimatedBytesPerTerm(oldEstimatedBytes);
            ExclusiveFrequentItemsetSelector.setPickerCoverageRewardPerTerm(oldCoverageReward);
        }
    }

    @Test
    void processRows_defaultsOverload_shouldMatchLegacyDefaultParameterCall() {
        List<DocTerms> rows = new ArrayList<DocTerms>();
        rows.add(new DocTerms(0, Arrays.asList(bytes("ABCD"))));
        rows.add(new DocTerms(1, Arrays.asList(bytes("ABCE"))));
        rows.add(new DocTerms(2, Arrays.asList(bytes("ABCF"))));

        LineFileProcessingResult defaultsResult = ExclusiveFpRowsProcessingApi.processRows(rows);
        LineFileProcessingResult legacyResult = ExclusiveFpRowsProcessingApi.processRows(
                rows,
                EngineTuningConfig.DEFAULT_RUNNER_MIN_SUPPORT,
                EngineTuningConfig.DEFAULT_RUNNER_MIN_ITEMSET_SIZE,
                EngineTuningConfig.DEFAULT_HOT_TERM_THRESHOLD_EXCLUSIVE
        );

        assertEquals(groupsFingerprint(defaultsResult.getFinalIndexData().getHighFreqMutexGroupPostings()),
                groupsFingerprint(legacyResult.getFinalIndexData().getHighFreqMutexGroupPostings()));
        assertEquals(hotTermsFingerprint(defaultsResult.getFinalIndexData().getHighFreqSingleTermPostings()),
                hotTermsFingerprint(legacyResult.getFinalIndexData().getHighFreqSingleTermPostings()));
        assertEquals(rowsFingerprint(defaultsResult.getFinalIndexData().getLowHitForwardRows()),
                rowsFingerprint(legacyResult.getFinalIndexData().getLowHitForwardRows()));
        assertEquals(defaultsResult.getFinalIndexData().getSkipHashMinGram(),
                legacyResult.getFinalIndexData().getSkipHashMinGram());
        assertEquals(defaultsResult.getFinalIndexData().getSkipHashMaxGram(),
                legacyResult.getFinalIndexData().getSkipHashMaxGram());
    }

    @Test
    void processRows_optionsOverload_shouldMatchEquivalentExplicitCall() {
        List<DocTerms> rawRows = new ArrayList<DocTerms>();
        rawRows.add(new DocTerms(0, Arrays.asList(bytes("ABCD"))));
        rawRows.add(new DocTerms(1, Arrays.asList(bytes("ABCE"))));
        rawRows.add(new DocTerms(2, Arrays.asList(bytes("ABDE"))));

        ExclusiveFpRowsProcessingApi.ProcessingOptions options =
                ExclusiveFpRowsProcessingApi.defaultOptions()
                        .withMinSupport(1)
                        .withMinItemsetSize(2)
                        .withHotTermThresholdExclusive(0)
                        .withNgramRange(3, 3)
                        .withSkipHashGramRange(3, 3);

        LineFileProcessingResult optionsResult = ExclusiveFpRowsProcessingApi.processRows(rawRows, options);
        LineFileProcessingResult explicitResult = ExclusiveFpRowsProcessingApi.processRowsWithNgramAndSkipHash(
                rawRows, 3, 3, 1, 2, 0, 3, 3);

        assertEquals(groupsFingerprint(optionsResult.getFinalIndexData().getHighFreqMutexGroupPostings()),
                groupsFingerprint(explicitResult.getFinalIndexData().getHighFreqMutexGroupPostings()));
        assertEquals(hotTermsFingerprint(optionsResult.getFinalIndexData().getHighFreqSingleTermPostings()),
                hotTermsFingerprint(explicitResult.getFinalIndexData().getHighFreqSingleTermPostings()));
        assertEquals(rowsFingerprint(optionsResult.getFinalIndexData().getLowHitForwardRows()),
                rowsFingerprint(explicitResult.getFinalIndexData().getLowHitForwardRows()));
    }

    @Test
    void processRows_compressionFocusedOptions_shouldIncreaseMutexAbsorbRatio_onClusteredRows() {
        List<DocTerms> rows = buildClusteredRowsForMutexAbsorption();

        ExclusiveFpRowsProcessingApi.ProcessingOptions baseline =
                ExclusiveFpRowsProcessingApi.defaultOptions()
                        .withMinSupport(2)
                        .withMinItemsetSize(2)
                        .withHotTermThresholdExclusive(2)
                        .withPickerEstimatedBytesPerTerm(2);
        LineFileProcessingResult baselineResult = ExclusiveFpRowsProcessingApi.processRows(rows, baseline);

        ExclusiveFpRowsProcessingApi.ProcessingOptions tuned =
                ExclusiveFpRowsProcessingApi.compressionFocusedOptions()
                        .withMinSupport(2)
                        .withMinItemsetSize(2)
                        .withHotTermThresholdExclusive(2)
                        .withSampleRatio(0.65d)
                        .withMinSampleCount(16)
                        .withPickerEstimatedBytesPerTerm(1);
        LineFileProcessingResult tunedResult = ExclusiveFpRowsProcessingApi.processRows(rows, tuned);

        LineFileProcessingResult.ProcessingStats baselineStats = baselineResult.getProcessingStats();
        LineFileProcessingResult.ProcessingStats tunedStats = tunedResult.getProcessingStats();

        assertTrue(
                tunedStats.getHighFreqSingleTermMovedToMutexGroupPercent()
                        >= baselineStats.getHighFreqSingleTermMovedToMutexGroupPercent(),
                () -> "tuned absorb ratio should be >= baseline, tuned="
                        + tunedStats.getHighFreqSingleTermMovedToMutexGroupPercent()
                        + ", baseline=" + baselineStats.getHighFreqSingleTermMovedToMutexGroupPercent()
        );
        assertTrue(
                tunedStats.getSelectedGroupCount() >= baselineStats.getSelectedGroupCount(),
                () -> "tuned group count should be >= baseline, tuned="
                        + tunedStats.getSelectedGroupCount()
                        + ", baseline=" + baselineStats.getSelectedGroupCount()
        );
    }

    @Test
    void processRows_optionsOverload_shouldRejectNullOptions() {
        List<DocTerms> rows = new ArrayList<DocTerms>();
        rows.add(new DocTerms(0, Arrays.asList(bytes("AB"), bytes("BC"))));

        assertNotNull(ExclusiveFpRowsProcessingApi.defaultOptions());
        assertThrows(NullPointerException.class, () ->
                ExclusiveFpRowsProcessingApi.processRows(rows, null));
    }

    @Test
    void processRows_optionsOverload_shouldRejectInvalidAdvancedTuning() {
        List<DocTerms> rows = new ArrayList<DocTerms>();
        rows.add(new DocTerms(0, Arrays.asList(bytes("AB"), bytes("BC"))));

        assertThrows(IllegalArgumentException.class, () ->
                ExclusiveFpRowsProcessingApi.processRows(
                        rows,
                        ExclusiveFpRowsProcessingApi.defaultOptions().withMaxItemsetSize(0)
                ));
        assertThrows(IllegalArgumentException.class, () ->
                ExclusiveFpRowsProcessingApi.processRows(
                        rows,
                        ExclusiveFpRowsProcessingApi.defaultOptions().withMaxCandidateCount(0)
                ));
        assertThrows(IllegalArgumentException.class, () ->
                ExclusiveFpRowsProcessingApi.processRows(
                        rows,
                        ExclusiveFpRowsProcessingApi.defaultOptions().withSampleRatio(1.2d)
                ));
        assertThrows(IllegalArgumentException.class, () ->
                ExclusiveFpRowsProcessingApi.processRows(
                        rows,
                        ExclusiveFpRowsProcessingApi.defaultOptions().withMinSampleCount(0)
                ));
        assertThrows(IllegalArgumentException.class, () ->
                ExclusiveFpRowsProcessingApi.processRows(
                        rows,
                        ExclusiveFpRowsProcessingApi.defaultOptions().withPickerMinNetGain(-1)
                ));
        assertThrows(IllegalArgumentException.class, () ->
                ExclusiveFpRowsProcessingApi.processRows(
                        rows,
                        ExclusiveFpRowsProcessingApi.defaultOptions().withPickerEstimatedBytesPerTerm(0)
                ));
        assertThrows(IllegalArgumentException.class, () ->
                ExclusiveFpRowsProcessingApi.processRows(
                        rows,
                        ExclusiveFpRowsProcessingApi.defaultOptions().withPickerCoverageRewardPerTerm(-1)
                ));
    }

    @Test
    void processRows_shouldReturnEmptyStructures_whenRowsEmpty() {
        LineFileProcessingResult result = ExclusiveFpRowsProcessingApi.processRows(
                new ArrayList<DocTerms>(), 1, 2, 1);

        assertTrue(result.getLoadedRows().isEmpty());
        assertTrue(result.getSelectionResult().getGroups().isEmpty());
        assertTrue(result.getFinalIndexData().getHighFreqMutexGroupPostings().isEmpty());
        assertTrue(result.getFinalIndexData().getHighFreqSingleTermPostings().isEmpty());
        assertTrue(result.getFinalIndexData().getLowHitForwardRows().isEmpty());
    }

    @Test
    void processRows_shouldRejectInvalidArguments() {
        List<DocTerms> rows = new ArrayList<DocTerms>();
        rows.add(new DocTerms(0, Arrays.asList(bytes("AA"), bytes("AB"))));

        assertThrows(IllegalArgumentException.class, () ->
                ExclusiveFpRowsProcessingApi.processRows(rows, 0, 2, 1));
        assertThrows(IllegalArgumentException.class, () ->
                ExclusiveFpRowsProcessingApi.processRows(rows, 1, 0, 1));
    }

    @Test
    void processRows_shouldUseStrictGreaterThan_forHotTermThreshold() {
        // A 出现 2 次、B 出现 3 次；阈值=2 时应只保留 B（严格 >2）
        List<DocTerms> rows = new ArrayList<DocTerms>();
        rows.add(new DocTerms(0, Arrays.asList(bytes("A"), bytes("B"))));
        rows.add(new DocTerms(1, Arrays.asList(bytes("A"), bytes("B"))));
        rows.add(new DocTerms(2, Arrays.asList(bytes("B"), bytes("C"))));

        // minSupport 设大，避免产生互斥组合，聚焦 hotTermThreshold 语义
        LineFileProcessingResult result = ExclusiveFpRowsProcessingApi.processRows(rows, 100, 2, 2);
        Map<String, List<Integer>> hotMap = toHotMap(result.getFinalIndexData().getHighFreqSingleTermPostings());

        assertFalse(hotMap.containsKey(hex("A")));
        assertTrue(hotMap.containsKey(hex("B")));
        assertEquals(Arrays.asList(0, 1, 2), hotMap.get(hex("B")));
    }

    @Test
    void processRows_shouldDefensivelyCopyInputRows() {
        byte[] mutable = bytes("AB");
        List<DocTerms> rows = new ArrayList<DocTerms>();
        rows.add(new DocTerms(0, Arrays.asList(mutable, bytes("BC"))));
        rows.add(new DocTerms(1, Arrays.asList(bytes("AB"), bytes("CD"))));

        LineFileProcessingResult result = ExclusiveFpRowsProcessingApi.processRows(rows, 1, 2, 1);

        // 调用后篡改调用方原始数组，不应影响 result 内部快照
        mutable[0] = 'Z';
        mutable[1] = 'Z';
        rows.clear();

        String loadedFingerprint = rowsFingerprint(result.getLoadedRows());
        assertTrue(loadedFingerprint.contains(hex("AB")));
        assertFalse(loadedFingerprint.contains(hex("ZZ")));
    }

    @Test
    void processRows_shouldKeepNewAndLegacyAccessorsConsistent() {
        List<DocTerms> rows = new ArrayList<DocTerms>();
        rows.add(new DocTerms(0, Arrays.asList(bytes("AA"), bytes("AB"), bytes("AC"))));
        rows.add(new DocTerms(1, Arrays.asList(bytes("AA"), bytes("AB"))));
        rows.add(new DocTerms(2, Arrays.asList(bytes("AA"), bytes("AD"))));

        LineFileProcessingResult result = ExclusiveFpRowsProcessingApi.processRows(rows, 2, 2, 1);
        LineFileProcessingResult.FinalIndexData finalData = result.getFinalIndexData();

        // 新命名访问器 vs 兼容访问器必须一致
        assertEquals(groupsFingerprint(finalData.getHighFreqMutexGroupPostings()),
                groupsFingerprint(finalData.getGroups()));
        assertEquals(hotTermsFingerprint(finalData.getHighFreqSingleTermPostings()),
                hotTermsFingerprint(finalData.getHotTerms()));
        assertEquals(rowsFingerprint(finalData.getLowHitForwardRows()),
                rowsFingerprint(finalData.getCutRes()));

        // 顶层便捷访问器也应与 FinalIndexData 一致
        assertEquals(groupsFingerprint(result.getGroups()),
                groupsFingerprint(finalData.getHighFreqMutexGroupPostings()));
        assertEquals(groupsFingerprint(result.getHighFreqMutexGroupPostings()),
                groupsFingerprint(finalData.getHighFreqMutexGroupPostings()));
        assertEquals(hotTermsFingerprint(result.getHotTerms()),
                hotTermsFingerprint(finalData.getHighFreqSingleTermPostings()));
        assertEquals(hotTermsFingerprint(result.getHighFreqSingleTermPostings()),
                hotTermsFingerprint(finalData.getHighFreqSingleTermPostings()));
        assertEquals(rowsFingerprint(result.getCutRes()),
                rowsFingerprint(finalData.getLowHitForwardRows()));
        assertEquals(rowsFingerprint(result.getLowHitForwardRows()),
                rowsFingerprint(finalData.getLowHitForwardRows()));
    }

    @Test
    void processRows_processingStats_shouldAggregateScatteredCounters() {
        List<DocTerms> rows = new ArrayList<DocTerms>();
        rows.add(new DocTerms(10, Arrays.asList(bytes("ABCD"))));
        rows.add(new DocTerms(11, Arrays.asList(bytes("ABCE"))));
        rows.add(new DocTerms(12, Arrays.asList(bytes("ABDE"))));

        LineFileProcessingResult result = ExclusiveFpRowsProcessingApi.processRows(rows, 1, 2, 0);
        LineFileProcessingResult.FinalIndexData finalData = result.getFinalIndexData();
        LineFileProcessingResult.ProcessingStats stats = result.getProcessingStats();

        assertEquals(result.getLoadedRows().size(), stats.getInputRowCount());
        assertEquals(finalData.getHighFreqMutexGroupPostings().size(), stats.getSelectedGroupCount());
        assertEquals(finalData.getHighFreqSingleTermPostings().size(), stats.getHighFreqSingleTermCount());
        assertEquals(finalData.getLowHitForwardRows().size(), stats.getLowHitForwardRowCount());
        assertEquals(finalData.getHighFreqMutexGroupTermsToIndex().size(), stats.getHighFreqMutexGroupTermsToIndexCount());
        assertEquals(finalData.getHighFreqSingleTermToIndex().size(), stats.getHighFreqSingleTermToIndexCount());
        assertEquals(finalData.getLowHitTermToIndexes().size(), stats.getLowHitTermToIndexesCount());
        assertEquals(result.getSelectionResult().getFrequentTermCount(), stats.getFrequentTermCount());
        assertEquals(result.getSelectionResult().getCandidateCount(), stats.getCandidateCount());
        assertEquals(result.getSelectionResult().getIntersectionCount(), stats.getIntersectionCount());
        assertEquals(result.getSelectionResult().getMaxCandidateCount(), stats.getMaxCandidateCount());
        assertEquals(result.getSelectionResult().isTruncatedByCandidateLimit(), stats.isTruncatedByCandidateLimit());
        assertEquals(finalData.getHotTermThresholdExclusive(), stats.getHotTermThresholdExclusive());
        assertEquals(finalData.getSkipHashMinGram(), stats.getSkipHashMinGram());
        assertEquals(finalData.getSkipHashMaxGram(), stats.getSkipHashMaxGram());
        assertEquals(finalData.getOneByteDocidBitsetIndex().getMaxDocId(), stats.getOneByteIndexMaxDocId());
        assertTrue(stats.getHighFreqSingleTermCandidateCountBeforeDedup() >= 0);
        assertTrue(stats.getHighFreqSingleTermMovedToMutexGroupCount() >= 0);
        assertTrue(stats.getHighFreqSingleTermMovedToMutexGroupCount()
                <= stats.getHighFreqSingleTermCandidateCountBeforeDedup());
        assertTrue(stats.getHighFreqSingleTermMovedToMutexGroupPercent() >= 0.0d);
        assertTrue(stats.getLowHitForwardRowsPercent() >= 0.0d);
        assertTrue(stats.getLowHitForwardTermsPercent() >= 0.0d);
    }

    @Test
    void processingStats_shouldReportMutexAbsorbRatio_andLowHitRatio() {
        List<DocTerms> loadedRows = new ArrayList<DocTerms>();
        loadedRows.add(new DocTerms(0, Arrays.asList(bytes("A"), bytes("B"), bytes("X"))));
        loadedRows.add(new DocTerms(1, Arrays.asList(bytes("A"), bytes("B"), bytes("Y"))));
        loadedRows.add(new DocTerms(2, Arrays.asList(bytes("A"), bytes("C"), bytes("Y"))));
        loadedRows.add(new DocTerms(3, Arrays.asList(bytes("A"), bytes("D"), bytes("Z"))));

        List<SelectedGroup> groups = new ArrayList<SelectedGroup>();
        groups.add(new SelectedGroup(Arrays.asList(bytes("A"), bytes("B")), Arrays.asList(0, 1), 2, 0));

        ExclusiveSelectionResult selectionResult = new ExclusiveSelectionResult(
                groups,
                6,
                12,
                20,
                1000,
                false
        );

        List<DocTerms> lowHitRows = new ArrayList<DocTerms>();
        lowHitRows.add(new DocTerms(0, Arrays.asList(bytes("X"))));
        lowHitRows.add(new DocTerms(2, Arrays.asList(bytes("Y"))));

        List<LineFileProcessingResult.HotTermDocList> hotTerms =
                new ArrayList<LineFileProcessingResult.HotTermDocList>();
        hotTerms.add(new LineFileProcessingResult.HotTermDocList(bytes("Y"), Arrays.asList(1, 2)));

        LineFileProcessingResult.DerivedData derivedData =
                new LineFileProcessingResult.DerivedData(1, lowHitRows, hotTerms);

        LineFileProcessingResult result = new LineFileProcessingResult(
                loadedRows,
                selectionResult,
                derivedData
        );
        LineFileProcessingResult.ProcessingStats stats = result.getProcessingStats();

        // 高频单词候选（阈值=1）共有 A、B、Y 三个，其中 A、B 被组合层吸收
        assertEquals(3, stats.getHighFreqSingleTermCandidateCountBeforeDedup());
        assertEquals(2, stats.getHighFreqSingleTermMovedToMutexGroupCount());
        assertEquals((2.0d * 100.0d) / 3.0d,
                stats.getHighFreqSingleTermMovedToMutexGroupPercent(),
                0.0001d);

        // 低频层：2 行 / 总 4 行 = 50%
        assertEquals(50.0d, stats.getLowHitForwardRowsPercent(), 0.0001d);
        // 低频层词项：2 词 / 总 12 词 = 16.666...%
        assertEquals((2.0d * 100.0d) / 12.0d, stats.getLowHitForwardTermsPercent(), 0.0001d);
    }

    @Test
    void processRows_shouldBuildIndexBasedInvertedViews_withoutChangingOriginalStructures() {
        List<DocTerms> rows = new ArrayList<DocTerms>();
        rows.add(new DocTerms(0, Arrays.asList(bytes("AA"), bytes("AB"), bytes("LX"))));
        rows.add(new DocTerms(1, Arrays.asList(bytes("AA"), bytes("AB"), bytes("LY"))));
        rows.add(new DocTerms(2, Arrays.asList(bytes("AA"), bytes("AZ"), bytes("LY"))));

        LineFileProcessingResult result = ExclusiveFpRowsProcessingApi.processRows(rows, 2, 2, 1);
        LineFileProcessingResult.FinalIndexData finalData = result.getFinalIndexData();

        List<SelectedGroup> groups = finalData.getHighFreqMutexGroupPostings();
        List<LineFileProcessingResult.TermsPostingIndexRef> groupIndexRefs =
                finalData.getHighFreqMutexGroupTermsToIndex();
        assertEquals(groups.size(), groupIndexRefs.size());
        for (int i = 0; i < groupIndexRefs.size(); i++) {
            assertEquals(i, groupIndexRefs.get(i).getPostingIndex());
            assertEquals(termsFingerprint(groups.get(i).getTerms()),
                    termsFingerprint(groupIndexRefs.get(i).getTerms()));
        }

        List<LineFileProcessingResult.HotTermDocList> hotTerms = finalData.getHighFreqSingleTermPostings();
        List<LineFileProcessingResult.TermsPostingIndexRef> hotTermIndexRefs =
                finalData.getHighFreqSingleTermToIndex();
        assertEquals(hotTerms.size(), hotTermIndexRefs.size());
        for (int i = 0; i < hotTermIndexRefs.size(); i++) {
            assertEquals(i, hotTermIndexRefs.get(i).getPostingIndex());
            assertEquals(1, hotTermIndexRefs.get(i).getTerms().size());
            assertEquals(ByteArrayUtils.toHex(hotTerms.get(i).getTerm()),
                    ByteArrayUtils.toHex(hotTermIndexRefs.get(i).getTerms().get(0)));
        }

        List<String> expectedLowHit = buildLowHitExpandedRefs(finalData.getLowHitForwardRows());
        List<String> actualLowHit = toLowHitExpandedRefs(finalData.getLowHitTermToIndexes());
        Collections.sort(expectedLowHit);
        Collections.sort(actualLowHit);
        assertEquals(expectedLowHit, actualLowHit);

        assertTrue(finalData.getHighFreqMutexGroupSkipBitsetIndex().getHashLevels().size() >= 1);
        assertTrue(finalData.getHighFreqSingleTermSkipBitsetIndex().getHashLevels().size() >= 1);
        assertTrue(finalData.getLowHitTermSkipBitsetIndex().getHashLevels().size() >= 1);

        LineFileProcessingResult.TermBlockSkipBitsetIndex lowHitBitsets =
                finalData.getLowHitTermSkipBitsetIndex();
        assertEquals(2, lowHitBitsets.getMaxPostingIndex());
        LineFileProcessingResult.HashLevelBitsets l2 = findLevel(lowHitBitsets, 2);
        int bucket = hashWindowToBucket(bytes("LX"), 0, 2);
        BitSet bucketBits = l2.getBuckets().get(bucket);
        assertTrue(bucketBits.get(0));
    }

    @Test
    void processRows_shouldAllowDynamicSkipHashGramRange() {
        List<DocTerms> rows = new ArrayList<DocTerms>();
        rows.add(new DocTerms(10, Arrays.asList(bytes("ABCD"), bytes("BCDE"))));
        rows.add(new DocTerms(20, Arrays.asList(bytes("ABCD"), bytes("CDEF"))));

        LineFileProcessingResult result = ExclusiveFpRowsProcessingApi.processRows(
                rows, 1, 2, 1, 3, 3);
        LineFileProcessingResult.TermBlockSkipBitsetIndex lowHitIndex =
                result.getFinalIndexData().getLowHitTermSkipBitsetIndex();

        assertEquals(3, result.getFinalIndexData().getSkipHashMinGram());
        assertEquals(3, result.getFinalIndexData().getSkipHashMaxGram());
        assertEquals(1, lowHitIndex.getHashLevels().size());
        assertEquals(3, lowHitIndex.getHashLevels().get(0).getGramLength());
    }

    @Test
    void processRows_shouldRejectInvalidSkipHashGramRange() {
        List<DocTerms> rows = new ArrayList<DocTerms>();
        rows.add(new DocTerms(1, Arrays.asList(bytes("ABCD"))));

        assertThrows(IllegalArgumentException.class, () ->
                ExclusiveFpRowsProcessingApi.processRows(rows, 1, 2, 1, 1, 4));
        assertThrows(IllegalArgumentException.class, () ->
                ExclusiveFpRowsProcessingApi.processRows(rows, 1, 2, 1, 4, 3));
    }

    @Test
    void processRowsWithNgram_shouldTokenizeSingleRawTermRowsInsideApi() {
        List<DocTerms> rawRows = new ArrayList<DocTerms>();
        rawRows.add(new DocTerms(0, Arrays.asList(bytes("ABCD"))));
        rawRows.add(new DocTerms(1, Arrays.asList(bytes("ABEF"))));

        LineFileProcessingResult result = ExclusiveFpRowsProcessingApi.processRowsWithNgram(
                rawRows, 2, 2, 100, 2, 0);
        Map<String, List<Integer>> hotMap = toHotMap(result.getFinalIndexData().getHighFreqSingleTermPostings());

        assertTrue(hotMap.containsKey(hex("AB")));
        assertFalse(hotMap.containsKey(hex("ABCD"))); // 原始整行不应作为 term 参与统计
    }

    @Test
    void processRows_shouldBuildOneByteDocidBitsetIndex_fromRawLoadedRows() {
        List<DocTerms> rawRows = new ArrayList<DocTerms>();
        rawRows.add(new DocTerms(0, Arrays.asList(bytes("ABCD"))));
        rawRows.add(new DocTerms(2, Arrays.asList(bytes("BC"))));
        rawRows.add(new DocTerms(5, Arrays.asList(bytes("A"))));

        LineFileProcessingResult result = ExclusiveFpRowsProcessingApi.processRowsWithNgram(
                rawRows, 2, 4, 100, 2, 0);
        LineFileProcessingResult.OneByteDocidBitsetIndex oneByteIndex =
                result.getFinalIndexData().getOneByteDocidBitsetIndex();

        assertEquals(5, oneByteIndex.getMaxDocId());
        assertTrue(oneByteIndex.getDocIdBitset('A').get(0));
        assertTrue(oneByteIndex.getDocIdBitset('A').get(5));
        assertFalse(oneByteIndex.getDocIdBitset('A').get(2));

        assertTrue(oneByteIndex.getDocIdBitset('C').get(0));
        assertTrue(oneByteIndex.getDocIdBitset('C').get(2));
        assertFalse(oneByteIndex.getDocIdBitset('C').get(5));

        // 顶层便捷访问器应返回一致结构
        assertEquals(oneByteIndex.getMaxDocId(), result.getOneByteDocidBitsetIndex().getMaxDocId());
    }

    private static Map<String, List<Integer>> toHotMap(
            List<LineFileProcessingResult.HotTermDocList> hotTerms
    ) {
        Map<String, List<Integer>> out = new LinkedHashMap<String, List<Integer>>();
        for (LineFileProcessingResult.HotTermDocList hotTerm : hotTerms) {
            out.put(ByteArrayUtils.toHex(hotTerm.getTerm()), hotTerm.getDocIds());
        }
        return out;
    }

    private static String groupsFingerprint(List<SelectedGroup> groups) {
        StringBuilder sb = new StringBuilder();
        for (SelectedGroup group : groups) {
            sb.append('{');
            for (byte[] term : group.getTerms()) {
                sb.append(ByteArrayUtils.toHex(term)).append(',');
            }
            sb.append('|');
            sb.append(group.getSupport()).append('|');
            sb.append(group.getDocIds().size()).append('}');
        }
        return sb.toString();
    }

    private static String hotTermsFingerprint(List<LineFileProcessingResult.HotTermDocList> hotTerms) {
        StringBuilder sb = new StringBuilder();
        for (LineFileProcessingResult.HotTermDocList hotTerm : hotTerms) {
            sb.append(ByteArrayUtils.toHex(hotTerm.getTerm())).append(':');
            for (Integer docId : hotTerm.getDocIds()) {
                sb.append(docId).append(',');
            }
            sb.append(';');
        }
        return sb.toString();
    }

    private static String rowsFingerprint(List<DocTerms> rows) {
        StringBuilder sb = new StringBuilder();
        for (DocTerms row : rows) {
            sb.append(row.getDocId()).append(':');
            for (byte[] term : row.getTermsUnsafe()) {
                sb.append(ByteArrayUtils.toHex(term)).append(',');
            }
            sb.append(';');
        }
        return sb.toString();
    }

    private static String termsFingerprint(List<byte[]> terms) {
        StringBuilder sb = new StringBuilder();
        for (byte[] term : terms) {
            sb.append(ByteArrayUtils.toHex(term)).append(',');
        }
        return sb.toString();
    }

    private static List<String> buildLowHitExpandedRefs(List<DocTerms> rows) {
        List<String> out = new ArrayList<String>();
        for (int rowIndex = 0; rowIndex < rows.size(); rowIndex++) {
            int docId = rows.get(rowIndex).getDocId();
            for (byte[] term : rows.get(rowIndex).getTermsUnsafe()) {
                out.add(ByteArrayUtils.toHex(term) + "@" + docId);
            }
        }
        return out;
    }

    private static List<String> toLowHitExpandedRefs(
            List<LineFileProcessingResult.TermsPostingIndexRef> refs
    ) {
        List<String> out = new ArrayList<String>();
        for (LineFileProcessingResult.TermsPostingIndexRef ref : refs) {
            assertEquals(1, ref.getTerms().size());
            out.add(ByteArrayUtils.toHex(ref.getTerms().get(0)) + "@" + ref.getPostingIndex());
        }
        return out;
    }

    private static LineFileProcessingResult.HashLevelBitsets findLevel(
            LineFileProcessingResult.TermBlockSkipBitsetIndex index,
            int gramLength
    ) {
        for (LineFileProcessingResult.HashLevelBitsets level : index.getHashLevels()) {
            if (level.getGramLength() == gramLength) {
                return level;
            }
        }
        throw new AssertionError("missing gram level: " + gramLength);
    }

    private static int hashWindowToBucket(byte[] arr, int start, int len) {
        int h = 1;
        for (int i = 0; i < len; i++) {
            h = 31 * h + (arr[start + i] & 0xFF);
        }
        return h & 0xFF;
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static String hex(String value) {
        return ByteArrayUtils.toHex(bytes(value));
    }

    private static List<DocTerms> buildClusteredRowsForMutexAbsorption() {
        List<DocTerms> rows = new ArrayList<DocTerms>();
        rows.add(new DocTerms(0, Arrays.asList(bytes("A"), bytes("B"), bytes("C"), bytes("X1"))));
        rows.add(new DocTerms(1, Arrays.asList(bytes("A"), bytes("B"), bytes("C"), bytes("X2"))));
        rows.add(new DocTerms(2, Arrays.asList(bytes("A"), bytes("B"), bytes("D"), bytes("X3"))));
        rows.add(new DocTerms(3, Arrays.asList(bytes("A"), bytes("B"), bytes("D"), bytes("X4"))));
        rows.add(new DocTerms(4, Arrays.asList(bytes("M"), bytes("N"), bytes("P"), bytes("Y1"))));
        rows.add(new DocTerms(5, Arrays.asList(bytes("M"), bytes("N"), bytes("P"), bytes("Y2"))));
        rows.add(new DocTerms(6, Arrays.asList(bytes("M"), bytes("N"), bytes("Q"), bytes("Y3"))));
        rows.add(new DocTerms(7, Arrays.asList(bytes("M"), bytes("N"), bytes("Q"), bytes("Y4"))));
        rows.add(new DocTerms(8, Arrays.asList(bytes("A"), bytes("B"), bytes("M"), bytes("N"), bytes("Z1"))));
        rows.add(new DocTerms(9, Arrays.asList(bytes("A"), bytes("B"), bytes("M"), bytes("N"), bytes("Z2"))));
        return rows;
    }
}
