package cn.lxdb.plugins.muqingyu.fptoken.tests.performance;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import cn.lxdb.plugins.muqingyu.fptoken.ExclusiveFrequentItemsetSelector;
import cn.lxdb.plugins.muqingyu.fptoken.api.ExclusiveFpRowsProcessingApi;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.config.EngineTuningConfig;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.config.SelectorConfig;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.index.TermTidsetIndex;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.miner.BeamFrequentItemsetMiner;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.CandidateItemset;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.DocTerms;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.ExclusiveSelectionResult;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.FrequentItemsetMiningResult;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.SelectedGroup;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.picker.TwoPhaseExclusiveItemsetPicker;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.util.ByteArrayUtils;
import cn.lxdb.plugins.muqingyu.fptoken.runner.result.LineFileProcessingResult;
import cn.lxdb.plugins.muqingyu.fptoken.tests.ByteArrayTestSupport;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

/**
 * 按“类职责 + 整体集成流程”做性能打点，并在测试里直接给出瓶颈提示。
 */
@Tag("performance")
@EnabledIfSystemProperty(named = "fptoken.runPerfTests", matches = "true")
@Timeout(value = 30, unit = TimeUnit.SECONDS)
class ResponsibilityStepProfilingPerformanceTest {

    private static final int CLASS_STAGE_INDEX = 0;
    private static final int CLASS_STAGE_MINE = 1;
    private static final int CLASS_STAGE_PICK = 2;
    private static final String[] CLASS_STAGE_NAMES = {
            "TermTidsetIndex",
            "BeamFrequentItemsetMiner",
            "TwoPhaseExclusiveItemsetPicker"
    };

    private static final int PIPE_STAGE_TOKENIZE = 0;
    private static final int PIPE_STAGE_SELECTOR = 1;
    private static final int PIPE_STAGE_DERIVED = 2;
    private static final int PIPE_STAGE_FINAL_INDEX = 3;
    private static final int PIPE_STAGE_END_TO_END_API = 4;
    private static final String[] PIPE_STAGE_NAMES = {
            "TokenizeRowsForMining",
            "Selector(selectExclusiveBestItemsetsWithStats)",
            "BuildDerivedData",
            "BuildFinalIndexData",
            "EndToEndApi(processRows)"
    };

    @Test
    void classResponsibilities_shouldKeepFunctionalInvariants_andPrintTiming() {
        List<DocTerms> rows = PerfTestSupport.protocolMixRows(12000, 20260423L);
        int minSupport = 8;
        int minItemsetSize = 2;

        long[] stageMs = new long[CLASS_STAGE_NAMES.length];
        TermTidsetIndex.BuildWithSupportBoundsTimings indexTiming =
                new TermTidsetIndex.BuildWithSupportBoundsTimings();

        final TermTidsetIndex[] indexHolder = new TermTidsetIndex[1];
        stageMs[CLASS_STAGE_INDEX] = PerfTestSupport.elapsedMillis(() ->
                indexHolder[0] = TermTidsetIndex.buildWithSupportBounds(
                        rows,
                        minSupport,
                        EngineTuningConfig.DEFAULT_MAX_DOC_COVERAGE_RATIO,
                        indexTiming));
        TermTidsetIndex index = indexHolder[0];
        assertTrue(!index.getIdToTermUnsafe().isEmpty());
        assertEquals(index.getIdToTermUnsafe().size(), index.getTidsetsByTermIdUnsafe().size());

        BeamFrequentItemsetMiner miner = new BeamFrequentItemsetMiner();
        SelectorConfig config = new SelectorConfig(
                minSupport,
                minItemsetSize,
                EngineTuningConfig.DEFAULT_MAX_ITEMSET_SIZE,
                EngineTuningConfig.DEFAULT_MAX_CANDIDATE_COUNT
        );
        final FrequentItemsetMiningResult[] miningHolder = new FrequentItemsetMiningResult[1];
        stageMs[CLASS_STAGE_MINE] = PerfTestSupport.elapsedMillis(() ->
                miningHolder[0] = miner.mineWithStats(
                        index.getTidsetsByTermIdUnsafe(),
                        config,
                        EngineTuningConfig.DEFAULT_MAX_FREQUENT_TERM_COUNT,
                        EngineTuningConfig.DEFAULT_MAX_BRANCHING_FACTOR,
                        EngineTuningConfig.FACADE_DEFAULT_BEAM_WIDTH,
                        2,
                        3000L));
        FrequentItemsetMiningResult mining = miningHolder[0];
        assertTrue(mining.getFrequentTermCount() >= 0);
        assertTrue(mining.getGeneratedCandidateCount() >= 0);

        TwoPhaseExclusiveItemsetPicker picker = new TwoPhaseExclusiveItemsetPicker();
        int dictionarySize = Math.max(1, index.getIdToTermUnsafe().size());
        final List<CandidateItemset>[] selectedCandidateHolder = new List[1];
        stageMs[CLASS_STAGE_PICK] = PerfTestSupport.elapsedMillis(() ->
                selectedCandidateHolder[0] = picker.pick(
                        mining.getCandidates(),
                        dictionarySize,
                        EngineTuningConfig.DEFAULT_MAX_SWAP_TRIALS,
                        EngineTuningConfig.DEFAULT_MIN_NET_GAIN));
        List<CandidateItemset> selected = selectedCandidateHolder[0];
        List<SelectedGroup> groups = toSelectedGroups(selected, index.getIdToTermUnsafe());
        assertTrue(ByteArrayTestSupport.pairwiseTermsDisjoint(groups));

        printClassBreakdown(stageMs, indexTiming);
        int hottest = hottestStage(stageMs);
        System.out.println("[responsibility-perf] hottestClass=" + CLASS_STAGE_NAMES[hottest]
                + ", avgMs=" + stageMs[hottest]
                + ", hint=" + optimizationHintForClassStage(hottest));
    }

    @Test
    void integrationPipeline_shouldPrintStepCost_andKeepFunctionalConsistency() {
        List<DocTerms> rawRows = buildRawRows(6000, 48);
        int ngramStart = 2;
        int ngramEnd = 4;
        int minSupport = 8;
        int minItemsetSize = 2;
        int hotTermThresholdExclusive = EngineTuningConfig.DEFAULT_HOT_TERM_THRESHOLD_EXCLUSIVE;

        long[] stageMs = new long[PIPE_STAGE_NAMES.length];

        final List<DocTerms>[] tokenizedHolder = new List[1];
        stageMs[PIPE_STAGE_TOKENIZE] = PerfTestSupport.elapsedMillis(() ->
                tokenizedHolder[0] = ExclusiveFpRowsProcessingApi.IntermediateSteps.tokenizeRowsForMining(
                        rawRows, ngramStart, ngramEnd));
        List<DocTerms> tokenizedRows = tokenizedHolder[0];
        assertEquals(rawRows.size(), tokenizedRows.size());
        assertTrue(!tokenizedRows.isEmpty());
        assertTrue(!tokenizedRows.get(0).getTermsUnsafe().isEmpty());

        final ExclusiveSelectionResult[] selectionHolder = new ExclusiveSelectionResult[1];
        stageMs[PIPE_STAGE_SELECTOR] = PerfTestSupport.elapsedMillis(() ->
                selectionHolder[0] = ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(
                        tokenizedRows,
                        minSupport,
                        minItemsetSize,
                        EngineTuningConfig.DEFAULT_MAX_ITEMSET_SIZE,
                        EngineTuningConfig.DEFAULT_MAX_CANDIDATE_COUNT));
        ExclusiveSelectionResult selection = selectionHolder[0];
        assertTrue(ByteArrayTestSupport.allGroupsTermCountInRange(
                selection.getGroups(),
                minItemsetSize,
                EngineTuningConfig.DEFAULT_MAX_ITEMSET_SIZE));
        assertTrue(ByteArrayTestSupport.pairwiseTermsDisjoint(selection.getGroups()));

        final LineFileProcessingResult.DerivedData[] derivedHolder =
                new LineFileProcessingResult.DerivedData[1];
        stageMs[PIPE_STAGE_DERIVED] = PerfTestSupport.elapsedMillis(() ->
                derivedHolder[0] = ExclusiveFpRowsProcessingApi.buildDerivedData(
                        tokenizedRows, selection, hotTermThresholdExclusive));
        LineFileProcessingResult.DerivedData derivedData = derivedHolder[0];
        assertTrue(derivedData.getCutRes().size() <= tokenizedRows.size());

        final LineFileProcessingResult[] manualPipelineHolder = new LineFileProcessingResult[1];
        stageMs[PIPE_STAGE_FINAL_INDEX] = PerfTestSupport.elapsedMillis(() ->
                manualPipelineHolder[0] = new LineFileProcessingResult(
                        rawRows,
                        selection,
                        derivedData,
                        EngineTuningConfig.DEFAULT_SKIP_HASH_MIN_GRAM,
                        EngineTuningConfig.DEFAULT_SKIP_HASH_MAX_GRAM));
        LineFileProcessingResult manual = manualPipelineHolder[0];
        assertTrue(manual.getFinalIndexData().getHighFreqMutexGroupPostings().size() >= 0);

        final LineFileProcessingResult[] apiPipelineHolder = new LineFileProcessingResult[1];
        stageMs[PIPE_STAGE_END_TO_END_API] = PerfTestSupport.elapsedMillis(() ->
                apiPipelineHolder[0] = ExclusiveFpRowsProcessingApi.processRowsWithNgramAndSkipHash(
                        rawRows,
                        ngramStart,
                        ngramEnd,
                        minSupport,
                        minItemsetSize,
                        hotTermThresholdExclusive,
                        EngineTuningConfig.DEFAULT_SKIP_HASH_MIN_GRAM,
                        EngineTuningConfig.DEFAULT_SKIP_HASH_MAX_GRAM));
        LineFileProcessingResult api = apiPipelineHolder[0];

        assertEquals(
                ByteArrayTestSupport.groupsFingerprint(manual.getSelectionResult().getGroups()),
                ByteArrayTestSupport.groupsFingerprint(api.getSelectionResult().getGroups())
        );
        assertEquals(
                manual.getFinalIndexData().getHighFreqMutexGroupPostings().size(),
                api.getFinalIndexData().getHighFreqMutexGroupPostings().size()
        );

        printPipelineBreakdown(stageMs);
        int hottest = hottestStage(stageMs);
        System.out.println("[responsibility-perf] hottestPipelineStage=" + PIPE_STAGE_NAMES[hottest]
                + ", avgMs=" + stageMs[hottest]
                + ", hint=" + optimizationHintForPipelineStage(hottest));
    }

    private static void printClassBreakdown(long[] stageMs, TermTidsetIndex.BuildWithSupportBoundsTimings indexTiming) {
        long total = Math.max(1L, stageMs[CLASS_STAGE_INDEX] + stageMs[CLASS_STAGE_MINE] + stageMs[CLASS_STAGE_PICK]);
        StringBuilder sb = new StringBuilder();
        sb.append("[responsibility-perf:class]");
        for (int i = 0; i < CLASS_STAGE_NAMES.length; i++) {
            long ms = stageMs[i];
            long share = (ms * 100L) / total;
            sb.append(" | ").append(CLASS_STAGE_NAMES[i]).append("=").append(ms).append("ms")
                    .append(" (").append(share).append("%)");
        }
        sb.append(" | total=").append(total).append("ms");
        sb.append(" | indexSub[first=").append(indexTiming.getFirstPassMillis()).append("ms")
                .append(", filter=").append(indexTiming.getFilterPrepareMillis()).append("ms")
                .append(", second=").append(indexTiming.getSecondPassMillis()).append("ms")
                .append("]");
        System.out.println(sb.toString());
    }

    private static void printPipelineBreakdown(long[] stageMs) {
        long total = 0L;
        for (long ms : stageMs) {
            total += ms;
        }
        long safeTotal = Math.max(1L, total);
        StringBuilder sb = new StringBuilder();
        sb.append("[responsibility-perf:pipeline]");
        for (int i = 0; i < PIPE_STAGE_NAMES.length; i++) {
            long ms = stageMs[i];
            long share = (ms * 100L) / safeTotal;
            sb.append(" | ").append(PIPE_STAGE_NAMES[i]).append("=").append(ms).append("ms")
                    .append(" (").append(share).append("%)");
        }
        sb.append(" | total=").append(safeTotal).append("ms");
        System.out.println(sb.toString());
    }

    private static int hottestStage(long[] stageMs) {
        int hottest = 0;
        for (int i = 1; i < stageMs.length; i++) {
            if (stageMs[i] > stageMs[hottest]) {
                hottest = i;
            }
        }
        return hottest;
    }

    private static String optimizationHintForClassStage(int stage) {
        if (stage == CLASS_STAGE_INDEX) {
            return "热点在索引构建，可继续优化 first/second pass：减少重复 hash 与 set 位图次数";
        }
        if (stage == CLASS_STAGE_MINE) {
            return "热点在挖掘，可先收窄 beamWidth/maxBranchingFactor，再看候选质量损失";
        }
        return "热点在互斥挑选，可按候选规模自适应降低 maxSwapTrials";
    }

    private static String optimizationHintForPipelineStage(int stage) {
        if (stage == PIPE_STAGE_TOKENIZE) {
            return "分词最慢时可预切词缓存或缩短 ngram 区间上界";
        }
        if (stage == PIPE_STAGE_SELECTOR) {
            return "选择器最慢时优先优化索引构建与候选裁剪参数";
        }
        if (stage == PIPE_STAGE_DERIVED) {
            return "派生阶段最慢时可减少重复 term 哈希查找，复用 key 映射";
        }
        if (stage == PIPE_STAGE_FINAL_INDEX) {
            return "FinalIndex 构建最慢时可按低命中层先做早过滤，减少 skip-bitset 建立量";
        }
        return "端到端最慢时优先从占比最高子步骤切入优化，而非整体盲调";
    }

    private static List<SelectedGroup> toSelectedGroups(List<CandidateItemset> selected, List<byte[]> idToTerm) {
        if (selected == null || selected.isEmpty()) {
            return Collections.emptyList();
        }
        List<SelectedGroup> out = new ArrayList<>(selected.size());
        for (CandidateItemset candidate : selected) {
            int[] termIds = candidate.getTermIdsUnsafe();
            List<byte[]> terms = new ArrayList<>(termIds.length);
            for (int i = 0; i < termIds.length; i++) {
                terms.add(ByteArrayUtils.copy(idToTerm.get(termIds[i])));
            }
            out.add(new SelectedGroup(
                    terms,
                    bitSetToDocIds(candidate.getDocBitsUnsafe()),
                    candidate.getSupport(),
                    candidate.getEstimatedSaving()
            ));
        }
        return out;
    }

    private static List<Integer> bitSetToDocIds(BitSet bits) {
        if (bits == null || bits.isEmpty()) {
            return Collections.emptyList();
        }
        List<Integer> out = new ArrayList<>(bits.cardinality());
        for (int i = bits.nextSetBit(0); i >= 0; i = bits.nextSetBit(i + 1)) {
            out.add(i);
        }
        return out;
    }

    private static List<DocTerms> buildRawRows(int docs, int bytesPerDoc) {
        List<DocTerms> out = new ArrayList<>(docs);
        for (int i = 0; i < docs; i++) {
            out.add(ByteArrayTestSupport.doc(i, ByteArrayTestSupport.pseudoRecord(1000 + i, bytesPerDoc)));
        }
        return out;
    }
}
