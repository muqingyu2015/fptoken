package cn.lxdb.plugins.muqingyu.fptoken.tests.performance;

import cn.lxdb.plugins.muqingyu.fptoken.config.SelectorConfig;
import cn.lxdb.plugins.muqingyu.fptoken.index.TermTidsetIndex;
import cn.lxdb.plugins.muqingyu.fptoken.miner.BeamFrequentItemsetMiner;
import cn.lxdb.plugins.muqingyu.fptoken.model.CandidateItemset;
import cn.lxdb.plugins.muqingyu.fptoken.model.DocTerms;
import cn.lxdb.plugins.muqingyu.fptoken.model.FrequentItemsetMiningResult;
import cn.lxdb.plugins.muqingyu.fptoken.picker.TwoPhaseExclusiveItemsetPicker;
import java.util.List;

final class CompressibilityPipelineSupport {
    private CompressibilityPipelineSupport() {
    }

    static PipelineResult run(
            List<DocTerms> rows,
            int minSupport,
            int minItemsetSize,
            int maxItemsetSize,
            int maxCandidateCount,
            int maxFrequentTermCount,
            int maxBranchingFactor,
            int beamWidth,
            int maxSwapTrials
    ) {
        long startNanos = System.nanoTime();
        TermTidsetIndex index = TermTidsetIndex.build(rows);
        SelectorConfig config = new SelectorConfig(minSupport, minItemsetSize, maxItemsetSize, maxCandidateCount);
        BeamFrequentItemsetMiner miner = new BeamFrequentItemsetMiner();
        FrequentItemsetMiningResult mining = miner.mineWithStats(
                index.getTidsetsByTermIdUnsafe(),
                config,
                maxFrequentTermCount,
                maxBranchingFactor,
                beamWidth
        );
        TwoPhaseExclusiveItemsetPicker picker = new TwoPhaseExclusiveItemsetPicker();
        List<CandidateItemset> selected = picker.pick(
                mining.getCandidates(),
                index.getIdToTermUnsafe().size(),
                maxSwapTrials
        );
        CompressionMetricsUtil.CompressionMetrics metrics =
                CompressionMetricsUtil.calculateCoverageAndPotentialSavings(selected, index, rows);
        long elapsedMs = (System.nanoTime() - startNanos) / 1_000_000L;
        return new PipelineResult(elapsedMs, index, mining, selected, metrics);
    }

    static final class PipelineResult {
        private final long elapsedMs;
        private final TermTidsetIndex index;
        private final FrequentItemsetMiningResult mining;
        private final List<CandidateItemset> selectedCandidates;
        private final CompressionMetricsUtil.CompressionMetrics metrics;

        private PipelineResult(
                long elapsedMs,
                TermTidsetIndex index,
                FrequentItemsetMiningResult mining,
                List<CandidateItemset> selectedCandidates,
                CompressionMetricsUtil.CompressionMetrics metrics
        ) {
            this.elapsedMs = elapsedMs;
            this.index = index;
            this.mining = mining;
            this.selectedCandidates = selectedCandidates;
            this.metrics = metrics;
        }

        long getElapsedMs() {
            return elapsedMs;
        }

        TermTidsetIndex getIndex() {
            return index;
        }

        FrequentItemsetMiningResult getMining() {
            return mining;
        }

        List<CandidateItemset> getSelectedCandidates() {
            return selectedCandidates;
        }

        CompressionMetricsUtil.CompressionMetrics getMetrics() {
            return metrics;
        }
    }
}
