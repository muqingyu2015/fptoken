/**
 * Developer: Profile where time is spent in the pipeline.
 */
import cn.lxdb.plugins.muqingyu.fptoken.*;
import cn.lxdb.plugins.muqingyu.fptoken.config.SelectorConfig;
import cn.lxdb.plugins.muqingyu.fptoken.config.EngineTuningConfig;
import cn.lxdb.plugins.muqingyu.fptoken.index.TermTidsetIndex;
import cn.lxdb.plugins.muqingyu.fptoken.miner.BeamFrequentItemsetMiner;
import cn.lxdb.plugins.muqingyu.fptoken.model.*;
import cn.lxdb.plugins.muqingyu.fptoken.picker.TwoPhaseExclusiveItemsetPicker;
import java.util.*;

public class BottleneckProfiler {

    public static void main(String[] args) {
        System.out.println("=== Pipeline Bottleneck Profiler - Developer ===\n");

        int docCount = 50000;
        int avgTerms = 30;
        System.out.println("Profiling: " + docCount + " docs, ~" + avgTerms + " terms/doc\n");

        List<DocTerms> docs = generateDocTerms(docCount, avgTerms - 5, avgTerms + 5);

        SelectorConfig config = SelectorConfig.builder()
            .minimumRequiredSupport(docCount / 20)
            .minimumPatternLength(2)
            .maximumPatternLength(4)
            .maximumIntermediateResults(500)
            .build();

        // Warmup
        ExclusiveFrequentItemsetSelector.setSamplingEnabled(false);
        try {
            ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsets(docs, config.getMinSupport(), config.getMinItemsetSize());
        } catch (Exception e) {}

        // Phase 1: Index build
        System.gc();
        long t0 = System.nanoTime();
        TermTidsetIndex termIndex = TermTidsetIndex.buildWithSupportBounds(
            docs, config.getMinSupport(), EngineTuningConfig.DEFAULT_MAX_DOC_COVERAGE_RATIO);
        long indexTime = System.nanoTime() - t0;
        System.out.println(String.format("Phase 1 - Index build: %6.1f ms", indexTime / 1_000_000.0));

        List<byte[]> vocab = termIndex.getIdToTermUnsafe();
        System.out.println("  Vocabulary: " + vocab.size() + " terms");
        System.out.println("  Tidsets: " + termIndex.getTidsetsByTermIdUnsafe().size());

        // Phase 2: Mining
        System.gc();
        t0 = System.nanoTime();
        BeamFrequentItemsetMiner miner = new BeamFrequentItemsetMiner();
        FrequentItemsetMiningResult miningResult = miner.mineWithStats(
            termIndex.getTidsetsByTermIdUnsafe(),
            config,
            EngineTuningConfig.DEFAULT_MAX_FREQUENT_TERM_COUNT,
            EngineTuningConfig.DEFAULT_MAX_BRANCHING_FACTOR,
            EngineTuningConfig.FACADE_DEFAULT_BEAM_WIDTH);
        long mineTime = System.nanoTime() - t0;
        System.out.println(String.format("Phase 2 - Mining:       %6.1f ms", mineTime / 1_000_000.0));

        List<CandidateItemset> candidates = miningResult.getCandidates();
        System.out.println("  Candidates: " + candidates.size());
        System.out.println("  Intersections: " + miningResult.getIntersectionCount());

        // Phase 3: Picking
        System.gc();
        t0 = System.nanoTime();
        TwoPhaseExclusiveItemsetPicker picker = new TwoPhaseExclusiveItemsetPicker();
        List<CandidateItemset> selected = picker.pick(
            candidates, vocab.size(), 100, 0);
        long pickTime = System.nanoTime() - t0;
        System.out.println(String.format("Phase 3 - Picking:      %6.1f ms", pickTime / 1_000_000.0));
        System.out.println("  Selected: " + selected.size());

        // Phase 4: To output
        System.gc();
        t0 = System.nanoTime();
        // Simulate toSelectedGroups
        List<SelectedGroup> groups = new ArrayList<>();
        for (CandidateItemset c : selected) {
            int[] termIds = c.getTermIdsUnsafe();
            List<byte[]> terms = new ArrayList<>(termIds.length);
            for (int tid : termIds) terms.add(vocab.get(tid));
            groups.add(new SelectedGroup(terms, toDocIds(c.getDocBitsUnsafe()), c.getSupport(), c.getEstimatedSaving()));
        }
        long outputTime = System.nanoTime() - t0;
        System.out.println(String.format("Phase 4 - To output:    %6.1f ms", outputTime / 1_000_000.0));

        long total = indexTime + mineTime + pickTime + outputTime;
        System.out.println("\n--- Summary ---");
        System.out.println(String.format("Index:  %5.1f%%", 100.0 * indexTime / total));
        System.out.println(String.format("Mining: %5.1f%%", 100.0 * mineTime / total));
        System.out.println(String.format("Pick:   %5.1f%%", 100.0 * pickTime / total));
        System.out.println(String.format("Output: %5.1f%%", 100.0 * outputTime / total));
        System.out.println(String.format("Total:  %6.1f ms", total / 1_000_000.0));
    }

    private static List<Integer> toDocIds(BitSet bits) {
        List<Integer> out = new ArrayList<>(bits.cardinality());
        for (int i = bits.nextSetBit(0); i >= 0; i = bits.nextSetBit(i + 1)) out.add(i);
        return out;
    }

    private static List<DocTerms> generateDocTerms(int count, int minTerms, int maxTerms) {
        Random rnd = new Random(42);
        List<DocTerms> docs = new ArrayList<>(count);
        byte[][] highFreq = {
            "GET".getBytes(), "HTTP".getBytes(), "200".getBytes(), "Host".getBytes(),
            "User-Agent".getBytes(), "Content-Type".getBytes(), "Accept".getBytes(),
            "Connection".getBytes(), "POST".getBytes(), "404".getBytes()
        };
        byte[][] midFreq = new byte[30][];
        for (int i = 0; i < 30; i++) midFreq[i] = ("MID_" + i).getBytes();
        for (int i = 0; i < count; i++) {
            int termCount = minTerms + rnd.nextInt(maxTerms - minTerms + 1);
            List<byte[]> terms = new ArrayList<>(termCount);
            for (byte[] p : highFreq) { if (rnd.nextDouble() < 0.30) terms.add(p); }
            for (byte[] p : midFreq) { if (rnd.nextDouble() < 0.10) terms.add(p); }
            for (int j = 0; j < termCount - terms.size(); j++) {
                byte[] t = new byte[2 + rnd.nextInt(4)];
                rnd.nextBytes(t); terms.add(t);
            }
            docs.add(new DocTerms(i, terms));
        }
        return docs;
    }
}
