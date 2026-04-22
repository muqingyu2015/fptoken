/**
 * Developer: Measure index build time vs mining time breakdown.
 * Uses larger vocabulary to make mining non-trivial.
 */
import cn.lxdb.plugins.muqingyu.fptoken.*;
import cn.lxdb.plugins.muqingyu.fptoken.config.SelectorConfig;
import cn.lxdb.plugins.muqingyu.fptoken.config.EngineTuningConfig;
import cn.lxdb.plugins.muqingyu.fptoken.index.TermTidsetIndex;
import cn.lxdb.plugins.muqingyu.fptoken.miner.BeamFrequentItemsetMiner;
import cn.lxdb.plugins.muqingyu.fptoken.model.*;
import java.util.*;

public class IndexTimeTest {

    public static void main(String[] args) {
        System.out.println("=== Index vs Mining Time Breakdown - Developer ===\n");

        // Use realistic parameters: 100K docs, 50 terms/doc, 10K vocab
        int docCount = 100000;
        int avgTerms = 50;

        System.out.println("Generating " + docCount + " docs with ~" + avgTerms + " terms/doc...");
        long t0 = System.nanoTime();
        List<DocTerms> docs = generateDocTerms(docCount, avgTerms - 10, avgTerms + 10, 10000);
        System.out.println("  Data gen: " + (System.nanoTime() - t0) / 1_000_000 + " ms\n");

        SelectorConfig config = SelectorConfig.builder()
            .minimumRequiredSupport(docCount / 20)
            .minimumPatternLength(2)
            .maximumPatternLength(4)
            .maximumIntermediateResults(500)
            .build();

        // Measure index build time
        System.gc();
        t0 = System.nanoTime();
        TermTidsetIndex fullIndex = TermTidsetIndex.buildWithSupportBounds(
            docs, config.getMinSupport(), EngineTuningConfig.DEFAULT_MAX_DOC_COVERAGE_RATIO);
        long indexTime = System.nanoTime() - t0;

        List<byte[]> vocab = fullIndex.getIdToTermUnsafe();
        List<BitSet> tidsets = fullIndex.getTidsetsByTermIdUnsafe();
        System.out.println("Index build: " + (indexTime / 1_000_000) + " ms");
        System.out.println("  Vocabulary: " + vocab.size() + " terms");
        System.out.println("  Total tidsets bit count: " + totalBits(tidsets) / 8 / 1024 + " KB\n");

        // Measure mining time
        System.gc();
        t0 = System.nanoTime();
        BeamFrequentItemsetMiner miner = new BeamFrequentItemsetMiner();
        FrequentItemsetMiningResult result = miner.mineWithStats(
            tidsets, config,
            EngineTuningConfig.DEFAULT_MAX_FREQUENT_TERM_COUNT,
            EngineTuningConfig.DEFAULT_MAX_BRANCHING_FACTOR,
            EngineTuningConfig.FACADE_DEFAULT_BEAM_WIDTH);
        long mineTime = System.nanoTime() - t0;

        System.out.println("Mining: " + (mineTime / 1_000_000) + " ms");
        System.out.println("  Candidates: " + result.getCandidates().size());
        System.out.println("  Intersections: " + result.getIntersectionCount());
        System.out.println("  Frequent terms: " + result.getFrequentTermCount());

        double totalMs = (indexTime + mineTime) / 1_000_000.0;
        System.out.println("\n--- Breakdown ---");
        System.out.println(String.format("Index: %.1f%%", 100.0 * indexTime / (indexTime + mineTime)));
        System.out.println(String.format("Mine:  %.1f%%", 100.0 * mineTime / (indexTime + mineTime)));
        System.out.println(String.format("Total: %.1f ms", totalMs));
    }

    private static long totalBits(List<BitSet> sets) {
        long total = 0;
        for (BitSet b : sets) total += b.size() / 8;
        return total;
    }

    private static List<DocTerms> generateDocTerms(int count, int minTerms, int maxTerms, int vocabTarget) {
        Random rnd = new Random(42);
        List<DocTerms> docs = new ArrayList<>(count);

        byte[][] allPatterns = new byte[vocabTarget][];
        for (int i = 0; i < 20; i++) allPatterns[i] = ("H" + i).getBytes();
        for (int i = 20; i < 200; i++) allPatterns[i] = ("M" + i).getBytes();
        for (int i = 200; i < vocabTarget; i++) allPatterns[i] = ("L" + i).getBytes();

        for (int i = 0; i < count; i++) {
            int termCount = minTerms + rnd.nextInt(maxTerms - minTerms + 1);
            List<byte[]> terms = new ArrayList<>(termCount);

            for (int j = 0; j < 20; j++) { if (rnd.nextDouble() < 0.50) terms.add(allPatterns[j]); }
            for (int j = 20; j < 200; j++) { if (rnd.nextDouble() < 0.15) terms.add(allPatterns[j]); }
            for (int j = 200; j < vocabTarget; j++) { if (rnd.nextDouble() < 0.02) terms.add(allPatterns[j]); }

            docs.add(new DocTerms(i, terms));
        }
        return docs;
    }
}
