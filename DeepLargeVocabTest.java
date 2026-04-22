/**
 * Developer: Large vocabulary deep test for sampling.
 * Tests with 10K+ unique terms to stress the mining phase.
 */
import cn.lxdb.plugins.muqingyu.fptoken.*;
import cn.lxdb.plugins.muqingyu.fptoken.config.SelectorConfig;
import cn.lxdb.plugins.muqingyu.fptoken.model.DocTerms;
import cn.lxdb.plugins.muqingyu.fptoken.model.SelectedGroup;
import java.util.*;

public class DeepLargeVocabTest {

    public static void main(String[] args) {
        System.out.println("=== Deep Large Vocabulary Sampling Test - Developer ===\n");

        // Case 1: Moderate docs, huge vocab → mining-heavy
        // Case 2: Many docs, moderate vocab → index-heavy  
        // Case 3: Many docs, huge vocab → both heavy
        int[][] configs = {
            {20000, 50, 10000},   // 20K docs, 50 terms/doc, 10K vocab
            {50000, 30, 5000},    // 50K docs, 30 terms/doc, 5K vocab
            {100000, 50, 20000},  // 100K docs, 50 terms/doc, 20K vocab
        };

        for (int[] cfg : configs) {
            int docCount = cfg[0], avgTerms = cfg[1], vocabTarget = cfg[2];
            int minSupport = Math.max(2, docCount / 20);
            
            System.out.println("--- " + docCount + " docs, ~" + avgTerms + " terms/doc, ~" + vocabTarget + " vocab ---");
            System.out.println("    minSupport=" + minSupport + "\n");

            long t0 = System.nanoTime();
            List<DocTerms> docs = generateDocTerms(docCount, avgTerms - 5, avgTerms + 5, vocabTarget);
            long genTime = System.nanoTime() - t0;
            System.out.println("  Data gen: " + (genTime / 1_000_000) + " ms\n");

            // Warmup
            ExclusiveFrequentItemsetSelector.setSamplingEnabled(true);
            try {
                ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsets(docs, minSupport, 2);
            } catch (Exception e) { e.printStackTrace(); }

            // ===== Full scan =====
            Runtime rt = Runtime.getRuntime();
            ExclusiveFrequentItemsetSelector.setSamplingEnabled(false);
            System.gc();
            
            long memBefore = rt.totalMemory() - rt.freeMemory();
            t0 = System.nanoTime();
            List<SelectedGroup> fullResult = ExclusiveFrequentItemsetSelector
                .selectExclusiveBestItemsets(docs, minSupport, 2);
            long fullTime = System.nanoTime() - t0;
            long memAfter = rt.totalMemory() - rt.freeMemory();
            System.gc();

            int fullGroups = fullResult != null ? fullResult.size() : 0;
            int fullTotalDocs = 0;
            if (fullResult != null) {
                for (SelectedGroup g : fullResult) fullTotalDocs += g.getDocIds().size();
            }

            System.out.println(String.format(
                "  [Full]   %8.1f ms | %3d groups, %8d doc-refs | mem=%d MB",
                fullTime / 1_000_000.0, fullGroups, fullTotalDocs,
                (memAfter - memBefore) / 1024 / 1024));

            // ===== Sampling =====
            ExclusiveFrequentItemsetSelector.setSamplingEnabled(true);
            System.gc();

            memBefore = rt.totalMemory() - rt.freeMemory();
            t0 = System.nanoTime();
            List<SelectedGroup> sampleResult = ExclusiveFrequentItemsetSelector
                .selectExclusiveBestItemsets(docs, minSupport, 2);
            long sampleTime = System.nanoTime() - t0;
            memAfter = rt.totalMemory() - rt.freeMemory();
            System.gc();

            int sampleGroups = sampleResult != null ? sampleResult.size() : 0;
            int sampleTotalDocs = 0;
            if (sampleResult != null) {
                for (SelectedGroup g : sampleResult) sampleTotalDocs += g.getDocIds().size();
            }

            System.out.println(String.format(
                "  [Sample] %8.1f ms | %3d groups, %8d doc-refs | mem=%d MB",
                sampleTime / 1_000_000.0, sampleGroups, sampleTotalDocs,
                (memAfter - memBefore) / 1024 / 1024));

            double speedup = (double) fullTime / sampleTime;
            System.out.println(String.format(
                "  => Speed: %.2fx | Groups diff: %d | Doc-refs diff: %d",
                speedup, sampleGroups - fullGroups, sampleTotalDocs - fullTotalDocs));
            
            // Breakdown estimation: how many candidates did we get?
            System.out.println();
        }

        System.out.println("=== Done - Developer ===");
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

            // High freq terms: ~50% appear
            for (int j = 0; j < 20; j++) { if (rnd.nextDouble() < 0.50) terms.add(allPatterns[j]); }
            // Mid freq terms: ~15% appear
            for (int j = 20; j < 200; j++) { if (rnd.nextDouble() < 0.15) terms.add(allPatterns[j]); }
            // Low freq terms: ~2% appear
            for (int j = 200; j < vocabTarget; j++) { if (rnd.nextDouble() < 0.02) terms.add(allPatterns[j]); }

            docs.add(new DocTerms(i, terms));
        }
        return docs;
    }
}
