/**
 * Developer: Large vocabulary test for sampling optimization.
 * Uses larger vocabulary to stress the mining phase.
 */
import cn.lxdb.plugins.muqingyu.fptoken.*;
import cn.lxdb.plugins.muqingyu.fptoken.config.SelectorConfig;
import cn.lxdb.plugins.muqingyu.fptoken.model.DocTerms;
import cn.lxdb.plugins.muqingyu.fptoken.model.SelectedGroup;
import java.util.*;

public class LargeVocabTest {

    public static void main(String[] args) {
        System.out.println("=== Large Vocabulary Sampling Test - Developer ===\n");

        // More realistic: more unique terms per doc
        int[][] testCases = {
            {10000, 50, 500},   // 10K docs, 50 terms/doc, 500 unique terms
            {20000, 50, 1000},  // 20K docs, 50 terms/doc, 1000 unique terms
            {50000, 50, 2000},  // 50K docs, 50 terms/doc, 2000 unique terms
        };

        for (int[] tc : testCases) {
            int docCount = tc[0];
            int avgTerms = tc[1];
            int vocabTarget = tc[2];

            System.out.println("--- " + docCount + " docs, ~" + avgTerms + " terms/doc, ~" + vocabTarget + " vocab ---");

            List<DocTerms> docs = generateDocTerms(docCount, avgTerms - 10, avgTerms + 10, vocabTarget);

            SelectorConfig config = SelectorConfig.builder()
                .minimumRequiredSupport(Math.max(2, docCount / 20))
                .minimumPatternLength(2)
                .maximumPatternLength(4)
                .maximumIntermediateResults(500)
                .build();

            int minSupport = config.getMinSupport();
            int minItemsetSize = config.getMinItemsetSize();

            // Warmup
            for (int w = 0; w < 2; w++) {
                try {
                    ExclusiveFrequentItemsetSelector.setSamplingEnabled(true);
                    ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsets(docs, minSupport, minItemsetSize);
                } catch (Exception e) {}
            }

            // ===== Full scan =====
            Runtime rt = Runtime.getRuntime();
            ExclusiveFrequentItemsetSelector.setSamplingEnabled(false);
            System.gc(); Thread.yield();

            long t0 = System.nanoTime();
            List<SelectedGroup> fullResult = ExclusiveFrequentItemsetSelector
                .selectExclusiveBestItemsets(docs, minSupport, minItemsetSize);
            long fullTime = System.nanoTime() - t0;
            System.gc(); Thread.yield();

            int fullGroups = fullResult != null ? fullResult.size() : 0;
            int fullTotalDocs = 0;
            if (fullResult != null) {
                for (SelectedGroup g : fullResult) fullTotalDocs += g.getDocIds().size();
            }

            System.out.println(String.format(
                "  [Full]   %8.1f ms | %3d groups, %8d doc-refs",
                fullTime / 1_000_000.0, fullGroups, fullTotalDocs));

            // ===== Sampling =====
            ExclusiveFrequentItemsetSelector.setSamplingEnabled(true);
            System.gc(); Thread.yield();

            t0 = System.nanoTime();
            List<SelectedGroup> sampleResult = ExclusiveFrequentItemsetSelector
                .selectExclusiveBestItemsets(docs, minSupport, minItemsetSize);
            long sampleTime = System.nanoTime() - t0;
            System.gc(); Thread.yield();

            int sampleGroups = sampleResult != null ? sampleResult.size() : 0;
            int sampleTotalDocs = 0;
            if (sampleResult != null) {
                for (SelectedGroup g : sampleResult) sampleTotalDocs += g.getDocIds().size();
            }

            System.out.println(String.format(
                "  [Sample] %8.1f ms | %3d groups, %8d doc-refs",
                sampleTime / 1_000_000.0, sampleGroups, sampleTotalDocs));

            double speedup = (double) fullTime / sampleTime;
            System.out.println(String.format(
                "  => Speed: %.2fx | Groups diff: %d | Doc-refs diff: %d",
                speedup, sampleGroups - fullGroups, sampleTotalDocs - fullTotalDocs));
            System.out.println();
        }

        System.out.println("=== Done - Developer ===");
    }

    private static List<DocTerms> generateDocTerms(int count, int minTerms, int maxTerms, int vocabTarget) {
        Random rnd = new Random(42);
        List<DocTerms> docs = new ArrayList<>(count);

        // Generate target vocabulary
        byte[][] allPatterns = new byte[vocabTarget][];
        for (int i = 0; i < 10; i++) {
            allPatterns[i] = ("HIGH_" + i).getBytes(); // ~40% freq
        }
        for (int i = 10; i < Math.min(100, vocabTarget); i++) {
            allPatterns[i] = ("MID_" + i).getBytes(); // ~15% freq
        }
        for (int i = 100; i < vocabTarget; i++) {
            allPatterns[i] = ("LOW_" + i).getBytes(); // ~3% freq
        }

        for (int i = 0; i < count; i++) {
            int termCount = minTerms + rnd.nextInt(maxTerms - minTerms + 1);
            List<byte[]> terms = new ArrayList<>(termCount);

            // High freq: ~40%
            for (int j = 0; j < 10 && j < vocabTarget; j++) {
                if (rnd.nextDouble() < 0.40) terms.add(allPatterns[j]);
            }
            // Mid freq: ~15%
            for (int j = 10; j < Math.min(100, vocabTarget); j++) {
                if (rnd.nextDouble() < 0.15) terms.add(allPatterns[j]);
            }
            // Low freq: ~3%
            for (int j = 100; j < vocabTarget; j++) {
                if (rnd.nextDouble() < 0.03) terms.add(allPatterns[j]);
            }

            docs.add(new DocTerms(i, terms));
        }
        return docs;
    }
}
