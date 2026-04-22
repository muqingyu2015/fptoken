/**
 * Developer: Large-scale test for sampling optimization.
 * Tests 20K-100K docs where Beam Search becomes the bottleneck.
 */
import cn.lxdb.plugins.muqingyu.fptoken.*;
import cn.lxdb.plugins.muqingyu.fptoken.config.SelectorConfig;
import cn.lxdb.plugins.muqingyu.fptoken.model.DocTerms;
import cn.lxdb.plugins.muqingyu.fptoken.model.SelectedGroup;
import java.util.*;

public class LargeScaleTest {

    public static void main(String[] args) {
        System.out.println("=== Large-Scale Sampling Test - Developer ===\n");

        // Fewer terms per doc to keep vocab manageable
        int[][] testCases = {
            {20000, 30},
            {50000, 30},
            {100000, 30},
        };

        for (int[] tc : testCases) {
            int docCount = tc[0];
            int avgTerms = tc[1];

            System.out.println("--- " + docCount + " docs, ~" + avgTerms + " terms/doc ---");

            long genStart = System.nanoTime();
            List<DocTerms> docs = generateDocTerms(docCount, avgTerms - 5, avgTerms + 5);
            long genTime = System.nanoTime() - genStart;
            System.out.println("  Data gen: " + (genTime / 1_000_000) + " ms");

            SelectorConfig config = SelectorConfig.builder()
                .minimumRequiredSupport(Math.max(2, docCount / 20))
                .minimumPatternLength(2)
                .maximumPatternLength(4)
                .maximumIntermediateResults(500)
                .build();

            int minSupport = config.getMinSupport();
            int minItemsetSize = config.getMinItemsetSize();

            // Single warmup
            try {
                ExclusiveFrequentItemsetSelector.setSamplingEnabled(true);
                ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsets(docs, minSupport, minItemsetSize);
            } catch (Exception e) { /* ignore */ }

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
                "  [Full]   %8.1f ms (%5.2f s) | %3d groups, %8d doc-refs",
                fullTime / 1_000_000.0, fullTime / 1_000_000_000.0,
                fullGroups, fullTotalDocs));

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
                "  [Sample] %8.1f ms (%5.2f s) | %3d groups, %8d doc-refs",
                sampleTime / 1_000_000.0, sampleTime / 1_000_000_000.0,
                sampleGroups, sampleTotalDocs));

            double speedup = (double) fullTime / sampleTime;
            System.out.println(String.format(
                "  => Speed: %.2fx | Groups diff: %d | Doc-refs diff: %d",
                speedup, sampleGroups - fullGroups, sampleTotalDocs - fullTotalDocs));
            System.out.println();
        }

        System.out.println("=== Done - Developer ===");
    }

    private static List<DocTerms> generateDocTerms(int count, int minTerms, int maxTerms) {
        Random rnd = new Random(42);
        List<DocTerms> docs = new ArrayList<>(count);

        // Use fewer patterns to keep vocabulary smaller
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

            for (byte[] p : highFreq) {
                if (rnd.nextDouble() < 0.30) terms.add(p);
            }
            for (byte[] p : midFreq) {
                if (rnd.nextDouble() < 0.10) terms.add(p);
            }
            for (int j = 0; j < termCount - terms.size(); j++) {
                byte[] t = new byte[2 + rnd.nextInt(4)];
                rnd.nextBytes(t);
                terms.add(t);
            }
            docs.add(new DocTerms(i, terms));
        }
        return docs;
    }
}
