/**
 * Developer: Sampling vs Full-scan comparison benchmark.
 *
 * Tests speed, memory, and result quality of 30% sampling mining
 * vs full-data mining on simulated network traffic data.
 */
import cn.lxdb.plugins.muqingyu.fptoken.*;
import cn.lxdb.plugins.muqingyu.fptoken.config.SelectorConfig;
import cn.lxdb.plugins.muqingyu.fptoken.model.DocTerms;
import cn.lxdb.plugins.muqingyu.fptoken.model.SelectedGroup;
import java.util.*;

public class SamplingCompareTest {

    public static void main(String[] args) {
        System.out.println("=== Sampling Optimization Comparison - Developer ===\n");

        int[][] testCases = {
            {500, 30},
            {1000, 40},
            {2000, 50},
            {5000, 60},
            {10000, 70},
        };

        for (int[] tc : testCases) {
            int docCount = tc[0];
            int avgTerms = tc[1];

            System.out.println("--- " + docCount + " docs, ~" + avgTerms + " terms/doc ---");

            List<DocTerms> docs = generateDocTerms(docCount, avgTerms - 10, avgTerms + 10);
            int vocabSize = estimateVocab(docs);
            System.out.println("  Vocabulary: " + vocabSize + " unique terms");

            SelectorConfig config = SelectorConfig.builder()
                .minimumRequiredSupport(Math.max(2, docCount / 20))
                .minimumPatternLength(2)
                .maximumPatternLength(4)
                .maximumIntermediateResults(200)
                .build();

            int minSupport = config.getMinSupport();
            int minItemsetSize = config.getMinItemsetSize();

            // Warmup
            for (int w = 0; w < 2; w++) {
                try {
                    ExclusiveFrequentItemsetSelector.setSamplingEnabled(false);
                    ExclusiveFrequentItemsetSelector.setSamplingSupportScale(0.0);
                    ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsets(docs, minSupport, minItemsetSize);
                    ExclusiveFrequentItemsetSelector.setSamplingEnabled(true);
                    ExclusiveFrequentItemsetSelector.setSamplingSupportScale(0.0);
                    ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsets(docs, minSupport, minItemsetSize);
                } catch (Exception e) { /* ignore */ }
            }

            // ===== Full scan =====
            Runtime rt = Runtime.getRuntime();
            ExclusiveFrequentItemsetSelector.setSamplingEnabled(false);
            ExclusiveFrequentItemsetSelector.setSamplingSupportScale(0.0);
            System.gc(); Thread.yield();
            long memBeforeFull = rt.totalMemory() - rt.freeMemory();

            long t0 = System.nanoTime();
            List<SelectedGroup> fullResult = ExclusiveFrequentItemsetSelector
                .selectExclusiveBestItemsets(docs, minSupport, minItemsetSize);
            long fullTime = System.nanoTime() - t0;

            System.gc(); Thread.yield();
            long memAfterFull = rt.totalMemory() - rt.freeMemory();
            long memFullKB = (memAfterFull - memBeforeFull) / 1024;

            int fullGroups = fullResult != null ? fullResult.size() : 0;
            int fullTotalDocs = 0;
            if (fullResult != null) {
                for (SelectedGroup g : fullResult) fullTotalDocs += g.getDocIds().size();
            }

            System.out.println(String.format(
                "  [Full]   %6.1f ms | %3d groups, %5d doc-refs | mem: %d KB",
                fullTime / 1_000_000.0, fullGroups, fullTotalDocs, memFullKB));

            // ===== Sampling (新策略: 在采样文档集上建索引，节省索引时间) =====
            ExclusiveFrequentItemsetSelector.setSamplingEnabled(true);
            ExclusiveFrequentItemsetSelector.setSamplingSupportScale(0.0); // auto-scale
            System.gc(); Thread.yield();
            long memBeforeSample = rt.totalMemory() - rt.freeMemory();

            t0 = System.nanoTime();
            List<SelectedGroup> sampleResult = ExclusiveFrequentItemsetSelector
                .selectExclusiveBestItemsets(docs, minSupport, minItemsetSize);
            long sampleTime = System.nanoTime() - t0;

            System.gc(); Thread.yield();
            long memAfterSample = rt.totalMemory() - rt.freeMemory();
            long memSampleKB = (memAfterSample - memBeforeSample) / 1024;

            int sampleGroups = sampleResult != null ? sampleResult.size() : 0;
            int sampleTotalDocs = 0;
            if (sampleResult != null) {
                for (SelectedGroup g : sampleResult) sampleTotalDocs += g.getDocIds().size();
            }

            System.out.println(String.format(
                "  [Sample] %6.1f ms | %3d groups, %5d doc-refs | mem: %d KB",
                sampleTime / 1_000_000.0, sampleGroups, sampleTotalDocs, memSampleKB));

            // Comparison
            double speedup = (double) fullTime / sampleTime;
            System.out.println(String.format(
                "  => Speed: %.1fx | Groups diff: %d | Doc-refs diff: %d",
                speedup, sampleGroups - fullGroups, sampleTotalDocs - fullTotalDocs));
            System.out.println();
        }

        System.out.println("=== Done - Developer ===");
    }

    private static int estimateVocab(List<DocTerms> docs) {
        Set<String> seen = new HashSet<>();
        int limit = Math.min(2000, docs.size());
        for (int i = 0; i < limit; i++) {
            for (byte[] term : docs.get(i).getTermsUnsafe()) {
                seen.add(new String(term));
            }
        }
        return seen.size();
    }

    private static List<DocTerms> generateDocTerms(int count, int minTerms, int maxTerms) {
        Random rnd = new Random(42);
        List<DocTerms> docs = new ArrayList<>(count);

        byte[][] highFreq = {
            "GET".getBytes(), "HTTP".getBytes(), "200".getBytes(), "Host".getBytes(),
            "User-Agent".getBytes(), "Content-Type".getBytes(), "Accept".getBytes(),
            "Connection".getBytes(), "POST".getBytes(), "404".getBytes()
        };

        byte[][] midFreq = new byte[80][];
        for (int i = 0; i < 80; i++) midFreq[i] = ("MID_" + i).getBytes();

        for (int i = 0; i < count; i++) {
            int termCount = minTerms + rnd.nextInt(maxTerms - minTerms + 1);
            List<byte[]> terms = new ArrayList<>(termCount);

            for (byte[] p : highFreq) {
                if (rnd.nextDouble() < 0.35) terms.add(p);
            }
            for (byte[] p : midFreq) {
                if (rnd.nextDouble() < 0.12) terms.add(p);
            }
            for (int j = 0; j < termCount - terms.size(); j++) {
                byte[] t = new byte[2 + rnd.nextInt(6)];
                rnd.nextBytes(t);
                terms.add(t);
            }
            docs.add(new DocTerms(i, terms));
        }
        return docs;
    }
}
