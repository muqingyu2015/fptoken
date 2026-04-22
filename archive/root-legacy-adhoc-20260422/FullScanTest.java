/**
 * Developer: Benchmark scanning full rows for doclist recomputation.
 * Compare: index build (buildWithSupportBounds) vs full scan for recompute.
 */
import cn.lxdb.plugins.muqingyu.fptoken.*;
import cn.lxdb.plugins.muqingyu.fptoken.config.SelectorConfig;
import cn.lxdb.plugins.muqingyu.fptoken.config.EngineTuningConfig;
import cn.lxdb.plugins.muqingyu.fptoken.index.TermTidsetIndex;
import cn.lxdb.plugins.muqingyu.fptoken.model.DocTerms;
import java.util.*;

public class FullScanTest {

    public static void main(String[] args) {
        System.out.println("=== Full Scan vs Index Build - Developer ===\n");

        int[][] configs = {
            {100000, 50, 10000},
            {50000, 50, 5000},
            {20000, 50, 2000},
        };

        for (int[] cfg : configs) {
            int docCount = cfg[0], avgTerms = cfg[1], vocabTarget = cfg[2];
            System.out.println("--- " + docCount + " docs, " + avgTerms + " terms/doc, " + vocabTarget + " vocab ---");

            long t0 = System.nanoTime();
            List<DocTerms> docs = generateDocTerms(docCount, avgTerms - 10, avgTerms + 10, vocabTarget);
            System.out.println("  Gen: " + (System.nanoTime() - t0) / 1_000_000 + " ms");

            // 1) Full index build (current bottleneck)
            System.gc();
            t0 = System.nanoTime();
            TermTidsetIndex fullIndex = TermTidsetIndex.buildWithSupportBounds(
                docs, 1000, EngineTuningConfig.DEFAULT_MAX_DOC_COVERAGE_RATIO);
            long buildTime = System.nanoTime() - t0;
            System.out.println("  Index build: " + (buildTime / 1_000_000) + " ms | vocab="
                + fullIndex.getIdToTermUnsafe().size());

            // 2) Simulate: scan full rows for 20 candidate combinations (each with 2-4 terms)
            // Candidate terms: pick from high-freq patterns
            byte[][] cand1 = {"H0".getBytes(), "H1".getBytes()};
            byte[][] cand2 = {"H2".getBytes(), "H3".getBytes(), "H4".getBytes()};
            byte[][] cand3 = {"H5".getBytes(), "H6".getBytes()};
            byte[][][] candidates = {cand1, cand2, cand3};
            // Add 17 more - just measure with 20 candidates
            int numCandidates = 20;

            // Measure scanning for 20 candidate sets
            System.gc();
            int scanRuns = 5;
            long totalScan = 0;
            for (int r = 0; r < scanRuns; r++) {
                t0 = System.nanoTime();
                // Scan all docs and find those matching 20 candidate sets
                for (int ci = 0; ci < 20; ci++) {
                    // Build a candidate set: 2-4 random high-freq terms
                    int candSize = 2 + (ci % 3); // 2-4
                    byte[][] candTerms = new byte[candSize][];
                    for (int ti = 0; ti < candSize; ti++) {
                        candTerms[ti] = ("H" + (ci * 3 + ti) % 20).getBytes();
                    }
                    // Scan docs
                    BitSet docBits = new BitSet(docCount);
                    for (int di = 0; di < docs.size(); di++) {
                        List<byte[]> terms = docs.get(di).getTermsUnsafe();
                        boolean allMatch = true;
                        for (byte[] ct : candTerms) {
                            if (!containsTerm(terms, ct)) {
                                allMatch = false;
                                break;
                            }
                        }
                        if (allMatch) {
                            docBits.set(di);
                        }
                    }
                    // Just need cardinality for timing
                    int sup = docBits.cardinality();
                    if (sup > 0) {
                        // Keep the result (not discarded by JIT)
                    }
                }
                totalScan += System.nanoTime() - t0;
            }
            long scanTime = totalScan / scanRuns;
            System.out.println("  Full scan (20 cands): " + (scanTime / 1_000_000) + " ms");
            System.out.println("  Scan vs Build ratio: " + String.format("%.1f%%", 100.0 * scanTime / buildTime));
            System.out.println();
        }
        System.out.println("=== Done - Developer ===");
    }

    private static boolean containsTerm(List<byte[]> terms, byte[] target) {
        for (byte[] t : terms) {
            if (Arrays.equals(t, target)) return true;
        }
        return false;
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
