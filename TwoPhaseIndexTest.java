/**
 * Developer: Test two-phase index build vs old build for memory & time.
 */
import cn.lxdb.plugins.muqingyu.fptoken.*;
import cn.lxdb.plugins.muqingyu.fptoken.config.SelectorConfig;
import cn.lxdb.plugins.muqingyu.fptoken.config.EngineTuningConfig;
import cn.lxdb.plugins.muqingyu.fptoken.index.TermTidsetIndex;
import cn.lxdb.plugins.muqingyu.fptoken.model.DocTerms;
import cn.lxdb.plugins.muqingyu.fptoken.model.SelectedGroup;
import java.util.*;

public class TwoPhaseIndexTest {

    public static void main(String[] args) {
        System.out.println("=== Two-Phase Index Build Validation - Developer ===\n");

        int[][] configs = {
            {20000, 50, 10000, 1000},
            {50000, 30, 5000, 2500},
            {100000, 50, 20000, 5000},
        };

        for (int[] cfg : configs) {
            int docCount = cfg[0], avgTerms = cfg[1], vocabTarget = cfg[2], minSupport = cfg[3];

            System.out.println("--- " + docCount + " docs, " + avgTerms + " terms/doc, " + vocabTarget + " vocab, minSup=" + minSupport + " ---");

            long t0 = System.nanoTime();
            List<DocTerms> docs = generateDocTerms(docCount, avgTerms - 5, avgTerms + 5, vocabTarget);
            System.out.println("  Data gen: " + (System.nanoTime() - t0) / 1_000_000 + " ms");

            // Two-phase buildWithSupportBounds
            Runtime rt = Runtime.getRuntime();
            System.gc(); try { Thread.sleep(100); } catch (Exception e) {}

            long memBefore = rt.totalMemory() - rt.freeMemory();
            t0 = System.nanoTime();
            TermTidsetIndex twoPhaseIndex = TermTidsetIndex.buildWithSupportBounds(
                docs, minSupport, EngineTuningConfig.DEFAULT_MAX_DOC_COVERAGE_RATIO);
            long twoPhaseTime = System.nanoTime() - t0;
            long memAfter = rt.totalMemory() - rt.freeMemory();

            int twoPhaseVocab = twoPhaseIndex.getIdToTermUnsafe().size();
            long twoPhaseBits = 0;
            for (BitSet b : twoPhaseIndex.getTidsetsByTermIdUnsafe()) twoPhaseBits += b.size();

            System.out.println(String.format(
                "  [TwoPhase] %6.1f ms | vocab=%d, bits=%d KB, mem=%d MB",
                twoPhaseTime / 1_000_000.0, twoPhaseVocab, twoPhaseBits / 8 / 1024,
                (memAfter - memBefore) / 1024 / 1024));

            // Old build() + filter (simulate old behavior)
            System.gc(); try { Thread.sleep(100); } catch (Exception e) {}

            memBefore = rt.totalMemory() - rt.freeMemory();
            t0 = System.nanoTime();
            TermTidsetIndex rawIndex = TermTidsetIndex.build(docs);
            long buildTime = System.nanoTime() - t0;

            // Filter
            List<byte[]> filteredTerms = new ArrayList<>();
            List<BitSet> filteredTidsets = new ArrayList<>();
            List<byte[]> rawTerms = rawIndex.getIdToTermUnsafe();
            List<BitSet> rawTidsets = rawIndex.getTidsetsByTermIdUnsafe();
            for (int i = 0; i < rawTerms.size(); i++) {
                BitSet bits = rawTidsets.get(i);
                if (bits.cardinality() >= minSupport) {
                    filteredTerms.add(rawTerms.get(i));
                    filteredTidsets.add(bits);
                }
            }
            long oldTime = System.nanoTime() - t0;
            memAfter = rt.totalMemory() - rt.freeMemory();

            int oldVocab = rawTerms.size();
            long oldBits = 0;
            for (BitSet b : rawTidsets) oldBits += b.size();

            System.out.println(String.format(
                "  [Old]     %6.1f ms | rawVocab=%d, afterFilter=%d, bits=%d KB, mem=%d MB",
                oldTime / 1_000_000.0, oldVocab, filteredTerms.size(), oldBits / 8 / 1024,
                (memAfter - memBefore) / 1024 / 1024));

            // Verify results match
            boolean match = twoPhaseVocab == filteredTerms.size();
            if (match) {
                for (int i = 0; i < twoPhaseVocab && match; i++) {
                    if (!Arrays.equals(twoPhaseIndex.getIdToTermUnsafe().get(i), filteredTerms.get(i))) {
                        match = false;
                    }
                }
            }
            System.out.println("  => Results match: " + match);
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

            for (int j = 0; j < 20; j++) { if (rnd.nextDouble() < 0.50) terms.add(allPatterns[j]); }
            for (int j = 20; j < 200; j++) { if (rnd.nextDouble() < 0.15) terms.add(allPatterns[j]); }
            for (int j = 200; j < vocabTarget; j++) { if (rnd.nextDouble() < 0.02) terms.add(allPatterns[j]); }

            docs.add(new DocTerms(i, terms));
        }
        return docs;
    }
}
