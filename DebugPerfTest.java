import cn.lxdb.plugins.muqingyu.fptoken.index.TermTidsetIndex;
import cn.lxdb.plugins.muqingyu.fptoken.model.DocTerms;
import java.util.*;

/**
 * Debug: trace why LargeScaleTest is slow with OpenHashTable.
 */
public class DebugPerfTest {
    public static void main(String[] args) {
        // Reproduce LargeScaleTest data
        Random rnd = new Random(42);
        int count = 20000;
        List<DocTerms> docs = new ArrayList<>(count);

        byte[][] highFreq = {
            "GET".getBytes(), "HTTP".getBytes(), "200".getBytes(), "Host".getBytes(),
            "User-Agent".getBytes(), "Content-Type".getBytes(), "Accept".getBytes(),
            "Connection".getBytes(), "POST".getBytes(), "404".getBytes()
        };
        byte[][] midFreq = new byte[30][];
        for (int i = 0; i < 30; i++) midFreq[i] = ("MID_" + i).getBytes();

        for (int i = 0; i < count; i++) {
            int termCount = 25 + rnd.nextInt(11); // avgTerms-5 to avgTerms+5 = 25-35
            List<byte[]> terms = new ArrayList<>(termCount);
            for (byte[] p : highFreq) {
                if (rnd.nextDouble() < 0.30) terms.add(p);
            }
            for (byte[] p : midFreq) {
                if (rnd.nextDouble() < 0.10) terms.add(p);
            }
            while (terms.size() < termCount) {
                byte[] t = new byte[2 + rnd.nextInt(4)];
                rnd.nextBytes(t);
                terms.add(t);
            }
            docs.add(new DocTerms(i, terms));
        }

        System.out.println("Docs: " + count + ", avg terms: ~" + (25+5));

        // Warmup
        for (int i = 0; i < 2; i++) TermTidsetIndex.build(docs);

        // Time build()
        long t0 = System.nanoTime();
        TermTidsetIndex idx = TermTidsetIndex.build(docs);
        long t1 = System.nanoTime();
        System.out.printf("build(): %.1f ms, vocab=%d%n",
            (t1 - t0) / 1_000_000.0, idx.getIdToTermUnsafe().size());

        // Now time selectExclusiveBestItemsets (full path, no sampling)
        int minSupport = Math.max(2, count / 20); // = 1000
        cn.lxdb.plugins.muqingyu.fptoken.ExclusiveFrequentItemsetSelector.setSamplingEnabled(false);
        
        t0 = System.nanoTime();
        List<cn.lxdb.plugins.muqingyu.fptoken.model.SelectedGroup> result = cn.lxdb.plugins.muqingyu.fptoken.ExclusiveFrequentItemsetSelector
            .selectExclusiveBestItemsets(docs, minSupport, 2);
        t1 = System.nanoTime();
        System.out.printf("selectExclusive(): %.1f ms, groups=%d%n",
            (t1 - t0) / 1_000_000.0, result != null ? result.size() : 0);
    }
}
