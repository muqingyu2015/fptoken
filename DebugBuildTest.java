import cn.lxdb.plugins.muqingyu.fptoken.index.TermTidsetIndex;
import cn.lxdb.plugins.muqingyu.fptoken.model.DocTerms;
import java.util.*;

public class DebugBuildTest {
    public static void main(String[] args) {
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
            int termCount = 25 + rnd.nextInt(11);
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

        System.out.println("Docs: " + count);

        int minSupport = 1000; // match LargeScaleTest

        // Warmup
        TermTidsetIndex.build(docs);
        TermTidsetIndex.buildWithSupportBounds(docs, minSupport, 1.0d);

        // Test build()
        long t0 = System.nanoTime();
        TermTidsetIndex idx1 = TermTidsetIndex.build(docs);
        long t1 = System.nanoTime();
        System.out.printf("build(): %.1f ms, vocab=%d%n",
            (t1 - t0) / 1_000_000.0, idx1.getIdToTermUnsafe().size());

        // Test buildWithSupportBounds
        t0 = System.nanoTime();
        TermTidsetIndex idx2 = TermTidsetIndex.buildWithSupportBounds(docs, minSupport, 1.0d);
        t1 = System.nanoTime();
        System.out.printf("buildWithSupportBounds(minSup=%d): %.1f ms, vocab=%d%n",
            minSupport, (t1 - t0) / 1_000_000.0, idx2.getIdToTermUnsafe().size());

        // Also try buildWithSupportBounds with minSupport=1 (no filter)
        t0 = System.nanoTime();
        TermTidsetIndex idx3 = TermTidsetIndex.buildWithSupportBounds(docs, 1, 1.0d);
        t1 = System.nanoTime();
        System.out.printf("buildWithSupportBounds(minSup=1): %.1f ms, vocab=%d%n",
            (t1 - t0) / 1_000_000.0, idx3.getIdToTermUnsafe().size());
    }
}
