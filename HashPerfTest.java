import cn.lxdb.plugins.muqingyu.fptoken.index.TermTidsetIndex;
import cn.lxdb.plugins.muqingyu.fptoken.model.DocTerms;
import java.util.*;

/**
 * 对比 build() 使用 HashMap vs OpenHashTable 的性能。
 */
public class HashPerfTest {
    public static void main(String[] args) {
        int[] docCounts = {5000, 10000, 20000, 50000};
        int termsPerDoc = 50;

        for (int docCount : docCounts) {
            System.out.println("\n=== " + docCount + " docs, " + termsPerDoc + " terms/doc ===");
            List<DocTerms> docs = generateDocs(docCount, termsPerDoc, 10000);
            
            // Warmup
            for (int i = 0; i < 3; i++) {
                TermTidsetIndex.build(docs);
            }
            
            long start = System.nanoTime();
            TermTidsetIndex idx = TermTidsetIndex.build(docs);
            long elapsed = System.nanoTime() - start;
            
            System.out.printf("  build(): %.1f ms | vocab=%d%n",
                elapsed / 1_000_000.0, idx.getIdToTermUnsafe().size());
        }
    }

    static List<DocTerms> generateDocs(int count, int termsPerDoc, int vocabSize) {
        Random rnd = new Random(42);
        List<DocTerms> docs = new ArrayList<>(count);
        byte[][] vocab = new byte[vocabSize][];
        for (int i = 0; i < vocabSize; i++) {
            vocab[i] = ("term" + i).getBytes();
        }
        for (int docId = 0; docId < count; docId++) {
            Set<byte[]> terms = new LinkedHashSet<>();
            while (terms.size() < termsPerDoc) {
                terms.add(vocab[rnd.nextInt(vocabSize)]);
            }
            docs.add(new DocTerms(docId, new ArrayList<>(terms)));
        }
        return docs;
    }
}
