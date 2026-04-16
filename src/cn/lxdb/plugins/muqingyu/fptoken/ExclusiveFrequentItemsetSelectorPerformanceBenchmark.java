package cn.lxdb.plugins.muqingyu.fptoken;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Locale;
import java.util.Random;

/**
 * 作者：muqingyu
 *
 * 性能测试示例：
 * 1) 自动构造不同规模数据（文档数、词数量）。
 * 2) 执行互斥频繁项集选择器并统计耗时。
 * 3) 输出 Markdown 报告到 docs/performance-benchmark-report.md。
 */
public class ExclusiveFrequentItemsetSelectorPerformanceBenchmark {

    private static final int[] DOC_SIZES = new int[] {2000, 5000, 10000};
    private static final int[] VOCAB_SIZES = new int[] {256, 1024, 4096};

    private static final int TERMS_PER_DOC = 12;
    private static final int BUNDLE_SIZE = 3;
    private static final int BUNDLE_COUNT = 24;
    private static final double BUNDLE_PROBABILITY = 0.70d;

    private static final int WARMUP_ROUNDS = 1;
    private static final int MEASURE_ROUNDS = 3;

    private static final int MIN_SUPPORT = 3;
    private static final int MIN_ITEMSET_SIZE = 2;
    private static final int MAX_ITEMSET_SIZE = 4;
    private static final int MAX_CANDIDATE_COUNT = 60_000;

    private static final long DATASET_SEED = 20260416L;

    public static void main(String[] args) throws Exception {
        List<BenchmarkResult> results = new ArrayList<BenchmarkResult>();
        int caseNo = 1;

        for (int docSize : DOC_SIZES) {
            for (int vocabSize : VOCAB_SIZES) {
                System.out.println("Running case " + caseNo
                        + " => docs=" + docSize + ", vocab=" + vocabSize);
                BenchmarkResult result = runSingleCase(caseNo, docSize, vocabSize);
                results.add(result);
                caseNo++;
            }
        }

        String report = buildMarkdownReport(results);
        File outFile = new File("docs/performance-benchmark-report.md");
        writeUtf8(outFile, report);

        System.out.println();
        System.out.println("===== Benchmark Summary =====");
        System.out.println(report);
        System.out.println("Report written to: " + outFile.getAbsolutePath());
    }

    private static BenchmarkResult runSingleCase(int caseNo, int docSize, int vocabSize) {
        List<DocTerms> dataset = generateDataset(docSize, vocabSize, TERMS_PER_DOC);

        // 预热，降低 JIT 对结果的影响。
        for (int i = 0; i < WARMUP_ROUNDS; i++) {
            ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsets(
                    dataset,
                    MIN_SUPPORT,
                    MIN_ITEMSET_SIZE,
                    MAX_ITEMSET_SIZE,
                    MAX_CANDIDATE_COUNT
            );
        }

        double minMs = Double.MAX_VALUE;
        double maxMs = 0.0d;
        double sumMs = 0.0d;
        int selectedGroups = 0;

        for (int i = 0; i < MEASURE_ROUNDS; i++) {
            long startNs = System.nanoTime();
            List<SelectedGroup> groups = ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsets(
                    dataset,
                    MIN_SUPPORT,
                    MIN_ITEMSET_SIZE,
                    MAX_ITEMSET_SIZE,
                    MAX_CANDIDATE_COUNT
            );
            long elapsedNs = System.nanoTime() - startNs;

            double elapsedMs = elapsedNs / 1_000_000.0d;
            minMs = Math.min(minMs, elapsedMs);
            maxMs = Math.max(maxMs, elapsedMs);
            sumMs += elapsedMs;
            selectedGroups = groups.size();
        }

        double avgMs = sumMs / MEASURE_ROUNDS;
        double docsPerSec = docSize / (avgMs / 1000.0d);
        return new BenchmarkResult(caseNo, docSize, vocabSize, avgMs, minMs, maxMs, docsPerSec, selectedGroups);
    }

    private static List<DocTerms> generateDataset(int docCount, int vocabSize, int termsPerDoc) {
        byte[][] vocabulary = buildVocabulary(vocabSize);
        int[][] bundles = buildBundles(vocabSize);
        Random random = new Random(DATASET_SEED + docCount * 97L + vocabSize * 131L);

        List<DocTerms> out = new ArrayList<DocTerms>(docCount);
        for (int docId = 1; docId <= docCount; docId++) {
            BitSet used = new BitSet(vocabSize);
            List<byte[]> terms = new ArrayList<byte[]>(termsPerDoc);

            if (random.nextDouble() < BUNDLE_PROBABILITY) {
                int[] bundle = bundles[random.nextInt(bundles.length)];
                for (int termId : bundle) {
                    if (!used.get(termId)) {
                        used.set(termId);
                        terms.add(vocabulary[termId]);
                    }
                }
            }

            while (terms.size() < termsPerDoc) {
                int termId = random.nextInt(vocabSize);
                if (!used.get(termId)) {
                    used.set(termId);
                    terms.add(vocabulary[termId]);
                }
            }
            out.add(new DocTerms(docId, terms));
        }
        return out;
    }

    private static byte[][] buildVocabulary(int vocabSize) {
        byte[][] vocab = new byte[vocabSize][];
        for (int i = 0; i < vocabSize; i++) {
            vocab[i] = intToBytes(i + 1);
        }
        return vocab;
    }

    private static int[][] buildBundles(int vocabSize) {
        int bundleCount = Math.min(BUNDLE_COUNT, Math.max(1, vocabSize / BUNDLE_SIZE));
        int[][] bundles = new int[bundleCount][BUNDLE_SIZE];
        int cursor = 0;
        for (int i = 0; i < bundleCount; i++) {
            for (int j = 0; j < BUNDLE_SIZE; j++) {
                bundles[i][j] = cursor % vocabSize;
                cursor++;
            }
        }
        return bundles;
    }

    private static byte[] intToBytes(int v) {
        return new byte[] {
                (byte) ((v >>> 24) & 0xFF),
                (byte) ((v >>> 16) & 0xFF),
                (byte) ((v >>> 8) & 0xFF),
                (byte) (v & 0xFF)
        };
    }

    private static String buildMarkdownReport(List<BenchmarkResult> results) {
        StringBuilder sb = new StringBuilder(4096);
        sb.append("# ExclusiveFrequentItemsetSelector 性能测试报告\n\n");
        sb.append("- 测试目标：按文档数与词数量对比算法耗时\n");
        sb.append("- 算法：高性能贪心互斥选择（byte[] 词）\n");
        sb.append("- 数据：合成数据，包含高频共现 bundle\n");
        sb.append("- 参数：")
                .append("minSupport=").append(MIN_SUPPORT)
                .append(", minItemsetSize=").append(MIN_ITEMSET_SIZE)
                .append(", maxItemsetSize=").append(MAX_ITEMSET_SIZE)
                .append(", maxCandidateCount=").append(MAX_CANDIDATE_COUNT)
                .append('\n');
        sb.append("- 轮次：warmup=").append(WARMUP_ROUNDS)
                .append(", measure=").append(MEASURE_ROUNDS)
                .append('\n');
        sb.append('\n');

        sb.append("| Case | Docs | Vocab | Avg(ms) | Min(ms) | Max(ms) | Docs/s | SelectedGroups |\n");
        sb.append("|---:|---:|---:|---:|---:|---:|---:|---:|\n");
        for (BenchmarkResult r : results) {
            sb.append('|').append(' ').append(r.caseNo).append(' ')
                    .append('|').append(' ').append(r.docCount).append(' ')
                    .append('|').append(' ').append(r.vocabSize).append(' ')
                    .append('|').append(' ').append(fmt(r.avgMs)).append(' ')
                    .append('|').append(' ').append(fmt(r.minMs)).append(' ')
                    .append('|').append(' ').append(fmt(r.maxMs)).append(' ')
                    .append('|').append(' ').append(fmt(r.docsPerSec)).append(' ')
                    .append('|').append(' ').append(r.selectedGroups).append(' ')
                    .append('|').append('\n');
        }

        sb.append('\n');
        sb.append("## 结果解读建议\n\n");
        sb.append("1. 先看同一 vocab 下 docs 增长时 Avg(ms) 的变化，评估文档规模扩展成本。\n");
        sb.append("2. 再看同一 docs 下 vocab 增长时 Avg(ms) 的变化，评估词典规模扩展成本。\n");
        sb.append("3. 若某组 Max(ms) 远高于 Avg(ms)，可适当调低 maxCandidateCount 以换取更稳时延。\n");
        sb.append("4. 合成数据与线上分布可能不同，建议再补一份真实样本基准。\n");
        return sb.toString();
    }

    private static void writeUtf8(File file, String content) throws IOException {
        File parent = file.getParentFile();
        if (parent != null && !parent.exists()) {
            parent.mkdirs();
        }
        Writer writer = new OutputStreamWriter(new FileOutputStream(file), "UTF-8");
        try {
            writer.write(content);
        } finally {
            writer.close();
        }
    }

    private static String fmt(double value) {
        return String.format(Locale.US, "%.2f", value);
    }

    private static final class BenchmarkResult {
        private final int caseNo;
        private final int docCount;
        private final int vocabSize;
        private final double avgMs;
        private final double minMs;
        private final double maxMs;
        private final double docsPerSec;
        private final int selectedGroups;

        private BenchmarkResult(
                int caseNo,
                int docCount,
                int vocabSize,
                double avgMs,
                double minMs,
                double maxMs,
                double docsPerSec,
                int selectedGroups
        ) {
            this.caseNo = caseNo;
            this.docCount = docCount;
            this.vocabSize = vocabSize;
            this.avgMs = avgMs;
            this.minMs = minMs;
            this.maxMs = maxMs;
            this.docsPerSec = docsPerSec;
            this.selectedGroups = selectedGroups;
        }
    }
}
