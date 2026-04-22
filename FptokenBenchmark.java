import cn.lxdb.plugins.muqingyu.fptoken.*;
import cn.lxdb.plugins.muqingyu.fptoken.config.SelectorConfig;
import cn.lxdb.plugins.muqingyu.fptoken.model.DocTerms;
import cn.lxdb.plugins.muqingyu.fptoken.model.SelectedGroup;
import java.util.*;

/**
 * 真正的 fptoken 项目性能基准测试
 */
public class FptokenBenchmark {

    public static void main(String[] args) {
        System.out.println("=== Fptoken 真实性能基准测试 ===\n");

        // 测试不同数据规模
        int[] docCounts = {100, 500, 1000, 2000};

        for (int docCount : docCounts) {
            System.out.println("--- 测试 " + docCount + " 条文档 ---");

            // 生成模拟数据
            List<DocTerms> docs = generateDocTerms(docCount, 10, 30);
            System.out.println("  生成 " + docs.size() + " 条 DocTerms");

            SelectorConfig config = SelectorConfig.builder()
                .minimumRequiredSupport(Math.max(2, docCount / 20))
                .minimumPatternLength(2)
                .maximumPatternLength(4)
                .maximumIntermediateResults(100)
                .build();

            // 预热
            for (int warm = 0; warm < 2; warm++) {
                try {
                    ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsets(
                        docs, config.getMinSupport(), config.getMinItemsetSize());
                } catch (Exception e) {
                    // ignore
                }
            }

            // 实际测试（3次取平均）
            long totalMs = 0;
            int totalGroups = 0;
            int runs = 3;

            for (int run = 0; run < runs; run++) {
                long start = System.nanoTime();

                List<SelectedGroup> result = ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsets(
                    docs, config.getMinSupport(), config.getMinItemsetSize());

                long elapsedNanos = System.nanoTime() - start;
                totalMs += elapsedNanos / 1_000_000;
                totalGroups += (result != null ? result.size() : 0);
            }

            double avgMs = totalMs / (double) runs;
            int avgGroups = totalGroups / runs;

            System.out.println("  平均: " + String.format("%.1f", avgMs) + " ms, " +
                avgGroups + " 个分组, " +
                String.format("%.0f", docCount / (avgMs / 1000.0)) + " docs/sec");
        }

        System.out.println("\n=== 基准测试完成 ===");
    }

    private static List<DocTerms> generateDocTerms(int count, int minTerms, int maxTerms) {
        Random rnd = new Random(42);
        List<DocTerms> docs = new ArrayList<>(count);

        byte[][] highFreqPatterns = {
            "GET".getBytes(), "HTTP".getBytes(), "200".getBytes(), "Host".getBytes(),
            "User-Agent".getBytes(), "Content-Type".getBytes(), "Accept".getBytes(),
            "Connection".getBytes(), "Cache-Control".getBytes(), "Cookie".getBytes()
        };

        for (int i = 0; i < count; i++) {
            int termCount = minTerms + rnd.nextInt(maxTerms - minTerms + 1);
            List<byte[]> terms = new ArrayList<>(termCount);

            for (byte[] pattern : highFreqPatterns) {
                if (rnd.nextDouble() < 0.3) {
                    terms.add(pattern);
                }
            }

            for (int j = 0; j < termCount - terms.size(); j++) {
                byte[] term = new byte[2 + rnd.nextInt(6)];
                rnd.nextBytes(term);
                terms.add(term);
            }

            docs.add(new DocTerms(i, terms));
        }

        return docs;
    }
}
