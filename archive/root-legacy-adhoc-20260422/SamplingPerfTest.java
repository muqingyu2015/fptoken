import cn.lxdb.plugins.muqingyu.fptoken.*;
import cn.lxdb.plugins.muqingyu.fptoken.config.SelectorConfig;
import cn.lxdb.plugins.muqingyu.fptoken.model.DocTerms;
import cn.lxdb.plugins.muqingyu.fptoken.model.ExclusiveSelectionResult;
import cn.lxdb.plugins.muqingyu.fptoken.model.SelectedGroup;
import java.util.*;

/**
 * 采样优化对比测试：
 * 对比「全量挖掘」vs「20%采样挖掘+全量回算」的速度和质量
 */
public class SamplingPerfTest {

    public static void main(String[] args) {
        System.out.println("=== 采样优化对比测试 ===\n");

        int[] docCounts = {500, 1000, 2000, 5000, 10000};
        int[] termCounts = {20, 30, 40, 50, 60}; // 每篇文档平均词数

        for (int idx = 0; idx < docCounts.length; idx++) {
            int docCount = docCounts[idx];
            int termCount = termCounts[idx];

            System.out.println("--- " + docCount + " 条文档, 每条约 " + termCount + " 词 ---");

            // 生成模拟数据
            List<DocTerms> docs = generateDocTerms(docCount, termCount - 10, termCount + 10);
            System.out.println("  词表大小: ~" + estimateVocabularySize(docs, 1000));

            SelectorConfig config = SelectorConfig.builder()
                .minimumRequiredSupport(Math.max(2, docCount / 20))
                .minimumPatternLength(2)
                .maximumPatternLength(4)
                .maximumIntermediateResults(200)
                .build();

            // 预热
            for (int warm = 0; warm < 2; warm++) {
                try {
                    ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsets(
                        docs, config.getMinSupport(), config.getMinItemsetSize());
                } catch (Exception e) { /* ignore */ }
            }

            // === 全量挖掘 ===
            Runtime rt = Runtime.getRuntime();
            System.gc();
            long memBefore = rt.totalMemory() - rt.freeMemory();

            long start = System.nanoTime();
            List<SelectedGroup> fullResult = ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsets(
                docs, config.getMinSupport(), config.getMinItemsetSize());
            long fullTime = System.nanoTime() - start;

            System.gc();
            long memAfter = rt.totalMemory() - rt.freeMemory();

            System.out.println("  全量: " + String.format("%.1f", fullTime / 1_000_000.0) + " ms, " +
                (fullResult != null ? fullResult.size() : 0) + " 分组, " +
                "mem delta: " + ((memAfter - memBefore) / 1024) + " KB");

            // === 采样挖掘（内部自动 20%） ===
            System.gc();
            long memBefore2 = rt.totalMemory() - rt.freeMemory();

            start = System.nanoTime();
            List<SelectedGroup> sampledResult = ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsets(
                docs, config.getMinSupport(), config.getMinItemsetSize());
            long sampledTime = System.nanoTime() - start;

            System.gc();
            long memAfter2 = rt.totalMemory() - rt.freeMemory();

            System.out.println("  采样: " + String.format("%.1f", sampledTime / 1_000_000.0) + " ms, " +
                (sampledResult != null ? sampledResult.size() : 0) + " 分组, " +
                "mem delta: " + ((memAfter2 - memBefore2) / 1024) + " KB");

            if (fullTime > 0) {
                double speedup = (double) fullTime / sampledTime;
                System.out.println("  加速比: " + String.format("%.1f", speedup) + "x");
            }

            System.out.println();
        }

        System.out.println("=== 测试完成 ===");
    }

    private static int estimateVocabularySize(List<DocTerms> docs, int maxSample) {
        Set<String> seen = new HashSet<>();
        int limit = Math.min(maxSample, docs.size());
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

        byte[][] highFreqPatterns = {
            "GET".getBytes(), "HTTP".getBytes(), "200".getBytes(), "Host".getBytes(),
            "User-Agent".getBytes(), "Content-Type".getBytes(), "Accept".getBytes(),
            "Connection".getBytes(), "Cache-Control".getBytes(), "Cookie".getBytes(),
            "POST".getBytes(), "404".getBytes(), "Location".getBytes(), "Referer".getBytes(),
            "Set-Cookie".getBytes(), "X-Forwarded-For".getBytes(), "Content-Length".getBytes()
        };

        // 增加高频模式种类，模拟更大词表
        byte[][] midFreqPatterns = new byte[50][];
        for (int i = 0; i < 50; i++) {
            midFreqPatterns[i] = ("MID_" + i).getBytes();
        }

        for (int i = 0; i < count; i++) {
            int termCount = minTerms + rnd.nextInt(maxTerms - minTerms + 1);
            List<byte[]> terms = new ArrayList<>(termCount);

            // 高频词：~40% 概率
            for (byte[] pattern : highFreqPatterns) {
                if (rnd.nextDouble() < 0.4) {
                    terms.add(pattern);
                }
            }

            // 中频词：~15% 概率
            for (byte[] pattern : midFreqPatterns) {
                if (rnd.nextDouble() < 0.15) {
                    terms.add(pattern);
                }
            }

            // 随机填充
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
