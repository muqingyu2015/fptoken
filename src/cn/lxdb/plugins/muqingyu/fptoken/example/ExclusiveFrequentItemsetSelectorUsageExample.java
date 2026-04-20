package cn.lxdb.plugins.muqingyu.fptoken.example;

import cn.lxdb.plugins.muqingyu.fptoken.ExclusiveFrequentItemsetSelector;
import cn.lxdb.plugins.muqingyu.fptoken.model.DocTerms;
import cn.lxdb.plugins.muqingyu.fptoken.model.ExclusiveSelectionResult;
import cn.lxdb.plugins.muqingyu.fptoken.model.SelectedGroup;
import java.util.ArrayList;
import java.util.List;

/**
 * 演示 {@link ExclusiveFrequentItemsetSelector} 的典型用法。
 *
 * <p><b>本示例在做什么</b>：
 * 构造若干 {@link DocTerms}（docId + 词），调用 {@link ExclusiveFrequentItemsetSelector#selectExclusiveBestItemsetsWithStats(List, int, int)}，
 * 打印互斥词组与 {@link ExclusiveSelectionResult} 中的统计字段。
 *
 * <p><b>其他 API</b>：
 * <ul>
 *   <li>只要词组、不要统计：{@link ExclusiveFrequentItemsetSelector#selectExclusiveBestItemsets(List, int, int)} 或
 *       {@link ExclusiveFrequentItemsetSelector#selectExclusiveBestItemsets(List, int, int, int, int)}</li>
 *   <li>需要限制最大项集长度 / 候选上限：使用五参数 {@code selectExclusiveBestItemsets(...)} / {@code selectExclusiveBestItemsetsWithStats(...)}</li>
 * </ul>
 *
 * <p><b>注意</b>：{@code String#getBytes()} 使用平台默认编码；生产环境若词为固定编码（如 UTF-8），请使用
 * {@code str.getBytes(StandardCharsets.UTF_8)} 等与索引侧一致的编码。
 *
 * @author muqingyu
 */
public final class ExclusiveFrequentItemsetSelectorUsageExample {

    public static void main(String[] args) {
        runBasicExample();
    }

    /**
     * 小数据集上的基础调用：{@code minSupport = 2}、{@code minItemsetSize = 2} 表示至少 2 个文档共现、且输出项集至少 2 个词。
     */
    private static void runBasicExample() {
        List<DocTerms> input = new ArrayList<>();
        input.add(new DocTerms(0, bytesTerms("m", "a", "b", "c", "x")));
        input.add(new DocTerms(1, bytesTerms("b", "a", "b", "c")));
        input.add(new DocTerms(2, bytesTerms("n", "a", "b", "c", "d")));
        input.add(new DocTerms(3, bytesTerms("a", "b", "c")));
        input.add(new DocTerms(4, bytesTerms("d", "e", "f")));
        input.add(new DocTerms(5, bytesTerms("d", "e", "f", "g")));
        input.add(new DocTerms(6, bytesTerms("d", "e", "f")));
        input.add(new DocTerms(7, bytesTerms("u", "v")));

        int minSupport = 2;
        int minItemsetSize = 2;

        ExclusiveSelectionResult result =
                ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(
                        input, minSupport, minItemsetSize);

        printResult("示例1：基础调用", result.getGroups());
        printStats(result);
    }

    /** 将字符串转为 {@code byte[]} 词列表（演示用）；生产侧通常直接消费字节 token。 */
    private static List<byte[]> bytesTerms(String... terms) {
        List<byte[]> out = new ArrayList<>(terms.length);
        for (String term : terms) {
            out.add(term.getBytes());
        }
        return out;
    }

    private static void printResult(String title, List<SelectedGroup> groups) {
        System.out.println();
        System.out.println("========== " + title + " ==========");
        if (groups.isEmpty()) {
            System.out.println("无结果");
            return;
        }
        for (int i = 0; i < groups.size(); i++) {
            SelectedGroup g = groups.get(i);
            System.out.println((i + 1) + ". terms=" + formatTerms(g.getTerms())
                    + ", support=" + g.getSupport()
                    + ", saving=" + g.getEstimatedSaving()
                    + ", docIds=" + g.getDocIds());
        }
    }

    private static void printStats(ExclusiveSelectionResult result) {
        System.out.println("stats: frequentTermCount=" + result.getFrequentTermCount()
                + ", candidateCount=" + result.getCandidateCount()
                + ", intersectionCount=" + result.getIntersectionCount()
                + ", maxCandidateCount=" + result.getMaxCandidateCount()
                + ", truncated=" + result.isTruncatedByCandidateLimit());
    }

    /** 仅用于控制台可读；调试二进制词请用 {@link cn.lxdb.plugins.muqingyu.fptoken.util.ByteArrayUtils#toHex(byte[])}。 */
    private static String formatTerms(List<byte[]> terms) {
        StringBuilder sb = new StringBuilder();
        sb.append('[');
        for (int i = 0; i < terms.size(); i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(new String(terms.get(i)));
        }
        sb.append(']');
        return sb.toString();
    }
}
