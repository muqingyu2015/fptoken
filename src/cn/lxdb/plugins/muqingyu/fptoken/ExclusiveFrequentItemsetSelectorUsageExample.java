package cn.lxdb.plugins.muqingyu.fptoken;

import java.util.ArrayList;
import java.util.List;

/**
 * 作者：muqingyu
 *
 * 用法示例类：
 * - 示例 1：基础调用（最少参数）
 * - 示例 2：性能参数调优调用（最大项集长度、候选上限）
 * - 示例 3：非 ASCII 二进制词输入
 */
public class ExclusiveFrequentItemsetSelectorUsageExample {

    public static void main(String[] args) {
        runBasicExample();
        runTuningExample();
        runBinaryTokenExample();
    }

    /**
     * 示例 1：最基础的调用方式。
     */
    private static void runBasicExample() {
        List<DocTerms> input = new ArrayList<DocTerms>();
        input.add(new DocTerms(1, bytesTerms("a", "b", "c", "x")));
        input.add(new DocTerms(2, bytesTerms("a", "b", "c")));
        input.add(new DocTerms(3, bytesTerms("a", "b", "c", "d")));
        input.add(new DocTerms(4, bytesTerms("a", "b", "c")));
        input.add(new DocTerms(5, bytesTerms("d", "e", "f")));
        input.add(new DocTerms(6, bytesTerms("d", "e", "f", "g")));
        input.add(new DocTerms(7, bytesTerms("d", "e", "f")));
        input.add(new DocTerms(8, bytesTerms("u", "v")));

        int minSupport = 2;
        int minItemsetSize = 2;

        List<SelectedGroup> result =
                ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsets(
                        input, minSupport, minItemsetSize);

        printResult("示例1：基础调用", result);
    }

    /**
     * 示例 2：带性能调优参数的调用方式。
     * 适用于数据量较大时的“速度优先”场景。
     */
    private static void runTuningExample() {
        List<DocTerms> input = new ArrayList<DocTerms>();
        input.add(new DocTerms(101, bytesTerms("k1", "k2", "k3", "k9")));
        input.add(new DocTerms(102, bytesTerms("k1", "k2", "k3")));
        input.add(new DocTerms(103, bytesTerms("k1", "k2", "k3", "k4")));
        input.add(new DocTerms(104, bytesTerms("k4", "k5", "k6")));
        input.add(new DocTerms(105, bytesTerms("k4", "k5", "k6")));
        input.add(new DocTerms(106, bytesTerms("k4", "k5", "k6", "k7")));
        input.add(new DocTerms(107, bytesTerms("k8", "k9")));

        int minSupport = 2;
        int minItemsetSize = 2;
        int maxItemsetSize = 4;      // 限制组合长度，降低挖掘开销
        int maxCandidateCount = 8000; // 候选硬上限，防止爆炸

        List<SelectedGroup> result =
                ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsets(
                        input, minSupport, minItemsetSize, maxItemsetSize, maxCandidateCount);

        printResult("示例2：性能参数调优", result);
    }

    /**
     * 示例 3：演示词为纯二进制内容（不依赖字符串语义）。
     */
    private static void runBinaryTokenExample() {
        List<DocTerms> input = new ArrayList<DocTerms>();
        input.add(new DocTerms(201, asList(
                bytes(0x01, 0x10), bytes(0x02, 0x20), bytes(0x03, 0x30))));
        input.add(new DocTerms(202, asList(
                bytes(0x01, 0x10), bytes(0x02, 0x20), bytes(0x03, 0x30))));
        input.add(new DocTerms(203, asList(
                bytes(0x01, 0x10), bytes(0x02, 0x20), bytes(0x03, 0x30), bytes(0x09, 0x09))));
        input.add(new DocTerms(204, asList(
                bytes(0x0A, 0x0B), bytes(0x0A, 0x0C), bytes(0x0A, 0x0D))));
        input.add(new DocTerms(205, asList(
                bytes(0x0A, 0x0B), bytes(0x0A, 0x0C), bytes(0x0A, 0x0D))));

        List<SelectedGroup> result =
                ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsets(
                        input, 2, 2, 4, 5000);

        printResult("示例3：纯二进制词输入", result);
    }

    private static List<byte[]> bytesTerms(String... terms) {
        List<byte[]> out = new ArrayList<byte[]>(terms.length);
        for (String term : terms) {
            out.add(term.getBytes());
        }
        return out;
    }

    private static List<byte[]> asList(byte[]... arr) {
        List<byte[]> out = new ArrayList<byte[]>(arr.length);
        for (byte[] b : arr) {
            out.add(b);
        }
        return out;
    }

    private static byte[] bytes(int... values) {
        byte[] out = new byte[values.length];
        for (int i = 0; i < values.length; i++) {
            out[i] = (byte) (values[i] & 0xFF);
        }
        return out;
    }

    private static void printResult(
            String title,
            List<SelectedGroup> groups
    ) {
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

    private static String formatTerms(List<byte[]> terms) {
        StringBuilder sb = new StringBuilder();
        sb.append('[');
        for (int i = 0; i < terms.size(); i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(toHex(terms.get(i)));
        }
        sb.append(']');
        return sb.toString();
    }

    private static String toHex(byte[] bytes) {
        final char[] hex = "0123456789ABCDEF".toCharArray();
        char[] out = new char[bytes.length * 2];
        int p = 0;
        for (byte b : bytes) {
            int v = b & 0xFF;
            out[p++] = hex[v >>> 4];
            out[p++] = hex[v & 0x0F];
        }
        return new String(out);
    }
}
