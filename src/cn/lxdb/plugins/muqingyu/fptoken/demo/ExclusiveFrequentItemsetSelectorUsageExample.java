package cn.lxdb.plugins.muqingyu.fptoken.demo;

import cn.lxdb.plugins.muqingyu.fptoken.ExclusiveFrequentItemsetSelector;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.config.EngineTuningConfig;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.ByteRef;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.DocTerms;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.ExclusiveSelectionResult;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.SelectedGroup;
import java.util.ArrayList;
import java.util.Arrays;
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
        runSamplingTuningExample();
        runBasicExample();
        runPcapLikeExample();
    }

    /**
     * 采样参数设置示例：
     * <ul>
     *   <li>代码内直接设置采样比例 / 最小样本数 / support 缩放。</li>
     * </ul>
     */
    private static void runSamplingTuningExample() {
        double oldRatio = ExclusiveFrequentItemsetSelector.getSampleRatio();
        int oldMinSample = ExclusiveFrequentItemsetSelector.getMinSampleCount();
        double oldScale = ExclusiveFrequentItemsetSelector.getSamplingSupportScale();
        try {
            ExclusiveFrequentItemsetSelector.setSampleRatio(EngineTuningConfig.DEFAULT_SAMPLE_RATIO);
            ExclusiveFrequentItemsetSelector.setMinSampleCount(EngineTuningConfig.DEFAULT_MIN_SAMPLE_COUNT);
            // 0.0 表示按实际样本占比自动缩放 minSupport
            ExclusiveFrequentItemsetSelector.setSamplingSupportScale(
                    EngineTuningConfig.DEFAULT_SAMPLING_SUPPORT_SCALE);

            List<DocTerms> rows = buildSamplingDemoRows(320);
            ExclusiveSelectionResult result =
                    ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(
                            rows, 40, 2, 4, 100_000);
            printResult("示例0：采样参数设置与调用", result.getGroups());
            printStats(result);
        } finally {
            // 示例结束后恢复，避免影响其他调用。
            ExclusiveFrequentItemsetSelector.setSampleRatio(oldRatio);
            ExclusiveFrequentItemsetSelector.setMinSampleCount(oldMinSample);
            ExclusiveFrequentItemsetSelector.setSamplingSupportScale(oldScale);
        }
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

    /**
     * 业务化示例：
     * <ul>
     *   <li>模拟若干条“PCAP payload 记录”（HTTP 风格头 + 可变尾部）。</li>
     *   <li>每条记录按固定窗口切片（windowLen/windowStep）。</li>
     *   <li>每个窗口提取 1/2/3 字节 token，映射为一个 {@link DocTerms}。</li>
     *   <li>调用带统计接口，观察候选规模、是否截断、最终互斥词组。</li>
     * </ul>
     */
    private static void runPcapLikeExample() {
        int windowLen = 16;
        int windowStep = 8;
        List<DocTerms> rows = buildPcapLikeRows(windowLen, windowStep);

        int minSupport = 3;
        int minItemsetSize = 2;
        int maxItemsetSize = 6;
        int maxCandidateCount = 30_000;

        ExclusiveSelectionResult result =
                ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(
                        rows, minSupport, minItemsetSize, maxItemsetSize, maxCandidateCount);

        printResultHex("示例2：PCAP滑窗 1/2/3-byte token", result.getGroups());
        printStats(result);
    }

    /** 将字符串转为 {@code byte[]} 词列表（演示用）；生产侧通常直接消费字节 token。 */
    private static List<ByteRef> bytesTerms(String... terms) {
        List<ByteRef> out = new ArrayList<>(terms.length);
        for (String term : terms) {
            byte[] bytes = term.getBytes();
            out.add(ByteRef.wrap(bytes));
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

    private static void printResultHex(String title, List<SelectedGroup> groups) {
        System.out.println();
        System.out.println("========== " + title + " ==========");
        if (groups.isEmpty()) {
            System.out.println("无结果");
            return;
        }
        for (int i = 0; i < groups.size(); i++) {
            SelectedGroup g = groups.get(i);
            System.out.println((i + 1) + ". termsHex=" + formatTerms(g.getTerms())
                    + ", support=" + g.getSupport()
                    + ", saving=" + g.getEstimatedSaving()
                    + ", docIds=" + g.getDocIds());
        }
    }

    /** 仅用于控制台可读；调试二进制词请用 {@link cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.util.ByteArrayUtils#toHex(byte[])}。 */
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

    private static List<DocTerms> buildPcapLikeRows(int windowLen, int windowStep) {
        List<byte[]> payloads = new ArrayList<>();
        payloads.add(payload(
                "GET /a HTTP/1.1\r\nHost:demo\r\n",
                new byte[] {0x11, 0x22, 0x33, 0x44, 0x55}));
        payloads.add(payload(
                "GET /b HTTP/1.1\r\nHost:demo\r\n",
                new byte[] {0x11, 0x22, 0x66, 0x77, 0x55}));
        payloads.add(payload(
                "POST /x HTTP/1.1\r\nHost:demo\r\n",
                new byte[] {0x11, 0x22, 0x33, 0x44, 0x21}));
        payloads.add(payload(
                "GET /c HTTP/1.1\r\nHost:demo\r\n",
                new byte[] {0x11, 0x22, 0x33, 0x44, 0x31}));

        List<DocTerms> rows = new ArrayList<>();
        int docId = 0;
        for (byte[] bytes : payloads) {
            for (int off = 0; off + windowLen <= bytes.length; off += windowStep) {
                byte[] window = Arrays.copyOfRange(bytes, off, off + windowLen);
                rows.add(new DocTerms(docId++, slidingItems123(window)));
            }
        }
        return rows;
    }

    private static List<ByteRef> slidingItems123(byte[] window) {
        List<ByteRef> out = new ArrayList<>();
        for (int i = 0; i < window.length; i++) {
            out.add(new ByteRef(window, i, 1));
            if (i + 2 <= window.length) {
                out.add(new ByteRef(window, i, 2));
            }
            if (i + 3 <= window.length) {
                out.add(new ByteRef(window, i, 3));
            }
        }
        return out;
    }

    private static byte[] payload(String header, byte[] tail) {
        byte[] h = header.getBytes();
        byte[] out = new byte[h.length + tail.length];
        System.arraycopy(h, 0, out, 0, h.length);
        System.arraycopy(tail, 0, out, h.length, tail.length);
        return out;
    }

    private static List<DocTerms> buildSamplingDemoRows(int docCount) {
        List<DocTerms> rows = new ArrayList<>(docCount);
        for (int i = 0; i < docCount; i++) {
            List<ByteRef> terms = new ArrayList<>();
            terms.add(ByteRef.wrap("coreA".getBytes()));
            terms.add(ByteRef.wrap("coreB".getBytes()));
            if (i % 2 == 0) {
                terms.add(ByteRef.wrap("even".getBytes()));
            }
            if (i % 3 == 0) {
                terms.add(ByteRef.wrap("mod3".getBytes()));
            }
            if (i % 5 == 0) {
                terms.add(ByteRef.wrap("mod5".getBytes()));
            }
            rows.add(new DocTerms(i, terms));
        }
        return rows;
    }
}
