package cn.lxdb.plugins.muqingyu.fptoken.runner.entry;

import cn.lxdb.plugins.muqingyu.fptoken.api.ExclusiveFpRowsProcessingApi;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.ExclusiveSelectionResult;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.SelectedGroup;
import cn.lxdb.plugins.muqingyu.fptoken.runner.dataset.LineRecordDatasetLoader;
import cn.lxdb.plugins.muqingyu.fptoken.runner.dataset.SampleDataPreparer;
import cn.lxdb.plugins.muqingyu.fptoken.runner.result.LineFileProcessingResult;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.util.ByteArrayUtils;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

/**
 * 文件行输入运行器（实现类）。
 */
public final class FptokenLineFileRunnerMain {

    private static final String DEFAULT_DATA_DIR = "sample-data/line-records";
    private static final String DEFAULT_INPUT_FILE = "records_001_small.txt";
    private static final int DEFAULT_NGRAM_START = 2;
    private static final int DEFAULT_NGRAM_END = 4;
    private static final int DEFAULT_HOT_TERM_THRESHOLD_EXCLUSIVE = 10;

    private FptokenLineFileRunnerMain() {
    }

    public static void run(String[] args) throws Exception {
        Path dataDir = Paths.get(DEFAULT_DATA_DIR);
        // 参数顺序与入口约定保持一致，避免外部脚本调用时出现语义漂移。
        Path inputFile = args.length > 0 ? Paths.get(args[0]) : dataDir.resolve(DEFAULT_INPUT_FILE);
        int ngramStart = args.length > 1 ? Integer.parseInt(args[1]) : DEFAULT_NGRAM_START;
        int ngramEnd = args.length > 2 ? Integer.parseInt(args[2]) : DEFAULT_NGRAM_END;
        int minSupport = args.length > 3 ? Integer.parseInt(args[3]) : 50;
        int minItemsetSize = args.length > 4 ? Integer.parseInt(args[4]) : 2;
        int hotTermThresholdExclusive =
                args.length > 5 ? Integer.parseInt(args[5]) : DEFAULT_HOT_TERM_THRESHOLD_EXCLUSIVE;

        // 文件读写层：仅生成 loaded(rows + stats)
        LineRecordDatasetLoader.LoadOutcome loadedOutcome =
                loadRowsFromFile(dataDir, inputFile, ngramStart, ngramEnd);
        LineRecordDatasetLoader.LoadedDataset loaded = loadedOutcome.getLoadedDataset();
        // 处理层：仅基于 rows，供 LXDB API 复用
        LineFileProcessingResult processing =
                ExclusiveFpRowsProcessingApi.processRows(
                        loaded.getRows(), minSupport, minItemsetSize, hotTermThresholdExclusive);
        LineRecordDatasetLoader.Stats stats = loadedOutcome.getStats();

        System.out.println("=== Input Summary ===");
        System.out.println("inputFile=" + inputFile.toAbsolutePath());
        System.out.println("files=" + stats.getFileCount()
                + ", lines=" + stats.getTotalLines()
                + ", docs=" + stats.getDocCount()
                + ", emptyLines=" + stats.getEmptyLines()
                + ", truncatedLines(>64B)=" + stats.getTruncatedLines()
                + ", droppedBy32000Cap=" + stats.getDroppedByCap());
        System.out.println("ngramRange=[" + ngramStart + "," + ngramEnd + "]");

        if (loaded.getRows().isEmpty()) {
            System.out.println("No rows loaded, stop.");
            return;
        }

        // 统一业务返回：高频倒排层(groups + hotTerms) + 低频正排层(cutRes)。
        LineFileProcessingResult.FinalIndexData finalIndexData = processing.getFinalIndexData();
        ExclusiveSelectionResult result = processing.getSelectionResult();

        System.out.println();
        System.out.println("=== Mining Stats ===");
        System.out.println("frequentTermCount=" + result.getFrequentTermCount());
        System.out.println("candidateCount=" + result.getCandidateCount());
        System.out.println("intersectionCount=" + result.getIntersectionCount());
        System.out.println("truncatedByLimit=" + result.isTruncatedByCandidateLimit());
        System.out.println("selectedGroups=" + result.getGroups().size());

        System.out.println();
        System.out.println("=== Derived Data Stats ===");
        System.out.println("hotTermThresholdExclusive(keep count > xxx): "
                + finalIndexData.getHotTermThresholdExclusive());
        System.out.println("highFreqMutexGroupPostings(inverted)="
                + finalIndexData.getHighFreqMutexGroupPostings().size());
        System.out.println("highFreqSingleTermPostings(inverted)="
                + finalIndexData.getHighFreqSingleTermPostings().size());
        System.out.println("lowHitForwardRows(skip-index source)="
                + finalIndexData.getLowHitForwardRows().size());

        printTopGroups(finalIndexData.getHighFreqMutexGroupPostings(), 20);
    }

    public static LineFileProcessingResult runPipeline(
            Path dataDir,
            Path inputFile,
            int ngramStart,
            int ngramEnd,
            int minSupport,
            int minItemsetSize,
            int hotTermThresholdExclusive
    ) throws Exception {
        // 这个方法用于测试与外部调用：返回结构化结果，不负责控制台打印。
        LineRecordDatasetLoader.LoadOutcome loaded =
                loadRowsFromFile(dataDir, inputFile, ngramStart, ngramEnd);
        return ExclusiveFpRowsProcessingApi.processRows(
                loaded.getLoadedDataset().getRows(), minSupport, minItemsetSize, hotTermThresholdExclusive);
    }

    private static LineRecordDatasetLoader.LoadOutcome loadRowsFromFile(
            Path dataDir, Path inputFile, int ngramStart, int ngramEnd
    ) throws Exception {
        // 保持 runner 的开箱即用：没有样例文件时自动补齐。
        SampleDataPreparer.ensureSampleFiles(dataDir);
        return LineRecordDatasetLoader.loadSingleFileWithStats(inputFile, ngramStart, ngramEnd);
    }

    private static void printTopGroups(List<SelectedGroup> groups, int limit) {
        System.out.println();
        System.out.println("=== Top Groups ===");
        if (groups.isEmpty()) {
            System.out.println("(empty)");
            return;
        }
        int bound = Math.min(limit, groups.size());
        for (int i = 0; i < bound; i++) {
            SelectedGroup g = groups.get(i);
            System.out.println("#" + (i + 1)
                    + " len=" + g.getTerms().size()
                    + " support=" + g.getSupport()
                    + " saving=" + g.getEstimatedSaving()
                    + " terms=" + toHexTerms(g)
                    + " docIdsSize=" + g.getDocIds().size());
        }
    }

    private static String toHexTerms(SelectedGroup group) {
        StringBuilder sb = new StringBuilder();
        sb.append('[');
        for (int i = 0; i < group.getTerms().size(); i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(ByteArrayUtils.toHex(group.getTerms().get(i)));
        }
        sb.append(']');
        return sb.toString();
    }
}
