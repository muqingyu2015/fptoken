package cn.lxdb.plugins.muqingyu.fptoken.runner.entry;

import cn.lxdb.plugins.muqingyu.fptoken.ExclusiveFrequentItemsetSelector;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.ExclusiveSelectionResult;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.SelectedGroup;
import cn.lxdb.plugins.muqingyu.fptoken.runner.dataset.LineRecordDatasetLoader;
import cn.lxdb.plugins.muqingyu.fptoken.runner.dataset.SampleDataPreparer;
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

    private FptokenLineFileRunnerMain() {
    }

    public static void run(String[] args) throws Exception {
        Path dataDir = Paths.get(DEFAULT_DATA_DIR);
        Path inputFile = args.length > 0 ? Paths.get(args[0]) : dataDir.resolve(DEFAULT_INPUT_FILE);
        int ngramStart = args.length > 1 ? Integer.parseInt(args[1]) : DEFAULT_NGRAM_START;
        int ngramEnd = args.length > 2 ? Integer.parseInt(args[2]) : DEFAULT_NGRAM_END;
        int minSupport = args.length > 3 ? Integer.parseInt(args[3]) : 50;
        int minItemsetSize = args.length > 4 ? Integer.parseInt(args[4]) : 2;

        // 代码内配置抽样参数（不依赖 properties 文件）
        ExclusiveFrequentItemsetSelector.setSamplingEnabled(true);
        ExclusiveFrequentItemsetSelector.setSampleRatio(0.30d);
        ExclusiveFrequentItemsetSelector.setMinSampleCount(50);
        ExclusiveFrequentItemsetSelector.setSamplingSupportScale(0.0d);

        SampleDataPreparer.ensureSampleFiles(dataDir);
        LineRecordDatasetLoader.LoadedDataset loaded =
                LineRecordDatasetLoader.loadSingleFile(inputFile, ngramStart, ngramEnd);
        LineRecordDatasetLoader.Stats stats = loaded.getStats();

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

        ExclusiveSelectionResult result = ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(
                loaded.getRows(), minSupport, minItemsetSize, 6, 200000);

        System.out.println();
        System.out.println("=== Mining Stats ===");
        System.out.println("frequentTermCount=" + result.getFrequentTermCount());
        System.out.println("candidateCount=" + result.getCandidateCount());
        System.out.println("intersectionCount=" + result.getIntersectionCount());
        System.out.println("truncatedByLimit=" + result.isTruncatedByCandidateLimit());
        System.out.println("selectedGroups=" + result.getGroups().size());

        printTopGroups(result.getGroups(), 20);
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
