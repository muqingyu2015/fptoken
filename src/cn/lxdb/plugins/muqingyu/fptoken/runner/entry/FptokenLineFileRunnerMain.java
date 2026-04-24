package cn.lxdb.plugins.muqingyu.fptoken.runner.entry;

import cn.lxdb.plugins.muqingyu.fptoken.api.ExclusiveFpRowsProcessingApi;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.config.EngineTuningConfig;
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
 *
 * <p><b>命令行参数顺序</b>：
 * {@code inputFile ngramStart ngramEnd minSupport minItemsetSize hotTermThresholdExclusive}</p>
 *
 * <p><b>限制说明</b>：
 * 输入文件按 {@link LineRecordDatasetLoader} 的上限规则处理（每文件最多 32000 行、每行最多 64B）。</p>
 */
public final class FptokenLineFileRunnerMain {

    private static final String DEFAULT_DATA_DIR = "sample-data/line-records";
    private static final String DEFAULT_INPUT_FILE = "records_001_small.txt";
    private static final int DEFAULT_NGRAM_START = EngineTuningConfig.DEFAULT_NGRAM_START;
    private static final int DEFAULT_NGRAM_END = EngineTuningConfig.DEFAULT_NGRAM_END;
    private static final int DEFAULT_HOT_TERM_THRESHOLD_EXCLUSIVE =
            EngineTuningConfig.DEFAULT_HOT_TERM_THRESHOLD_EXCLUSIVE;
    private static final int DEFAULT_MIN_SUPPORT = EngineTuningConfig.DEFAULT_RUNNER_MIN_SUPPORT;
    private static final int DEFAULT_MIN_ITEMSET_SIZE = EngineTuningConfig.DEFAULT_RUNNER_MIN_ITEMSET_SIZE;
    // Heuristic guardrails for hint-effect diagnosis in console output.
    private static final double HINT_MERGE_COST_PERCENT_WARN = 25.0d;
    private static final double LOW_SAVING_PERCENT_WARN = 55.0d;
    private static final double MAPPED_HINT_RATIO_WARN = 20.0d;

    private FptokenLineFileRunnerMain() {
    }

    public static void run(String[] args) throws Exception {
        Path dataDir = Paths.get(DEFAULT_DATA_DIR);
        // 参数顺序与入口约定保持一致，避免外部脚本调用时出现语义漂移。
        Path inputFile = args.length > 0 ? Paths.get(args[0]) : dataDir.resolve(DEFAULT_INPUT_FILE);
        int ngramStart = args.length > 1 ? Integer.parseInt(args[1]) : DEFAULT_NGRAM_START;
        int ngramEnd = args.length > 2 ? Integer.parseInt(args[2]) : DEFAULT_NGRAM_END;
        int minSupport = args.length > 3 ? Integer.parseInt(args[3]) : DEFAULT_MIN_SUPPORT;
        int minItemsetSize = args.length > 4 ? Integer.parseInt(args[4]) : DEFAULT_MIN_ITEMSET_SIZE;
        int hotTermThresholdExclusive =
                args.length > 5 ? Integer.parseInt(args[5]) : DEFAULT_HOT_TERM_THRESHOLD_EXCLUSIVE;

        // 文件读写层：仅生成 loaded(rows + stats)
        LineRecordDatasetLoader.LoadOutcome loadedOutcome =
                loadRowsFromFile(dataDir, inputFile);
        LineRecordDatasetLoader.LoadedDataset loaded = loadedOutcome.getLoadedDataset();
        // 处理层：仅基于 rows，供 LXDB API 复用
        LineFileProcessingResult processing =
                ExclusiveFpRowsProcessingApi.processRowsWithNgram(
                        loaded.getRows(), ngramStart, ngramEnd, minSupport, minItemsetSize, hotTermThresholdExclusive);
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
        LineFileProcessingResult.ProcessingStats statsView = processing.getProcessingStats();

        System.out.println();
        System.out.println("=== Mining Stats ===");
        System.out.println("frequentTermCount=" + statsView.getFrequentTermCount());
        System.out.println("candidateCount=" + statsView.getCandidateCount());
        System.out.println("intersectionCount=" + statsView.getIntersectionCount());
        System.out.println("truncatedByLimit=" + statsView.isTruncatedByCandidateLimit());
        System.out.println("selectedGroups=" + statsView.getSelectedGroupCount());
        System.out.println("selectedGroupTotalSupport=" + statsView.getSelectedGroupTotalSupport());
        System.out.println("selectedGroupTotalEstimatedSaving=" + statsView.getSelectedGroupTotalEstimatedSaving());
        System.out.println("selectedGroupAverageEstimatedSaving=" + statsView.getSelectedGroupAverageEstimatedSaving());
        System.out.println("selectedGroupLowSavingPercent=" + statsView.getSelectedGroupLowSavingPercent());
        System.out.println("premergeHintInputCount=" + statsView.getPremergeHintInputCount());
        System.out.println("mappedHintCandidateCount=" + statsView.getMappedHintCandidateCount());
        System.out.println("mergedDistinctCandidateCount=" + statsView.getMergedDistinctCandidateCount());
        System.out.println("hintMergeMs=" + statsView.getHintMergeMs());
        System.out.println("totalPipelineMs=" + statsView.getTotalPipelineMs());
        System.out.println("hintMergeMsPercent=" + statsView.getHintMergeMsPercent());
        printHintEffectAssessment(statsView);

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
        // 约束：调用方应保证 inputFile 可读、ngram 区间与 support 参数合法。
        LineRecordDatasetLoader.LoadOutcome loaded =
                loadRowsFromFile(dataDir, inputFile);
        return ExclusiveFpRowsProcessingApi.processRowsWithNgram(
                loaded.getLoadedDataset().getRows(),
                ngramStart, ngramEnd, minSupport, minItemsetSize, hotTermThresholdExclusive);
    }

    private static LineRecordDatasetLoader.LoadOutcome loadRowsFromFile(
            Path dataDir, Path inputFile
    ) throws Exception {
        // 保持 runner 的开箱即用：没有样例文件时自动补齐。
        SampleDataPreparer.ensureSampleFiles(dataDir);
        return LineRecordDatasetLoader.loadSingleFileRawWithStats(inputFile);
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

    private static void printHintEffectAssessment(LineFileProcessingResult.ProcessingStats statsView) {
        int hintInput = statsView.getPremergeHintInputCount();
        if (hintInput <= 0) {
            System.out.println("hintEffectAssessment=NO_HINTS");
            return;
        }
        double mappedRatio = percent(statsView.getMappedHintCandidateCount(), hintInput);
        boolean highMergeCost = statsView.getHintMergeMsPercent() >= HINT_MERGE_COST_PERCENT_WARN;
        boolean highLowSaving = statsView.getSelectedGroupLowSavingPercent() >= LOW_SAVING_PERCENT_WARN;
        boolean lowHintMapping = mappedRatio <= MAPPED_HINT_RATIO_WARN;
        String assessment;
        String action;
        if (highMergeCost && (highLowSaving || lowHintMapping)) {
            assessment = "DEGRADE_RISK";
            action = "建议先降低 hintBoostWeight 或关闭 single-term hints，再观察";
        } else if (highLowSaving || lowHintMapping) {
            assessment = "NEED_TUNING";
            action = "建议优化 hints 质量（清理过时提示、提升 mutex hints 占比）";
        } else {
            assessment = "HEALTHY";
            action = "提示效果稳定，可继续按当前配置运行";
        }
        System.out.println("hintMappedRatioPercent=" + mappedRatio);
        System.out.println("hintEffectAssessment=" + assessment);
        System.out.println("hintSuggestedAction=" + action);
    }

    private static double percent(int numerator, int denominator) {
        if (denominator <= 0) {
            return 0d;
        }
        return (numerator * 100.0d) / denominator;
    }
}
