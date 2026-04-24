package cn.lxdb.plugins.muqingyu.fptoken.tests.support;

import cn.lxdb.plugins.muqingyu.fptoken.api.ExclusiveFpRowsProcessingApi;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.ByteRef;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.DocTerms;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.SelectedGroup;
import cn.lxdb.plugins.muqingyu.fptoken.runner.result.LineFileProcessingResult;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * 基于 {@code sample-data} 行样例构造「历史三段 + merge 尾段」与预合并 hints 的测试辅助。
 *
 * <p>与 {@link cn.lxdb.plugins.muqingyu.fptoken.runner.dataset.LineRecordDatasetLoader} 的 RAW / TOKENIZED
 * 加载方式配合使用；切分比例为前 3/5 均分为 A/B/C，后 2/5 为 merge 段（docId 各自从 0 重排）。</p>
 */
public final class SampleDataPremergeHintTestSupport {

    public static final Path LINE_RECORDS_002 =
            Paths.get("sample-data/line-records/records_002_medium.txt");
    public static final Path LINE_RECORDS_004 =
            Paths.get("sample-data/line-records/records_004_limit32000.txt");
    public static final Path REAL_DOCS_AWESOME_PYTHON =
            Paths.get("sample-data/real-docs/awesome_python_lines.txt");

    private SampleDataPremergeHintTestSupport() {
    }

    /**
     * merge 场景常用参数：与合成 merge 性能/功能用例对齐，便于在真实样例上对比行为。
     */
    public static ExclusiveFpRowsProcessingApi.ProcessingOptions mergeScenarioBaseOptions() {
        return ExclusiveFpRowsProcessingApi.defaultOptions()
                .withMinSupport(3)
                .withMinItemsetSize(2)
                .withMaxItemsetSize(6)
                .withMaxCandidateCount(220_000)
                .withHotTermThresholdExclusive(4)
                .withSampleRatio(0.45d)
                .withHintBoostWeight(0);
    }

    /**
     * 将已加载的 rows 按 3×(N/5) + 2N/5 切分为 A、B、C 与 merge 尾段。
     *
     * @param allRows 不可为 null；若行数过少调用方应先跳过
     */
    public static FiveWaySplit splitFiveWay(List<DocTerms> allRows) {
        int n = allRows.size();
        int segSize = Math.max(1, n / 5);
        List<DocTerms> segA = sliceRenumber(allRows, 0, segSize);
        List<DocTerms> segB = sliceRenumber(allRows, segSize, segSize * 2);
        List<DocTerms> segC = sliceRenumber(allRows, segSize * 2, segSize * 3);
        List<DocTerms> merged = sliceRenumber(allRows, segSize * 3, n);
        return new FiveWaySplit(n, segSize, segA, segB, segC, merged);
    }

    /**
     * 对 A/B/C 各跑一遍 {@link ExclusiveFpRowsProcessingApi#processRows}，从高频层抽取 mutex / single 提示
     * （语义与合成 merge 用例一致：首段 mutex 提示重复一条以模拟上游重复投递）。
     */
    public static HintsFromSegments buildHintsFromSegments(
            ExclusiveFpRowsProcessingApi.ProcessingOptions options,
            List<DocTerms> segA,
            List<DocTerms> segB,
            List<DocTerms> segC,
            boolean includeMutexGroupPostings,
            boolean includeSingleTermPostings
    ) {
        List<ExclusiveFpRowsProcessingApi.PremergeHint> mutexOut = new ArrayList<>();
        List<ExclusiveFpRowsProcessingApi.PremergeHint> singleOut = new ArrayList<>();
        List<LineFileProcessingResult> results = Arrays.asList(
                ExclusiveFpRowsProcessingApi.processRows(segA, options),
                ExclusiveFpRowsProcessingApi.processRows(segB, options),
                ExclusiveFpRowsProcessingApi.processRows(segC, options)
        );
        for (int i = 0; i < results.size(); i++) {
            LineFileProcessingResult.FinalIndexData idx = results.get(i).getFinalIndexData();
            if (includeMutexGroupPostings) {
                for (SelectedGroup g : idx.getHighFreqMutexGroupPostings()) {
                    List<ByteRef> refs = new ArrayList<>();
                    for (byte[] t : g.getTerms()) {
                        refs.add(ByteRef.wrap(Arrays.copyOf(t, t.length)));
                    }
                    mutexOut.add(new ExclusiveFpRowsProcessingApi.PremergeHint(refs));
                    if (i == 0) {
                        mutexOut.add(new ExclusiveFpRowsProcessingApi.PremergeHint(refs));
                    }
                }
            }
            if (includeSingleTermPostings) {
                for (LineFileProcessingResult.HotTermDocList hot : idx.getHighFreqSingleTermPostings()) {
                    byte[] term = hot.getTerm();
                    singleOut.add(new ExclusiveFpRowsProcessingApi.PremergeHint(
                            Collections.singletonList(ByteRef.wrap(Arrays.copyOf(term, term.length)))));
                }
            }
        }
        return new HintsFromSegments(mutexOut, singleOut);
    }

    public static List<DocTerms> sliceRenumber(List<DocTerms> rows, int fromInclusive, int toExclusive) {
        List<DocTerms> out = new ArrayList<>(Math.max(0, toExclusive - fromInclusive));
        int id = 0;
        for (int i = fromInclusive; i < toExclusive; i++) {
            DocTerms r = rows.get(i);
            out.add(new DocTerms(id++, r.getTermRefsUnsafe()));
        }
        return out;
    }

    public static final class FiveWaySplit {
        public final int totalRows;
        public final int segSize;
        public final List<DocTerms> segA;
        public final List<DocTerms> segB;
        public final List<DocTerms> segC;
        public final List<DocTerms> merged;

        private FiveWaySplit(
                int totalRows,
                int segSize,
                List<DocTerms> segA,
                List<DocTerms> segB,
                List<DocTerms> segC,
                List<DocTerms> merged
        ) {
            this.totalRows = totalRows;
            this.segSize = segSize;
            this.segA = segA;
            this.segB = segB;
            this.segC = segC;
            this.merged = merged;
        }
    }

    public static final class HintsFromSegments {
        public final List<ExclusiveFpRowsProcessingApi.PremergeHint> mutexGroupHints;
        public final List<ExclusiveFpRowsProcessingApi.PremergeHint> singleTermHints;

        public HintsFromSegments(
                List<ExclusiveFpRowsProcessingApi.PremergeHint> mutexGroupHints,
                List<ExclusiveFpRowsProcessingApi.PremergeHint> singleTermHints
        ) {
            this.mutexGroupHints = mutexGroupHints;
            this.singleTermHints = singleTermHints;
        }
    }
}
