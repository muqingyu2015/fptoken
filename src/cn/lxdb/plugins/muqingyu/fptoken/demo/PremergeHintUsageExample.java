package cn.lxdb.plugins.muqingyu.fptoken.demo;

import cn.lxdb.plugins.muqingyu.fptoken.api.ExclusiveFpRowsProcessingApi;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.ByteRef;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.DocTerms;
import cn.lxdb.plugins.muqingyu.fptoken.runner.result.LineFileProcessingResult;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Lucene merge 场景示例：
 * 1) 先处理历史段 A/B/C，提取高频层 term 作为 premerge hints；
 * 2) 再处理合并段 D，对比 hint off / hint on 的效果。
 */
public final class PremergeHintUsageExample {

    private PremergeHintUsageExample() {
    }

    public static void main(String[] args) {
        ExclusiveFpRowsProcessingApi.ProcessingOptions options = ExclusiveFpRowsProcessingApi.defaultOptions()
                .withMinSupport(3)
                .withMinItemsetSize(2)
                .withMaxItemsetSize(6)
                .withMaxCandidateCount(200_000)
                .withHotTermThresholdExclusive(4)
                .withSampleRatio(0.45d);

        List<DocTerms> segA = buildRows(0, 800, 11);
        List<DocTerms> segB = buildRows(900, 800, 23);
        List<DocTerms> segC = buildRows(1800, 800, 37);
        List<DocTerms> mergedSegment = buildRows(3000, 2200, 91);

        List<ExclusiveFpRowsProcessingApi.PremergeHint> mutexHints = new ArrayList<>();
        List<ExclusiveFpRowsProcessingApi.PremergeHint> singleHints = new ArrayList<>();
        collectPremergeHints(options, segA, segB, segC, mutexHints, singleHints);

        // baseline: 不使用 hints
        long t0 = System.nanoTime();
        LineFileProcessingResult baseline = ExclusiveFpRowsProcessingApi.processRows(mergedSegment, options);
        long baselineMs = (System.nanoTime() - t0) / 1_000_000L;

        // hinted: 使用前置高频 hints
        ExclusiveFpRowsProcessingApi.ProcessingOptions hintedOptions = options
                .withPremergeMutexGroupHints(mutexHints)
                .withPremergeSingleTermHints(singleHints)
                .withHintBoostWeight(8)
                .withHintValidationMode(ExclusiveFpRowsProcessingApi.HintValidationMode.FILTER_ONLY);
        long t1 = System.nanoTime();
        LineFileProcessingResult hinted = ExclusiveFpRowsProcessingApi.processRows(mergedSegment, hintedOptions);
        long hintedMs = (System.nanoTime() - t1) / 1_000_000L;

        System.out.println("=== Premerge Hint Example ===");
        System.out.println("mutexHints.size=" + mutexHints.size() + ", singleHints.size=" + singleHints.size());
        System.out.println("baselineMs=" + baselineMs
                + ", baselineCandidates=" + baseline.getSelectionResult().getCandidateCount()
                + ", baselineGroups=" + baseline.getFinalIndexData().getHighFreqMutexGroupPostings().size());
        System.out.println("hintedMs=" + hintedMs
                + ", hintedCandidates=" + hinted.getSelectionResult().getCandidateCount()
                + ", hintedGroups=" + hinted.getFinalIndexData().getHighFreqMutexGroupPostings().size());
    }

    private static void collectPremergeHints(
            ExclusiveFpRowsProcessingApi.ProcessingOptions options,
            List<DocTerms> segA,
            List<DocTerms> segB,
            List<DocTerms> segC,
            List<ExclusiveFpRowsProcessingApi.PremergeHint> mutexOut,
            List<ExclusiveFpRowsProcessingApi.PremergeHint> singleOut
    ) {
        List<LineFileProcessingResult> segmentResults = Arrays.asList(
                ExclusiveFpRowsProcessingApi.processRows(segA, options),
                ExclusiveFpRowsProcessingApi.processRows(segB, options),
                ExclusiveFpRowsProcessingApi.processRows(segC, options)
        );
        for (int i = 0; i < segmentResults.size(); i++) {
            LineFileProcessingResult.FinalIndexData idx = segmentResults.get(i).getFinalIndexData();
            for (LineFileProcessingResult.HotTermDocList hot : idx.getHighFreqSingleTermPostings()) {
                byte[] term = hot.getTerm();
                singleOut.add(new ExclusiveFpRowsProcessingApi.PremergeHint(
                        Collections.singletonList(ByteRef.wrap(Arrays.copyOf(term, term.length)))));
            }
            for (int g = 0; g < idx.getHighFreqMutexGroupPostings().size(); g++) {
                List<byte[]> terms = idx.getHighFreqMutexGroupPostings().get(g).getTerms();
                List<ByteRef> refs = new ArrayList<>(terms.size());
                for (int t = 0; t < terms.size(); t++) {
                    refs.add(ByteRef.wrap(Arrays.copyOf(terms.get(t), terms.get(t).length)));
                }
                mutexOut.add(new ExclusiveFpRowsProcessingApi.PremergeHint(refs));
            }
        }
    }

    private static List<DocTerms> buildRows(int docIdStart, int count, int seed) {
        List<DocTerms> out = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            int docId = docIdStart + i;
            List<ByteRef> terms = new ArrayList<>();
            terms.add(ref("K" + (i % 20)));
            terms.add(ref("V" + ((i / 2 + seed) % 10)));
            if (i % 3 == 0) {
                terms.add(ref("ANCHOR_A"));
                terms.add(ref("ANCHOR_B"));
            }
            terms.add(ref("N" + ((i + seed) % 37)));
            if (i % 11 == 0) {
                terms.add(ref("TAIL_X"));
            }
            out.add(new DocTerms(docId, terms));
        }
        return out;
    }

    private static ByteRef ref(String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        return new ByteRef(bytes, 0, bytes.length);
    }
}
