package cn.lxdb.plugins.muqingyu.fptoken.tests.unit;

import static org.junit.jupiter.api.Assertions.assertTrue;

import cn.lxdb.plugins.muqingyu.fptoken.api.ExclusiveFpRowsProcessingApi;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.ByteRef;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.DocTerms;
import cn.lxdb.plugins.muqingyu.fptoken.runner.result.LineFileProcessingResult;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * 内存泄漏守护测试：同进程重复调用后，历史结果应可回收。
 */
class MemoryLeakGuardUnitTest {

    @Test
    @Timeout(20)
    void processRows_repeatedCallsInSameProcess_shouldNotRetainHistoricalResults() {
        ExclusiveFpRowsProcessingApi.IntermediateSteps.clearPremergeHintAggregateCache();
        List<DocTerms> rows = buildRows(300);
        ExclusiveFpRowsProcessingApi.ProcessingOptions options = ExclusiveFpRowsProcessingApi.defaultOptions()
                .withMinSupport(3)
                .withMinItemsetSize(2)
                .withMaxItemsetSize(5)
                .withMaxCandidateCount(120_000)
                .withSampleRatio(0.45d);

        List<WeakReference<LineFileProcessingResult>> refs = new ArrayList<WeakReference<LineFileProcessingResult>>();
        for (int i = 0; i < 140; i++) {
            LineFileProcessingResult result = ExclusiveFpRowsProcessingApi.processRows(rows, options);
            refs.add(new WeakReference<LineFileProcessingResult>(result));
        }

        int cleared = forceGcAndCountCleared(refs);
        double clearedRatio = (cleared * 100.0d) / refs.size();
        assertTrue(
                clearedRatio >= 95.0d,
                "historical results should be reclaimable, clearedRatio=" + clearedRatio
        );
    }

    @Test
    @Timeout(20)
    void processRows_diverseHintProfiles_shouldTriggerCacheHygieneCleanup() {
        ExclusiveFpRowsProcessingApi.IntermediateSteps.clearPremergeHintAggregateCache();
        List<DocTerms> rows = buildRows(120);

        for (int i = 0; i < 420; i++) {
            ExclusiveFpRowsProcessingApi.ProcessingOptions options = ExclusiveFpRowsProcessingApi.defaultOptions()
                    .withMinSupport(2)
                    .withMinItemsetSize(2)
                    .withHintBoostWeight(8)
                    .withPremergeMutexGroupHints(Arrays.asList(
                            hint("A", "B", "M" + i),
                            hint("A", "C", "N" + i)
                    ));
            ExclusiveFpRowsProcessingApi.processRows(rows, options);
        }

        long cleanups = ExclusiveFpRowsProcessingApi.IntermediateSteps.premergeHintAggregateCacheCleanupCount();
        assertTrue(cleanups >= 1L, "cache hygiene cleanup should trigger under sustained miss-dominant load");
    }

    private static List<DocTerms> buildRows(int count) {
        List<DocTerms> rows = new ArrayList<DocTerms>();
        for (int i = 0; i < count; i++) {
            rows.add(doc(i, "A", "B", "C" + (i % 13), "D" + (i % 21)));
        }
        return rows;
    }

    private static DocTerms doc(int docId, String... terms) {
        List<ByteRef> refs = new ArrayList<ByteRef>();
        for (String term : terms) {
            refs.add(ref(term));
        }
        return new DocTerms(docId, refs);
    }

    private static ByteRef ref(String term) {
        byte[] bytes = term.getBytes();
        return new ByteRef(bytes, 0, bytes.length);
    }

    private static ExclusiveFpRowsProcessingApi.PremergeHint hint(String... terms) {
        List<ByteRef> refs = new ArrayList<ByteRef>();
        for (String term : terms) {
            refs.add(ref(term));
        }
        return new ExclusiveFpRowsProcessingApi.PremergeHint(refs);
    }

    private static int forceGcAndCountCleared(List<WeakReference<LineFileProcessingResult>> refs) {
        int cleared = 0;
        for (int round = 0; round < 8; round++) {
            System.gc();
            byte[][] pressure = new byte[16][];
            for (int i = 0; i < pressure.length; i++) {
                pressure[i] = new byte[512 * 1024];
            }
            pressure = null;
            cleared = countCleared(refs);
            if (cleared == refs.size()) {
                return cleared;
            }
        }
        return cleared;
    }

    private static int countCleared(List<WeakReference<LineFileProcessingResult>> refs) {
        int cleared = 0;
        for (WeakReference<LineFileProcessingResult> ref : refs) {
            if (ref.get() == null) {
                cleared++;
            }
        }
        return cleared;
    }
}
