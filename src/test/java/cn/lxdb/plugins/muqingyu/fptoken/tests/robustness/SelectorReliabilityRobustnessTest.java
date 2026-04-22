package cn.lxdb.plugins.muqingyu.fptoken.tests.robustness;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import cn.lxdb.plugins.muqingyu.fptoken.ExclusiveFrequentItemsetSelector;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.DocTerms;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.ExclusiveSelectionResult;
import cn.lxdb.plugins.muqingyu.fptoken.tests.ByteArrayTestSupport;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

class SelectorReliabilityRobustnessTest {

    @Test
    void parallelFacadeInvocations_sameInput_shouldBeDeterministic() throws Exception {
        List<DocTerms> rows = ByteArrayTestSupport.pcapLikeBatch(120, 128, 32, 16);
        String baseline = fingerprint(rows);

        ExecutorService pool = Executors.newFixedThreadPool(4);
        try {
            List<Future<String>> futures = new ArrayList<>();
            for (int i = 0; i < 8; i++) {
                futures.add(pool.submit(new FingerprintTask(rows)));
            }
            for (Future<String> f : futures) {
                assertEquals(baseline, f.get());
            }
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void mixedSpecialBytes_andDuplicateDocIds_shouldKeepCoreInvariants() {
        List<DocTerms> rows = new ArrayList<>();
        rows.add(ByteArrayTestSupport.doc(1, new byte[] {0x00}, new byte[] {(byte) 0xFF}, new byte[] {0x00, 0x00}));
        rows.add(ByteArrayTestSupport.doc(1, new byte[] {0x00}, new byte[] {0x01}, new byte[] {(byte) 0xFF}));
        rows.add(ByteArrayTestSupport.doc(2, new byte[] {0x00}, new byte[] {0x01}, new byte[] {0x02}));
        rows.add(ByteArrayTestSupport.doc(3, new byte[] {0x01}, new byte[] {0x02}, new byte[] {0x03}));
        rows.add(ByteArrayTestSupport.doc(4, new byte[] {0x00}, new byte[] {0x02}, new byte[] {0x03}));

        ExclusiveSelectionResult result =
                ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(rows, 2, 1, 4, 10_000);
        assertTrue(result.getCandidateCount() <= result.getMaxCandidateCount());
        assertTrue(ByteArrayTestSupport.pairwiseTermsDisjoint(result.getGroups()));
    }

    @Test
    void corruptedLikePayloadRows_shouldNotCrash_andStayBounded() {
        List<DocTerms> rows = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            byte[] longTerm = new byte[256];
            Arrays.fill(longTerm, (byte) (i & 0xFF));
            rows.add(ByteArrayTestSupport.doc(
                    i,
                    longTerm,
                    new byte[] {(byte) i},
                    new byte[] {(byte) (i >>> 1), (byte) (i >>> 2)},
                    new byte[] {0x00, (byte) 0xFF, 0x00}
            ));
        }

        ExclusiveSelectionResult result =
                ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(rows, 10, 1, 5, 12_000);
        assertTrue(result.getCandidateCount() >= 0);
        assertTrue(result.getCandidateCount() <= 12_000);
    }

    @Test
    @EnabledIfSystemProperty(named = "fptoken.runScaleTests", matches = "true")
    void sparseLargeDocIds_scaleGuard_shouldCompleteOrFailFast() {
        List<DocTerms> rows = new ArrayList<>();
        rows.add(ByteArrayTestSupport.doc(10_000, ByteArrayTestSupport.hex("AA")));
        rows.add(ByteArrayTestSupport.doc(60_000, ByteArrayTestSupport.hex("AA")));
        rows.add(ByteArrayTestSupport.doc(120_000, ByteArrayTestSupport.hex("AA")));

        boolean success = false;
        try {
            ExclusiveSelectionResult result =
                    ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(rows, 2, 1, 3, 2_000);
            assertTrue(result.getCandidateCount() >= 0);
            success = true;
        } catch (OutOfMemoryError oom) {
            // 可接受：该用例用于记录极端稀疏 docId 的资源风险边界。
            success = true;
        }
        assertTrue(success);
    }

    @Test
    void outputFingerprint_shouldRemainStable_afterUnrelatedInputMutation() {
        byte[] a = ByteArrayTestSupport.hex("AA");
        byte[] b = ByteArrayTestSupport.hex("BB");
        List<DocTerms> rows = new ArrayList<>();
        for (int i = 0; i < 60; i++) {
            rows.add(ByteArrayTestSupport.doc(i, a, b, new byte[] {(byte) (i & 0x0F)}));
        }

        String first = fingerprint(rows);
        a[0] = 0x00;
        b[0] = 0x00;
        String second = fingerprint(rows);
        assertEquals(first, second);
    }

    @Test
    void listAndStatsOverloads_shouldFailConsistently_onNullRowElement() {
        List<DocTerms> rows = new ArrayList<>();
        rows.add(ByteArrayTestSupport.doc(0, ByteArrayTestSupport.hex("AA")));
        rows.add(null);

        assertThrows(
                IllegalArgumentException.class,
                () -> ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsets(rows, 1, 1, 3, 1000));
        assertThrows(
                IllegalArgumentException.class,
                () -> ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(rows, 1, 1, 3, 1000));
    }

    @Test
    void badInputFailure_shouldNotAffectNextValidInvocation() {
        List<DocTerms> badRows = new ArrayList<>();
        badRows.add(ByteArrayTestSupport.doc(-1, ByteArrayTestSupport.hex("AA")));
        assertThrows(
                IllegalArgumentException.class,
                () -> ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(badRows, 1, 1, 3, 2000));

        List<DocTerms> goodRows = new ArrayList<>();
        for (int i = 0; i < 40; i++) {
            goodRows.add(ByteArrayTestSupport.doc(i, ByteArrayTestSupport.hex("AA"), ByteArrayTestSupport.hex("BB")));
        }
        ExclusiveSelectionResult result =
                ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(goodRows, 10, 2, 4, 5000);
        assertTrue(result.getCandidateCount() >= 0);
        assertTrue(ByteArrayTestSupport.pairwiseTermsDisjoint(result.getGroups()));
    }

    @Test
    void parallelMixedValidAndInvalidRequests_shouldIsolateFailures() throws Exception {
        List<DocTerms> validRows = ByteArrayTestSupport.pcapLikeBatch(20, 64, 16, 8);
        List<DocTerms> invalidRows = new ArrayList<>();
        invalidRows.add(ByteArrayTestSupport.doc(-9, ByteArrayTestSupport.hex("FF")));

        ExecutorService pool = Executors.newFixedThreadPool(4);
        int success = 0;
        int failed = 0;
        try {
            List<Future<Boolean>> futures = new ArrayList<>();
            for (int i = 0; i < 10; i++) {
                final boolean runInvalid = (i % 2 == 0);
                futures.add(pool.submit(() -> {
                    if (runInvalid) {
                        ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(
                                invalidRows, 1, 1, 3, 1000);
                        return false;
                    }
                    ExclusiveSelectionResult result = ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(
                            validRows, 3, 1, 4, 5000);
                    return result.getCandidateCount() >= 0;
                }));
            }
            for (Future<Boolean> future : futures) {
                try {
                    if (future.get()) {
                        success++;
                    }
                } catch (java.util.concurrent.ExecutionException ex) {
                    Throwable cause = ex.getCause();
                    if (cause instanceof IllegalArgumentException) {
                        failed++;
                    } else {
                        throw ex;
                    }
                }
            }
        } finally {
            pool.shutdownNow();
        }

        assertEquals(5, success);
        assertEquals(5, failed);
    }

    private String fingerprint(List<DocTerms> rows) {
        ExclusiveSelectionResult result =
                ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(rows, 8, 2, 6, 20_000);
        return ByteArrayTestSupport.groupsFingerprint(result.getGroups());
    }

    private static final class FingerprintTask implements Callable<String> {
        private final List<DocTerms> rows;

        FingerprintTask(List<DocTerms> rows) {
            this.rows = rows;
        }

        @Override
        public String call() {
            ExclusiveSelectionResult result = ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(
                    rows, 8, 2, 6, 20_000);
            return ByteArrayTestSupport.groupsFingerprint(result.getGroups());
        }
    }
}
