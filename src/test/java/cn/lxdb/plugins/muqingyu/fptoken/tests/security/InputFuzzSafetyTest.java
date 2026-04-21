package cn.lxdb.plugins.muqingyu.fptoken.tests.security;

import static org.junit.jupiter.api.Assertions.assertTrue;

import cn.lxdb.plugins.muqingyu.fptoken.ExclusiveFrequentItemsetSelector;
import cn.lxdb.plugins.muqingyu.fptoken.model.DocTerms;
import cn.lxdb.plugins.muqingyu.fptoken.tests.ByteArrayTestSupport;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.junit.jupiter.api.Test;

class InputFuzzSafetyTest {

    @Test
    void randomByteInputs_shouldNotCrashAndShouldKeepMutexInvariant() {
        Random r = new Random(20260421L);
        List<DocTerms> rows = new ArrayList<>();
        for (int docId = 0; docId < 120; docId++) {
            int terms = 3 + r.nextInt(8);
            List<byte[]> list = new ArrayList<>();
            for (int i = 0; i < terms; i++) {
                int len = 1 + r.nextInt(6);
                byte[] term = new byte[len];
                r.nextBytes(term);
                list.add(term);
            }
            rows.add(ByteArrayTestSupport.doc(docId, list));
        }

        var result = ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(rows, 5, 2, 5, 25_000);
        assertTrue(result.getCandidateCount() >= 0);
        assertTrue(ByteArrayTestSupport.pairwiseTermsDisjoint(result.getGroups()));
    }

    @Test
    void veryLongTerms_shouldBeHandledWithoutFailure() {
        List<DocTerms> rows = new ArrayList<>();
        byte[] longTerm = new byte[2048];
        for (int i = 0; i < longTerm.length; i++) {
            longTerm[i] = (byte) (i & 0xFF);
        }
        for (int d = 0; d < 30; d++) {
            rows.add(ByteArrayTestSupport.doc(d, longTerm, new byte[] {(byte) d}));
        }

        var result = ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(rows, 10, 1, 3, 5000);
        assertTrue(result.getFrequentTermCount() >= 1);
        assertTrue(result.getCandidateCount() <= result.getMaxCandidateCount());
    }
}
