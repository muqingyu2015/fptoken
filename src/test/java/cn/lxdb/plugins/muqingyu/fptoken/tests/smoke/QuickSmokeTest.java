package cn.lxdb.plugins.muqingyu.fptoken.tests.smoke;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import cn.lxdb.plugins.muqingyu.fptoken.ExclusiveFrequentItemsetSelector;
import cn.lxdb.plugins.muqingyu.fptoken.tests.ByteArrayTestSupport;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("smoke")
class QuickSmokeTest {

    @Test
    void smoke_selectorCorePath_runsFastAndReturnsValidShape() {
        List<cn.lxdb.plugins.muqingyu.fptoken.model.DocTerms> rows = new ArrayList<>();
        byte[] p = ByteArrayTestSupport.hex("A1B2");
        for (int i = 0; i < 18; i++) {
            rows.add(ByteArrayTestSupport.doc(i, p, new byte[] {(byte) (i % 3)}));
        }

        var r = ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(rows, 4, 2, 4, 3000);
        assertFalse(r.isTruncatedByCandidateLimit());
        assertTrue(r.getCandidateCount() >= 0);
        assertTrue(ByteArrayTestSupport.pairwiseTermsDisjoint(r.getGroups()));
    }
}
