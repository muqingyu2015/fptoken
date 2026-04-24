package cn.lxdb.plugins.muqingyu.fptoken.tests.functional;

import cn.lxdb.plugins.muqingyu.fptoken.tests.ByteArrayTestSupport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import cn.lxdb.plugins.muqingyu.fptoken.ExclusiveFrequentItemsetSelector;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.index.TermTidsetIndex;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.DocTerms;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import org.junit.jupiter.api.Test;

/** 璁捐鏂囨。 C-00x锛氭敮鎸佸害涓庝綅鍥炬墜宸ユ牳瀵广€佺‘瀹氭€х瓑锛堝皬鏁版嵁鍙弗鏍奸獙璇侊級銆?*/
class CorrectnessDesignDocTest {

    @Test
    void c002_supportMatchesTidsetCardinality() {
        byte[] t = ByteArrayTestSupport.hex("FACADE");
        List<DocTerms> rows = new ArrayList<>();
        int expectedDocs = 0;
        for (int i = 0; i < 18; i++) {
            if (i % 3 != 0) {
                rows.add(ByteArrayTestSupport.doc(i, t));
                expectedDocs++;
            }
        }
        var index = TermTidsetIndex.build(rows);
        int termId = -1;
        List<byte[]> dict = index.getIdToTerm();
        for (int i = 0; i < dict.size(); i++) {
            if (java.util.Arrays.equals(dict.get(i), t)) {
                termId = i;
                break;
            }
        }
        assertTrue(termId >= 0);
        BitSet bits = index.getTidsetsByTermId().get(termId);
        assertEquals(expectedDocs, bits.cardinality());
    }

    @Test
    void c004_deterministicFingerprint() {
        List<DocTerms> rows = new ArrayList<>();
        byte[] z = {0x7A};
        for (int i = 0; i < 24; i++) {
            rows.add(ByteArrayTestSupport.doc(i, z));
        }
        var a = ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(rows, 5, 1, 4, 50_000);
        var b = ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(rows, 5, 1, 4, 50_000);
        assertEquals(
                ByteArrayTestSupport.groupsFingerprint(a.getGroups()),
                ByteArrayTestSupport.groupsFingerprint(b.getGroups()));
    }
}

