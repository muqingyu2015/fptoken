package cn.lxdb.plugins.muqingyu.fptoken.tests.functional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import cn.lxdb.plugins.muqingyu.fptoken.ExclusiveFrequentItemsetSelector;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.DocTerms;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.ExclusiveSelectionResult;
import cn.lxdb.plugins.muqingyu.fptoken.tests.ByteArrayTestSupport;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class ExclusiveFrequentItemsetSelectorEdgeCasesFunctionalTest {

    @Test
    void rowsContainingNullDocTerms_throwsIllegalArgument() {
        List<DocTerms> rows = new ArrayList<>();
        rows.add(ByteArrayTestSupport.doc(0, new byte[] {0x11}));
        rows.add(null);

        assertThrows(
                IllegalArgumentException.class,
                () -> ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(rows, 1, 1, 4, 1000));
    }

    @Test
    void sparseDocIds_preservedInSelectedGroupDocIdList() {
        List<DocTerms> rows = new ArrayList<>();
        byte[] term = ByteArrayTestSupport.hex("CAFE");
        rows.add(ByteArrayTestSupport.doc(3, term));
        rows.add(ByteArrayTestSupport.doc(11, term));
        rows.add(ByteArrayTestSupport.doc(42, term));

        ExclusiveSelectionResult result = ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(
                rows, 2, 1, 3, 2000);

        assertTrue(!result.getGroups().isEmpty(), "expected at least one selected group");
        List<Integer> docIds = result.getGroups().get(0).getDocIds();
        assertEquals(List.of(3, 11, 42), docIds);
    }

    @Test
    void negativeDocId_propagatesIndexBuildFailure() {
        List<DocTerms> rows = new ArrayList<>();
        rows.add(ByteArrayTestSupport.doc(-1, ByteArrayTestSupport.hex("AA")));
        assertThrows(
                IllegalArgumentException.class,
                () -> ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(rows, 1, 1, 3, 1000));
    }
}
