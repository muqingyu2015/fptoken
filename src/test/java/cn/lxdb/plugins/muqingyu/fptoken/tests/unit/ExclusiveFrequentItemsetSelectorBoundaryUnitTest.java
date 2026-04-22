package cn.lxdb.plugins.muqingyu.fptoken.tests.unit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import cn.lxdb.plugins.muqingyu.fptoken.ExclusiveFrequentItemsetSelector;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.DocTerms;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.ExclusiveSelectionResult;
import cn.lxdb.plugins.muqingyu.fptoken.tests.ByteArrayTestSupport;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;

class ExclusiveFrequentItemsetSelectorBoundaryUnitTest {

    @Test
    void listOverloads_nullRows_shortCircuitAndReturnEmptyList() {
        assertTrue(ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsets(null, 0, 0).isEmpty());
        assertTrue(ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsets(null, 0, 0, 0, -99).isEmpty());
    }

    @Test
    void withStats_nullRows_shortCircuitAndEchoNegativeMaxCandidateCount() {
        ExclusiveSelectionResult r =
                ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(null, 0, 0, 0, -123);
        assertTrue(r.getGroups().isEmpty());
        assertEquals(0, r.getFrequentTermCount());
        assertEquals(0, r.getCandidateCount());
        assertEquals(0, r.getIntersectionCount());
        assertEquals(-123, r.getMaxCandidateCount());
    }

    @Test
    void withStats_emptyRows_shortCircuitAndEchoNegativeMaxCandidateCount() {
        ExclusiveSelectionResult r = ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(
                Collections.emptyList(), 0, 0, 0, -7);
        assertTrue(r.getGroups().isEmpty());
        assertEquals(-7, r.getMaxCandidateCount());
    }

    @Test
    void withStats_nonEmptyRows_invalidMaxCandidateCountNegative_throws() {
        List<DocTerms> rows = Collections.singletonList(ByteArrayTestSupport.doc(0, new byte[] {1}));
        assertThrows(
                IllegalArgumentException.class,
                () -> ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(rows, 1, 1, 4, -1));
    }

    @Test
    void withStats_configValidatedBeforeIndexBuildEvenIfRowsHaveNoTerms() {
        List<DocTerms> rows = new ArrayList<>();
        rows.add(ByteArrayTestSupport.doc(0, Collections.emptyList()));
        assertThrows(
                IllegalArgumentException.class,
                () -> ExclusiveFrequentItemsetSelector.selectExclusiveBestItemsetsWithStats(rows, 0, 1, 4, 10));
    }
}
