package cn.lxdb.plugins.muqingyu.fptoken.tests.unit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import cn.lxdb.plugins.muqingyu.fptoken.api.ExclusiveFpRowsProcessingApi;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.DocTerms;
import cn.lxdb.plugins.muqingyu.fptoken.runner.result.LineFileProcessingResult;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * OneByteDocidBitsetIndex 边界与防御性行为测试。
 */
class OneByteDocidBitsetIndexBoundaryUnitTest {

    @Test
    void oneByteIndex_shouldHandleEmptyRows() {
        LineFileProcessingResult result = ExclusiveFpRowsProcessingApi.processRows(
                new ArrayList<DocTerms>(), 1, 2, 0);
        LineFileProcessingResult.OneByteDocidBitsetIndex oneByte = result.getOneByteDocidBitsetIndex();

        assertEquals(-1, oneByte.getMaxDocId());
        assertTrue(oneByte.getDocIdBitset(0x41).isEmpty());
        assertEquals(256, oneByte.getBuckets().size());
    }

    @Test
    void oneByteIndex_shouldRejectOutOfRangeByteValue() {
        List<DocTerms> rows = new ArrayList<DocTerms>();
        rows.add(new DocTerms(0, Arrays.asList(bytes("ABC"))));
        LineFileProcessingResult.OneByteDocidBitsetIndex oneByte =
                ExclusiveFpRowsProcessingApi.processRowsWithNgram(rows, 2, 2, 100, 2, 0)
                        .getOneByteDocidBitsetIndex();

        assertThrows(IllegalArgumentException.class, () -> oneByte.getDocIdBitset(-1));
        assertThrows(IllegalArgumentException.class, () -> oneByte.getDocIdBitset(256));
    }

    @Test
    void oneByteIndex_getDocIdBitset_shouldReturnDefensiveCopy() {
        List<DocTerms> rows = new ArrayList<DocTerms>();
        rows.add(new DocTerms(0, Arrays.asList(bytes("A"))));
        rows.add(new DocTerms(1, Arrays.asList(bytes("A"))));
        LineFileProcessingResult.OneByteDocidBitsetIndex oneByte =
                ExclusiveFpRowsProcessingApi.processRowsWithNgram(rows, 2, 2, 100, 2, 0)
                        .getOneByteDocidBitsetIndex();

        BitSet firstRead = oneByte.getDocIdBitset('A');
        assertTrue(firstRead.get(0));
        assertTrue(firstRead.get(1));

        firstRead.clear(0);
        firstRead.clear(1);

        BitSet secondRead = oneByte.getDocIdBitset('A');
        assertTrue(secondRead.get(0));
        assertTrue(secondRead.get(1));
        assertFalse(secondRead.isEmpty());
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}

