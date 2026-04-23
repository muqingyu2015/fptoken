package cn.lxdb.plugins.muqingyu.fptoken.tests.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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
 * processRows API 的 one-byte 倒排接口集成测试。
 */
class ExclusiveFpRowsProcessingApiOneByteIndexIntegrationTest {

    @Test
    void processRows_shouldBuildIsolatedOneByteIndexes_acrossRepeatedCalls() {
        List<DocTerms> firstRows = new ArrayList<DocTerms>();
        firstRows.add(new DocTerms(0, Arrays.asList(bytes("AA"))));
        firstRows.add(new DocTerms(1, Arrays.asList(bytes("AB"))));

        List<DocTerms> secondRows = new ArrayList<DocTerms>();
        secondRows.add(new DocTerms(0, Arrays.asList(bytes("ZZ"))));
        secondRows.add(new DocTerms(1, Arrays.asList(bytes("ZY"))));

        LineFileProcessingResult first = ExclusiveFpRowsProcessingApi.processRowsWithNgram(
                firstRows, 2, 2, 100, 2, 0);
        LineFileProcessingResult second = ExclusiveFpRowsProcessingApi.processRowsWithNgram(
                secondRows, 2, 2, 100, 2, 0);

        BitSet firstA = first.getFinalIndexData().getOneByteDocidBitsetIndex().getDocIdBitset('A');
        BitSet firstZ = first.getFinalIndexData().getOneByteDocidBitsetIndex().getDocIdBitset('Z');
        BitSet secondZ = second.getFinalIndexData().getOneByteDocidBitsetIndex().getDocIdBitset('Z');
        BitSet secondA = second.getFinalIndexData().getOneByteDocidBitsetIndex().getDocIdBitset('A');

        assertTrue(firstA.get(0));
        assertTrue(firstA.get(1));
        assertTrue(firstZ.isEmpty());

        assertTrue(secondZ.get(0));
        assertTrue(secondZ.get(1));
        assertTrue(secondA.isEmpty());
    }

    @Test
    void processRows_shouldKeepTopLevelAndFinalDataOneByteAccessorsConsistent() {
        List<DocTerms> rows = new ArrayList<DocTerms>();
        rows.add(new DocTerms(2, Arrays.asList(bytes("ABC"))));
        rows.add(new DocTerms(4, Arrays.asList(bytes("ABD"))));

        LineFileProcessingResult result = ExclusiveFpRowsProcessingApi.processRowsWithNgram(
                rows, 2, 2, 100, 2, 0);

        LineFileProcessingResult.OneByteDocidBitsetIndex fromTop = result.getOneByteDocidBitsetIndex();
        LineFileProcessingResult.OneByteDocidBitsetIndex fromFinal =
                result.getFinalIndexData().getOneByteDocidBitsetIndex();

        assertEquals(fromFinal.getMaxDocId(), fromTop.getMaxDocId());
        assertEquals(fromFinal.getDocIdBitset('A'), fromTop.getDocIdBitset('A'));
        assertEquals(fromFinal.getDocIdBitset('D'), fromTop.getDocIdBitset('D'));
        assertFalse(fromTop.getDocIdBitset('X').get(2));
        assertFalse(fromTop.getDocIdBitset('X').get(4));
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}

