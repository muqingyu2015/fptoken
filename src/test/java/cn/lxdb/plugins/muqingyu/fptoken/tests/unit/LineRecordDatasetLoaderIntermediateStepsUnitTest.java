package cn.lxdb.plugins.muqingyu.fptoken.tests.unit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.DocTerms;
import cn.lxdb.plugins.muqingyu.fptoken.runner.dataset.LineRecordDatasetLoader;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import org.junit.jupiter.api.Test;

class LineRecordDatasetLoaderIntermediateStepsUnitTest {

    @Test
    void validateNgramRange_shouldRejectInvalidRanges() {
        assertThrows(
                IllegalArgumentException.class,
                () -> LineRecordDatasetLoader.IntermediateSteps.validateNgramRange(0, 2));
        assertThrows(
                IllegalArgumentException.class,
                () -> LineRecordDatasetLoader.IntermediateSteps.validateNgramRange(3, 2));
    }

    @Test
    void prepareLineBytes_shouldFlagEmptyAndTruncatedStates() {
        LineRecordDatasetLoader.PreparedLine empty =
                LineRecordDatasetLoader.IntermediateSteps.prepareLineBytes("");
        assertTrue(empty.isEmptyLine());
        assertFalse(empty.isTruncated());
        assertEquals(0, empty.getBytes().length);

        int overLimitSize = LineRecordDatasetLoader.MAX_BYTES_PER_LINE + 12;
        byte[] source = new byte[overLimitSize];
        Arrays.fill(source, (byte) 'A');
        String longAsciiLine = new String(source, StandardCharsets.UTF_8);
        LineRecordDatasetLoader.PreparedLine longPrepared =
                LineRecordDatasetLoader.IntermediateSteps.prepareLineBytes(longAsciiLine);
        assertFalse(longPrepared.isEmptyLine());
        assertTrue(longPrepared.isTruncated());
        assertEquals(LineRecordDatasetLoader.MAX_BYTES_PER_LINE, longPrepared.getBytes().length);
    }

    @Test
    void buildTokenizedRow_shouldApplyNgramSplit() {
        DocTerms row = LineRecordDatasetLoader.IntermediateSteps.buildTokenizedRow(
                7, "ABCD".getBytes(StandardCharsets.UTF_8), 2, 2);

        assertEquals(7, row.getDocId());
        assertEquals(3, row.getTermsUnsafe().size());
        assertArrayEquals("AB".getBytes(StandardCharsets.UTF_8), row.getTermsUnsafe().get(0));
        assertArrayEquals("BC".getBytes(StandardCharsets.UTF_8), row.getTermsUnsafe().get(1));
        assertArrayEquals("CD".getBytes(StandardCharsets.UTF_8), row.getTermsUnsafe().get(2));
    }

    @Test
    void buildRawRow_shouldKeepSingleOriginalBytes() {
        byte[] input = "ABCD".getBytes(StandardCharsets.UTF_8);
        DocTerms row = LineRecordDatasetLoader.IntermediateSteps.buildRawRow(9, input);

        assertEquals(9, row.getDocId());
        assertEquals(1, row.getTermsUnsafe().size());
        assertArrayEquals(input, row.getTermsUnsafe().get(0));
    }
}
