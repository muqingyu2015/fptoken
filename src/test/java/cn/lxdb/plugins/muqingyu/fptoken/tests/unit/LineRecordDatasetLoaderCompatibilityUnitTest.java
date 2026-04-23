package cn.lxdb.plugins.muqingyu.fptoken.tests.unit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.DocTerms;
import cn.lxdb.plugins.muqingyu.fptoken.runner.dataset.LineRecordDatasetLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

class LineRecordDatasetLoaderCompatibilityUnitTest {

    @Test
    void legacyNgramApis_shouldReturnTokenizedRows_andRawApisShouldKeepSingleRawTerm() throws Exception {
        Path dir = Files.createTempDirectory("fptoken-loader-compat-");
        Path file = dir.resolve("sample.txt");
        Files.write(file, Arrays.asList("ABCD"), StandardCharsets.UTF_8);

        List<DocTerms> ngram2Rows = LineRecordDatasetLoader.loadSingleFile(file, 2, 2).getRows();
        List<DocTerms> ngram3Rows = LineRecordDatasetLoader.loadSingleFile(file, 3, 3).getRows();
        List<DocTerms> rawRows = LineRecordDatasetLoader.loadSingleFileRaw(file).getRows();

        assertEquals(1, ngram2Rows.size());
        assertEquals(1, ngram3Rows.size());
        assertEquals(1, rawRows.size());

        // "ABCD" -> 2-gram: AB BC CD
        assertEquals(3, ngram2Rows.get(0).getTermsUnsafe().size());
        // "ABCD" -> 3-gram: ABC BCD
        assertEquals(2, ngram3Rows.get(0).getTermsUnsafe().size());
        assertNotEquals(
                ngram2Rows.get(0).getTermsUnsafe().size(),
                ngram3Rows.get(0).getTermsUnsafe().size());

        // raw API: 一条记录只保留一个原始 byte[]。
        assertEquals(1, rawRows.get(0).getTermsUnsafe().size());
        assertArrayEquals("ABCD".getBytes(StandardCharsets.UTF_8), rawRows.get(0).getTermsUnsafe().get(0));
    }
}
