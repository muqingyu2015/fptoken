package cn.lxdb.plugins.muqingyu.fptoken.tests.functional;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import cn.lxdb.plugins.muqingyu.fptoken.runner.entry.FptokenLineFileRunnerMain;
import cn.lxdb.plugins.muqingyu.fptoken.runner.result.LineFileProcessingResult;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.BitSet;
import org.junit.jupiter.api.Test;

/**
 * 文件输入路径下 one-byte 倒排索引功能测试。
 */
class OneByteDocidBitsetFromFileFunctionalTest {

    @Test
    void runPipeline_shouldBuildOneByteDocidBitset_fromRawLines() throws Exception {
        Path dir = Files.createTempDirectory("fptoken-one-byte-functional-");
        Path input = dir.resolve("input.txt");
        Files.write(input, Arrays.asList("A", "B", "AB", ""), StandardCharsets.UTF_8);

        LineFileProcessingResult result = FptokenLineFileRunnerMain.runPipeline(
                dir, input, 2, 2, 100, 2, 0);
        LineFileProcessingResult.OneByteDocidBitsetIndex oneByte = result.getOneByteDocidBitsetIndex();

        BitSet aDocs = oneByte.getDocIdBitset('A');
        BitSet bDocs = oneByte.getDocIdBitset('B');
        BitSet cDocs = oneByte.getDocIdBitset('C');

        // doc0="A", doc2="AB"
        assertTrue(aDocs.get(0));
        assertTrue(aDocs.get(2));
        assertFalse(aDocs.get(1));
        assertFalse(aDocs.get(3));

        // doc1="B", doc2="AB"
        assertTrue(bDocs.get(1));
        assertTrue(bDocs.get(2));
        assertFalse(bDocs.get(0));
        assertFalse(bDocs.get(3));

        // 不存在的字节不应命中任何 doc
        assertTrue(cDocs.isEmpty());
    }
}

