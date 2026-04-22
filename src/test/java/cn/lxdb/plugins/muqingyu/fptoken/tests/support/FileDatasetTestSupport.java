package cn.lxdb.plugins.muqingyu.fptoken.tests.support;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * 文件型测试数据构建工具：一行一条记录，便于行读取场景回归。
 */
public final class FileDatasetTestSupport {

    private static final String ALPHABET = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz-_";

    private FileDatasetTestSupport() {
    }

    public static void writeRecordFile(
            Path file,
            int lineCount,
            String prefix,
            int majorLen,
            int shortEvery,
            int overflowEvery
    ) throws IOException {
        Files.createDirectories(file.getParent());
        try (BufferedWriter writer = Files.newBufferedWriter(file, StandardCharsets.UTF_8)) {
            for (int i = 0; i < lineCount; i++) {
                writer.write(buildLine(prefix, i, majorLen, shortEvery, overflowEvery));
                writer.newLine();
            }
        }
    }

    private static String buildLine(String prefix, int index, int majorLen, int shortEvery, int overflowEvery) {
        if (index % 37 == 0) {
            return "";
        }
        int targetLen = majorLen;
        if (shortEvery > 0 && index % shortEvery == 0) {
            targetLen = Math.max(4, majorLen / 2);
        }
        if (overflowEvery > 0 && index % overflowEvery == 0) {
            targetLen = majorLen + 20;
        }

        StringBuilder sb = new StringBuilder(targetLen);
        sb.append("COMMON_").append(prefix).append('_').append(index).append('_');
        int cursor = index;
        while (sb.length() < targetLen) {
            sb.append(ALPHABET.charAt(Math.abs(cursor) % ALPHABET.length()));
            cursor += 17;
        }
        if (sb.length() > targetLen) {
            sb.setLength(targetLen);
        }
        return sb.toString();
    }
}
