package cn.lxdb.plugins.muqingyu.fptoken.runner.dataset;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * 准备测试输入文件（每行 <=64 byte，且大多数行为 64 byte）。
 */
public final class SampleDataPreparer {

    private static final int MAJOR_LEN = 64;
    private static final String ALPHABET = "0123456789abcdefABCDEFGHIJKLMNOPQRSTUVWXYZ-_";

    private SampleDataPreparer() {
    }

    public static List<Path> ensureSampleFiles(Path dataDir) throws IOException {
        Files.createDirectories(dataDir);
        List<Path> generated = new ArrayList<Path>();

        generated.add(ensureFile(dataDir.resolve("records_001_small.txt"), 128, "S"));
        generated.add(ensureFile(dataDir.resolve("records_002_medium.txt"), 2048, "M"));
        generated.add(ensureFile(dataDir.resolve("records_003_large.txt"), 12000, "L"));
        generated.add(ensureFile(dataDir.resolve("records_004_limit32000.txt"), 32000, "X"));
        generated.add(ensureFile(dataDir.resolve("records_005_sparse_short.txt"), 768, "P"));
        generated.add(ensureFile(dataDir.resolve("records_006_mix.txt"), 4096, "Q"));

        return generated;
    }

    private static Path ensureFile(Path file, int lineCount, String prefix) throws IOException {
        if (Files.exists(file) && Files.size(file) > 0L) {
            return file;
        }
        try (BufferedWriter writer = Files.newBufferedWriter(file, StandardCharsets.UTF_8)) {
            for (int i = 0; i < lineCount; i++) {
                writer.write(buildLine(prefix, i));
                writer.newLine();
            }
        }
        return file;
    }

    private static String buildLine(String prefix, int index) {
        // 每 23 行制造空行（0 byte），覆盖极端情况
        if (index % 23 == 0) {
            return "";
        }

        int targetLength = MAJOR_LEN;
        // 少量短行（0~64 之间）
        if (index % 11 == 0) {
            targetLength = 12 + (index % 37);
        } else if (index % 7 == 0) {
            targetLength = 48 + (index % 16);
        }

        String base = prefix + "_line_" + index + "_";
        StringBuilder sb = new StringBuilder(targetLength);
        sb.append(base);
        int cursor = index;
        while (sb.length() < targetLength) {
            sb.append(ALPHABET.charAt(Math.abs(cursor) % ALPHABET.length()));
            cursor += 13;
        }
        if (sb.length() > targetLength) {
            sb.setLength(targetLength);
        }
        return sb.toString();
    }
}
