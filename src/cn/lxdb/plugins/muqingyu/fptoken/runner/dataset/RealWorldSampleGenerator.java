package cn.lxdb.plugins.muqingyu.fptoken.runner.dataset;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * 将真实文档规范化为测试输入文件：
 * 每行 <= 64 bytes，每文件最多 32000 行。
 */
public final class RealWorldSampleGenerator {

    private static final int MAX_BYTES_PER_LINE = 64;
    private static final int MAX_LINES_PER_FILE = 32000;

    private RealWorldSampleGenerator() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 3 || args.length % 3 != 0) {
            System.out.println("Usage:");
            System.out.println("  RealWorldSampleGenerator <srcFile> <dstFile> <maxLines> [<srcFile> <dstFile> <maxLines> ...]");
            return;
        }

        for (int i = 0; i < args.length; i += 3) {
            Path src = Paths.get(args[i]);
            Path dst = Paths.get(args[i + 1]);
            int maxLines = Integer.parseInt(args[i + 2]);
            normalize(src, dst, Math.max(1, Math.min(MAX_LINES_PER_FILE, maxLines)));
            System.out.println("generated: " + dst + " from " + src);
        }
    }

    public static void normalize(Path src, Path dst, int maxLines) throws IOException {
        Files.createDirectories(dst.getParent());
        try (BufferedReader reader = Files.newBufferedReader(src, StandardCharsets.UTF_8);
             BufferedWriter writer = Files.newBufferedWriter(dst, StandardCharsets.UTF_8)) {
            String line;
            int lines = 0;
            while ((line = reader.readLine()) != null && lines < maxLines) {
                writer.write(normalizeLine(line));
                writer.newLine();
                lines++;
            }
        }
    }

    private static String normalizeLine(String line) {
        if (line == null || line.isEmpty()) {
            return "";
        }
        byte[] bytes = line.getBytes(StandardCharsets.UTF_8);
        if (bytes.length <= MAX_BYTES_PER_LINE) {
            return line;
        }
        byte[] out = new byte[MAX_BYTES_PER_LINE];
        System.arraycopy(bytes, 0, out, 0, MAX_BYTES_PER_LINE);
        return new String(out, StandardCharsets.UTF_8);
    }
}
