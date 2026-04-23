package cn.lxdb.plugins.muqingyu.fptoken.runner.dataset;

import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.DocTerms;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.config.EngineTuningConfig;
import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * 遍历目录中的测试文件，逐行读取并转为 DocTerms。
 */
public final class LineRecordDatasetLoader {

    public static final int MAX_LINES_PER_FILE = EngineTuningConfig.DEFAULT_MAX_LINES_PER_FILE;
    public static final int MAX_BYTES_PER_LINE = EngineTuningConfig.DEFAULT_MAX_BYTES_PER_LINE;

    private LineRecordDatasetLoader() {
    }

    public static LoadedDataset loadSingleFile(Path file, int ngramStart, int ngramEnd) throws IOException {
        return loadSingleFileWithStats(file, ngramStart, ngramEnd).getLoadedDataset();
    }

    public static LoadOutcome loadSingleFileWithStats(Path file, int ngramStart, int ngramEnd) throws IOException {
        if (!Files.isRegularFile(file)) {
            throw new IOException("Input file not found: " + file);
        }
        List<Path> files = new ArrayList<Path>();
        files.add(file);
        return loadFromFiles(files, ngramStart, ngramEnd);
    }

    public static LoadedDataset load(Path dataDir, int ngramStart, int ngramEnd) throws IOException {
        return loadWithStats(dataDir, ngramStart, ngramEnd).getLoadedDataset();
    }

    public static LoadOutcome loadWithStats(Path dataDir, int ngramStart, int ngramEnd) throws IOException {
        if (!Files.isDirectory(dataDir)) {
            throw new IOException("Data directory not found: " + dataDir);
        }
        List<Path> files = collectFiles(dataDir);
        return loadFromFiles(files, ngramStart, ngramEnd);
    }

    private static LoadOutcome loadFromFiles(List<Path> files, int ngramStart, int ngramEnd) throws IOException {
        List<DocTerms> rows = new ArrayList<DocTerms>();
        int docId = 0;
        long totalLines = 0L;
        long emptyLines = 0L;
        long truncatedLines = 0L;
        long droppedByCap = 0L;

        for (Path file : files) {
            long fileLine = 0L;
            try (BufferedReader reader = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
                String line;
                while ((line = reader.readLine()) != null) {
                    fileLine++;
                    // 每个文件最多处理 32000 行，多余行直接计入 droppedByCap。
                    if (fileLine > MAX_LINES_PER_FILE) {
                        droppedByCap++;
                        continue;
                    }

                    totalLines++;
                    byte[] bytes = line.getBytes(StandardCharsets.UTF_8);
                    if (bytes.length == 0) {
                        emptyLines++;
                    }
                    // 每行最多 64 字节，超过即截断，不做多字节字符回退。
                    if (bytes.length > MAX_BYTES_PER_LINE) {
                        byte[] truncated = new byte[MAX_BYTES_PER_LINE];
                        System.arraycopy(bytes, 0, truncated, 0, MAX_BYTES_PER_LINE);
                        bytes = truncated;
                        truncatedLines++;
                    }

                    // 仅保存原始行字节；具体切词由处理层 processRows 在内部执行。
                    List<byte[]> raw = new ArrayList<byte[]>(1);
                    raw.add(bytes);
                    // docId 在加载时连续分配，作为行级唯一标识。
                    rows.add(new DocTerms(docId++, raw));
                }
            }
        }

        Stats stats = new Stats(files.size(), totalLines, rows.size(), emptyLines, truncatedLines, droppedByCap);
        return new LoadOutcome(new LoadedDataset(rows), stats);
    }

    private static List<Path> collectFiles(Path dataDir) throws IOException {
        List<Path> files = new ArrayList<Path>();
        java.nio.file.DirectoryStream<Path> stream = Files.newDirectoryStream(dataDir);
        try {
            for (Path file : stream) {
                if (Files.isRegularFile(file)) {
                    files.add(file);
                }
            }
        } finally {
            stream.close();
        }
        Collections.sort(files, new Comparator<Path>() {
            @Override
            public int compare(Path a, Path b) {
                return a.getFileName().toString().compareTo(b.getFileName().toString());
            }
        });
        return files;
    }

    public static final class LoadedDataset {
        private final List<DocTerms> rows;

        private LoadedDataset(List<DocTerms> rows) {
            this.rows = rows;
        }

        public List<DocTerms> getRows() {
            return rows;
        }
    }

    /**
     * 加载结果封装：将业务行数据与统计信息解耦返回。
     */
    public static final class LoadOutcome {
        private final LoadedDataset loadedDataset;
        private final Stats stats;

        private LoadOutcome(LoadedDataset loadedDataset, Stats stats) {
            this.loadedDataset = loadedDataset;
            this.stats = stats;
        }

        public LoadedDataset getLoadedDataset() {
            return loadedDataset;
        }

        public Stats getStats() {
            return stats;
        }
    }

    public static final class Stats {
        private final int fileCount;
        private final long totalLines;
        private final int docCount;
        private final long emptyLines;
        private final long truncatedLines;
        private final long droppedByCap;

        private Stats(
                int fileCount,
                long totalLines,
                int docCount,
                long emptyLines,
                long truncatedLines,
                long droppedByCap
        ) {
            this.fileCount = fileCount;
            this.totalLines = totalLines;
            this.docCount = docCount;
            this.emptyLines = emptyLines;
            this.truncatedLines = truncatedLines;
            this.droppedByCap = droppedByCap;
        }

        public int getFileCount() {
            return fileCount;
        }

        public long getTotalLines() {
            return totalLines;
        }

        public int getDocCount() {
            return docCount;
        }

        public long getEmptyLines() {
            return emptyLines;
        }

        public long getTruncatedLines() {
            return truncatedLines;
        }

        public long getDroppedByCap() {
            return droppedByCap;
        }
    }
}
