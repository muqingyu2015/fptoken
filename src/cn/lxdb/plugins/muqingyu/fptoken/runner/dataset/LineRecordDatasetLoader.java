package cn.lxdb.plugins.muqingyu.fptoken.runner.dataset;

import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.DocTerms;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.config.EngineTuningConfig;
import cn.lxdb.plugins.muqingyu.fptoken.runner.ngram.ByteNgramTokenizer;
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
 * 从文件系统加载行记录，并转换为 {@link DocTerms} 列表。
 *
 * <p>支持两种模式：</p>
 * <ul>
 *   <li>兼容模式：加载阶段直接做 n-gram 分词（TOKENIZED）</li>
 *   <li>原始模式：每行仅保留一段原始字节，后续由处理层决定是否分词（RAW_BYTES）</li>
 * </ul>
 *
 * <p><b>硬限制（避免误用）</b>：</p>
 * <ul>
 *   <li>每个文件最多读取 {@link #MAX_LINES_PER_FILE} 行；超出部分仅计入 {@code droppedByCap}，不会进入 rows。</li>
 *   <li>每行 UTF-8 字节最多保留 {@link #MAX_BYTES_PER_LINE} 字节；超长会截断并计入 {@code truncatedLines}。</li>
 *   <li>docId 由加载顺序从 0 递增分配（跨文件连续）。</li>
 * </ul>
 */
public final class LineRecordDatasetLoader {

    public static final int MAX_LINES_PER_FILE = EngineTuningConfig.DEFAULT_MAX_LINES_PER_FILE;
    public static final int MAX_BYTES_PER_LINE = EngineTuningConfig.DEFAULT_MAX_BYTES_PER_LINE;

    private LineRecordDatasetLoader() {
    }

    /**
     * 兼容入口：返回按 n-gram 分词后的 rows。
     *
     * <p><b>限制说明</b>：会应用行数/行长上限；返回结果不包含 stats。</p>
     */
    public static LoadedDataset loadSingleFile(Path file, int ngramStart, int ngramEnd) throws IOException {
        return loadSingleFileWithStats(file, ngramStart, ngramEnd).getLoadedDataset();
    }

    /**
     * 兼容入口：返回按 n-gram 分词后的 rows + stats。
     *
     * <p><b>限制说明</b>：{@code ngramStart > 0} 且 {@code ngramEnd >= ngramStart}。</p>
     */
    public static LoadOutcome loadSingleFileWithStats(Path file, int ngramStart, int ngramEnd) throws IOException {
        validateNgramRange(ngramStart, ngramEnd);
        return loadFromFiles(
                singleFileList(file),
                ngramStart,
                ngramEnd,
                RowBuildMode.TOKENIZED
        );
    }

    /**
     * 原始行入口：不做分词，rows 中每条仅包含 1 段原始行字节。
     *
     * <p><b>限制说明</b>：会应用行数/行长上限；返回结果不包含 stats。</p>
     */
    public static LoadedDataset loadSingleFileRaw(Path file) throws IOException {
        return loadSingleFileRawWithStats(file).getLoadedDataset();
    }

    /**
     * 原始行入口：不做分词，返回 rows + stats。
     *
     * <p>适合给 {@code ExclusiveFpRowsProcessingApi} 作为原始 rows 输入。</p>
     */
    public static LoadOutcome loadSingleFileRawWithStats(Path file) throws IOException {
        return loadFromFiles(
                singleFileList(file),
                0,
                0,
                RowBuildMode.RAW_BYTES
        );
    }

    /**
     * 兼容入口：返回按 n-gram 分词后的 rows。
     *
     * <p><b>限制说明</b>：仅扫描目录下普通文件，不递归子目录。</p>
     */
    public static LoadedDataset load(Path dataDir, int ngramStart, int ngramEnd) throws IOException {
        return loadWithStats(dataDir, ngramStart, ngramEnd).getLoadedDataset();
    }

    /**
     * 兼容入口：返回按 n-gram 分词后的 rows + stats。
     *
     * <p><b>限制说明</b>：仅扫描目录下普通文件，不递归子目录。</p>
     */
    public static LoadOutcome loadWithStats(Path dataDir, int ngramStart, int ngramEnd) throws IOException {
        validateNgramRange(ngramStart, ngramEnd);
        return loadFromFiles(
                collectFiles(requireDirectory(dataDir)),
                ngramStart,
                ngramEnd,
                RowBuildMode.TOKENIZED
        );
    }

    /**
     * 原始行入口：不做分词，rows 中每条仅包含 1 段原始行字节。
     *
     * <p><b>限制说明</b>：仅扫描目录下普通文件，不递归子目录。</p>
     */
    public static LoadedDataset loadRaw(Path dataDir) throws IOException {
        return loadRawWithStats(dataDir).getLoadedDataset();
    }

    /**
     * 原始行入口：不做分词，返回 rows + stats。
     *
     * <p><b>限制说明</b>：仅扫描目录下普通文件，不递归子目录。</p>
     */
    public static LoadOutcome loadRawWithStats(Path dataDir) throws IOException {
        return loadFromFiles(
                collectFiles(requireDirectory(dataDir)),
                0,
                0,
                RowBuildMode.RAW_BYTES
        );
    }

    private static LoadOutcome loadFromFiles(
            List<Path> files,
            int ngramStart,
            int ngramEnd,
            RowBuildMode mode
    ) throws IOException {
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
                    PreparedLine preparedLine = prepareLineBytes(line);
                    if (preparedLine.isEmptyLine()) {
                        emptyLines++;
                    }
                    if (preparedLine.isTruncated()) {
                        truncatedLines++;
                    }
                    byte[] bytes = preparedLine.getBytes();
                    rows.add(buildRow(docId++, bytes, ngramStart, ngramEnd, mode));
                }
            }
        }

        Stats stats = new Stats(files.size(), totalLines, rows.size(), emptyLines, truncatedLines, droppedByCap);
        return new LoadOutcome(new LoadedDataset(rows), stats);
    }

    private static PreparedLine prepareLineBytes(String line) {
        byte[] bytes = line.getBytes(StandardCharsets.UTF_8);
        boolean emptyLine = bytes.length == 0;
        boolean truncated = false;
        if (bytes.length > MAX_BYTES_PER_LINE) {
            byte[] capped = new byte[MAX_BYTES_PER_LINE];
            System.arraycopy(bytes, 0, capped, 0, MAX_BYTES_PER_LINE);
            bytes = capped;
            truncated = true;
        }
        return new PreparedLine(bytes, emptyLine, truncated);
    }

    private static DocTerms buildRow(
            int docId,
            byte[] bytes,
            int ngramStart,
            int ngramEnd,
            RowBuildMode mode
    ) {
        if (mode == RowBuildMode.TOKENIZED) {
            return new DocTerms(docId, ByteNgramTokenizer.tokenize(bytes, ngramStart, ngramEnd));
        }
        // RAW_BYTES：仅保存原始行字节；后续由处理层决定是否切词。
        List<byte[]> raw = new ArrayList<byte[]>(1);
        raw.add(bytes);
        return new DocTerms(docId, raw);
    }

    private static Path requireDirectory(Path dataDir) throws IOException {
        if (!Files.isDirectory(dataDir)) {
            throw new IOException("Data directory not found: " + dataDir);
        }
        return dataDir;
    }

    private static List<Path> singleFileList(Path file) throws IOException {
        if (!Files.isRegularFile(file)) {
            throw new IOException("Input file not found: " + file);
        }
        List<Path> files = new ArrayList<Path>(1);
        files.add(file);
        return files;
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

    private static void validateNgramRange(int ngramStart, int ngramEnd) {
        if (ngramStart <= 0) {
            throw new IllegalArgumentException("ngramStart must be > 0");
        }
        if (ngramEnd < ngramStart) {
            throw new IllegalArgumentException("ngramEnd must be >= ngramStart");
        }
    }

    /**
     * 单行预处理结果：记录 UTF-8 编码后字节、是否空行、是否触发行长截断。
     */
    public static final class PreparedLine {
        private final byte[] bytes;
        private final boolean emptyLine;
        private final boolean truncated;

        private PreparedLine(byte[] bytes, boolean emptyLine, boolean truncated) {
            this.bytes = bytes;
            this.emptyLine = emptyLine;
            this.truncated = truncated;
        }

        public byte[] getBytes() {
            return bytes;
        }

        public boolean isEmptyLine() {
            return emptyLine;
        }

        public boolean isTruncated() {
            return truncated;
        }
    }

    /**
     * 中间步骤测试入口：用于对参数校验、单行预处理、行构建步骤做独立断言。
     */
    public static final class IntermediateSteps {
        private IntermediateSteps() {
        }

        public static void validateNgramRange(int ngramStart, int ngramEnd) {
            LineRecordDatasetLoader.validateNgramRange(ngramStart, ngramEnd);
        }

        public static PreparedLine prepareLineBytes(String line) {
            return LineRecordDatasetLoader.prepareLineBytes(line);
        }

        public static DocTerms buildTokenizedRow(
                int docId,
                byte[] bytes,
                int ngramStart,
                int ngramEnd
        ) {
            return LineRecordDatasetLoader.buildRow(
                    docId, bytes, ngramStart, ngramEnd, RowBuildMode.TOKENIZED);
        }

        public static DocTerms buildRawRow(int docId, byte[] bytes) {
            return LineRecordDatasetLoader.buildRow(
                    docId, bytes, 0, 0, RowBuildMode.RAW_BYTES);
        }
    }

    private enum RowBuildMode {
        TOKENIZED,
        RAW_BYTES
    }

    public static final class LoadedDataset {
        private final List<DocTerms> rows;

        private LoadedDataset(List<DocTerms> rows) {
            this.rows = rows;
        }

        /**
         * 返回加载后的 rows。
         *
         * <p><b>注意</b>：该列表用于高性能链路，不做额外防御性复制；调用方应按只读使用。</p>
         */
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

        /** 实际参与扫描的文件数量（仅普通文件）。 */
        public int getFileCount() {
            return fileCount;
        }

        /** 进入读取循环的总行数（不含被 MAX_LINES_PER_FILE 丢弃的超限行）。 */
        public long getTotalLines() {
            return totalLines;
        }

        /** 最终输出 rows 数量。 */
        public int getDocCount() {
            return docCount;
        }

        /** 空行数量（UTF-8 字节长度为 0）。 */
        public long getEmptyLines() {
            return emptyLines;
        }

        /** 触发行长截断（>MAX_BYTES_PER_LINE）的行数量。 */
        public long getTruncatedLines() {
            return truncatedLines;
        }

        /** 被每文件行数上限(MAX_LINES_PER_FILE)丢弃的行数量。 */
        public long getDroppedByCap() {
            return droppedByCap;
        }
    }
}
