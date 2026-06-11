package cn.lxdb.plugins.muqingyu.fptoken.api;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Locale;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.MMapDirectory;

/**
 * 验证 Lucene {@link MMapDirectory} 在大步长随机 seek 后读 1 字节时，操作系统实际读盘量。
 * <p>
 * 本类负责在指定目录下生成测试文件并执行随机读；配合 {@code drop_caches} + {@code iostat} 观察冷 cache。
 *
 * <pre>
 *   sync; echo 3 &gt; /proc/sys/vm/drop_caches   # root，每次冷测前
 *   iostat -x 1 vdb
 *   java ... MmapRandomReadBench /data1/mmap_bench_index 1   # 1 次/秒，便于与 iostat 对齐
 *   java ... MmapRandomReadBench /data1/mmap_bench_index 0   # burst，不限速
 * </pre>
 */
public final class MmapRandomReadBench {

	private static final String DATA_FILE_NAME = "mmap_bench.dat";

	/** 测试文件大小（MiB）；10GiB，便于模拟大索引位图区随机 fault */
	private static final long FILE_SIZE_MIB = 10L * 1024L;

	/** 每次 seek 跳跃（MiB），模拟 fp bitindex tier 大跨度 */
	private static final long STEP_MIB = 32L;

	/** 随机读次数，每次读 1 字节 */
	private static final int READ_COUNT = 32;

	/**
	 * 默认读速率（次/秒）。{@code <= 0} 表示不限速（ burst，难与 iostat 1s 窗口对齐）。
	 * 可通过第 2 个参数覆盖，例如 {@code ... /data1/mmap_bench_index 2} = 2 次/秒。
	 */
	private static final double DEFAULT_READS_PER_SEC = 1.0;

	private static final class ProcIo {
		final long readBytes;
		final long syscr;

		ProcIo(long readBytes, long syscr) {
			this.readBytes = readBytes;
			this.syscr = syscr;
		}
	}

	/**
	 * @param args {@code args[0]} = Lucene 目录路径；{@code args[1]} 可选，读速率（次/秒，0=不限速）
	 */
	public static void main(String[] args) throws Exception {
		if (args == null || args.length < 1 || args.length > 2 || args[0] == null || args[0].isEmpty()) {
			System.err.println(
					"Usage: java cn.lxdb.plugins.muqingyu.fptoken.api.MmapRandomReadBench <directoryPath> [readsPerSec]");
			System.err.println("  readsPerSec: pace reads for iostat compare (default 1.0); 0 = burst/no throttle");
			System.err.println("Example: ... MmapRandomReadBench /data1/mmap_bench_index 1");
			System.exit(1);
		}

		final Path indexDir = Paths.get(args[0]).toAbsolutePath().normalize();
		final double readsPerSec = args.length >= 2 ? parseReadsPerSec(args[1]) : DEFAULT_READS_PER_SEC;
		final long fileSizeBytes = FILE_SIZE_MIB * 1024L * 1024L;
		final long stepBytes = STEP_MIB * 1024L * 1024L;
		final int reads = resolveReadCount(fileSizeBytes, stepBytes, READ_COUNT);

		printHeader(indexDir, fileSizeBytes, stepBytes, reads, readsPerSec);
		ensureDataFile(indexDir, fileSizeBytes);

		try (Directory dir = openDirectory(indexDir)) {
			runBench(dir, fileSizeBytes, stepBytes, reads, readsPerSec);
		}
	}

	private static double parseReadsPerSec(String raw) {
		try {
			return Double.parseDouble(raw.trim());
		} catch (NumberFormatException e) {
			throw new IllegalArgumentException("readsPerSec must be a number (0 = no throttle): " + raw, e);
		}
	}

	/** 在文件范围内可容纳的最大读次数：偏移 0, step, 2*step, ... */
	private static int resolveReadCount(long fileSizeBytes, long stepBytes, int requested) {
		if (stepBytes <= 0 || fileSizeBytes <= 0) {
			return 1;
		}
		final int maxByFile = (int) Math.min(Integer.MAX_VALUE,
				(fileSizeBytes - 1) / stepBytes + 1);
		final int reads = Math.min(requested, maxByFile);
		if (reads < requested) {
			System.out.printf(Locale.ROOT,
					"note: READ_COUNT capped %d -> %d (file %.0f MiB, step %.0f MiB)%n",
					requested, reads, fileSizeBytes / 1024.0 / 1024.0, stepBytes / 1024.0 / 1024.0);
		}
		return Math.max(1, reads);
	}

	private static Directory openDirectory(Path indexDir) throws IOException {
		MMapDirectory mmap = new MMapDirectory(indexDir);
		mmap.setPreload(false);
		return mmap;
	}

	private static void ensureDataFile(Path indexDir, long fileSizeBytes) throws IOException {
		Files.createDirectories(indexDir);
		final Path dataPath = indexDir.resolve(DATA_FILE_NAME);
		if (Files.isRegularFile(dataPath) && Files.size(dataPath) == fileSizeBytes) {
			System.out.printf(Locale.ROOT, "reuse data file %s (%d bytes)%n", dataPath, fileSizeBytes);
			return;
		}
		if (Files.isRegularFile(dataPath)) {
			System.out.printf(Locale.ROOT, "remove stale data file (size %d != %d)%n", Files.size(dataPath),
					fileSizeBytes);
			Files.delete(dataPath);
		}

		System.out.printf(Locale.ROOT, "creating data file %s size=%d bytes ...%n", dataPath, fileSizeBytes);
		try (Directory dir = openDirectory(indexDir)) {
			try (IndexOutput out = dir.createOutput(DATA_FILE_NAME, IOContext.DEFAULT)) {
				final byte[] buf = new byte[8192];
				long written = 0;
				while (written < fileSizeBytes) {
					final int chunk = (int) Math.min(buf.length, fileSizeBytes - written);
					out.writeBytes(buf, 0, chunk);
					written += chunk;
				}
			}
		}
		System.out.println("data file ready.");
	}

	private static void runBench(Directory dir, long fileSizeBytes, long stepBytes, int reads,
			double readsPerSec) throws IOException, InterruptedException {
		if ((reads - 1L) * stepBytes >= fileSizeBytes) {
			throw new IllegalStateException("READ_COUNT * STEP exceeds file size; adjust constants");
		}

		final boolean paced = readsPerSec > 0;
		final long intervalNanos = paced ? (long) (1_000_000_000L / readsPerSec) : 0L;
		if (paced) {
			System.out.printf(Locale.ROOT,
					"paced reads: target %.3f reads/s (interval %.1f ms); run iostat -x 1 <dev> in parallel%n",
					readsPerSec, intervalNanos / 1_000_000.0);
		} else {
			System.out.println("burst reads: no throttle (hard to align with iostat 1s window)");
		}

		try (IndexInput in = dir.openInput(DATA_FILE_NAME, IOContext.READ)) {
			final ProcIo io0 = readProcIo();
			final long wall0 = System.nanoTime();
			int xor = 0;
			long prevReadBytes = io0.readBytes;
			long prevSyscr = io0.syscr;

			for (int i = 0; i < reads; i++) {
				final long readStart = System.nanoTime();
				final long off = (long) i * stepBytes;
				in.seek(off);
				xor ^= in.readByte() & 0xff;

				final ProcIo ioNow = readProcIo();
				final long ioDelta = ioNow.readBytes >= 0 && prevReadBytes >= 0
						? ioNow.readBytes - prevReadBytes
						: -1;
				final long syscrDelta = ioNow.syscr >= 0 && prevSyscr >= 0 ? ioNow.syscr - prevSyscr : -1;
				final long readNs = System.nanoTime() - readStart;

				System.out.printf(Locale.ROOT,
						"read %2d/%d off=%d ioDelta=%d syscrDelta=%d readUs=%d%n",
						i + 1, reads, off, ioDelta, syscrDelta, readNs / 1000L);
				prevReadBytes = ioNow.readBytes;
				prevSyscr = ioNow.syscr;

				if (paced && i + 1 < reads) {
					final long elapsedNs = System.nanoTime() - readStart;
					final long sleepNs = intervalNanos - elapsedNs;
					if (sleepNs > 0) {
						Thread.sleep(sleepNs / 1_000_000L, (int) (sleepNs % 1_000_000L));
					}
				}
			}

			final long elapsedMs = (System.nanoTime() - wall0) / 1_000_000L;
			final ProcIo io1 = readProcIo();

			final long deltaBytes = io1.readBytes - io0.readBytes;
			final long deltaSyscr = io1.syscr - io0.syscr;
			final double avgKiB = reads > 0 ? (deltaBytes / 1024.0) / reads : 0;
			final double achievedReadsPerSec = elapsedMs > 0 ? reads * 1000.0 / elapsedMs : 0;
			final double procIoKiBps = elapsedMs > 0 ? (deltaBytes / 1024.0) * 1000.0 / elapsedMs : 0;

			System.out.println();
			System.out.println("=== benchmark result ===");
			System.out.printf(Locale.ROOT, "directoryType=MMapDirectory%n");
			System.out.printf(Locale.ROOT, "reads=%d stepBytes=%d (%.0f MiB)%n", reads, stepBytes,
					stepBytes / 1024.0 / 1024.0);
			System.out.printf(Locale.ROOT, "targetReadsPerSec=%.3f achievedReadsPerSec=%.3f elapsedMs=%d%n",
					readsPerSec, achievedReadsPerSec, elapsedMs);
			System.out.printf(Locale.ROOT, "checksumXor=%d%n", xor);
			System.out.printf(Locale.ROOT, "/proc/self/io read_bytes: before=%d after=%d delta=%d%n",
					io0.readBytes, io1.readBytes, deltaBytes);
			System.out.printf(Locale.ROOT, "/proc/self/io syscr:       before=%d after=%d delta=%d%n",
					io0.syscr, io1.syscr, deltaSyscr);
			System.out.printf(Locale.ROOT,
					"procIo throughput: %.2f KiB/s (delta over wall clock; compare iostat rkB/s)%n", procIoKiBps);
			System.out.printf(Locale.ROOT,
					"avg read_bytes per 1-byte read (delta/reads) = %.2f KiB (~%.2f x 4KB pages)%n", avgKiB,
					avgKiB / 4.0);
			printInterpretation(avgKiB, paced, procIoKiBps);
		}
	}

	private static void printInterpretation(double avgKiB, boolean paced, double procIoKiBps) {
		System.out.println();
		System.out.println("=== interpret ===");
		System.out.println("Cold test: sync; echo 3 > /proc/sys/vm/drop_caches (root) before each run.");
		System.out.println("Parallel: iostat -x 1 vdb  ->  rkB/s, r/s during paced window");
		if (paced) {
			System.out.printf(Locale.ROOT,
					"Each read ~1/iostat-second: per-line ioDelta should match ~rkB/s when r/s~1.%n");
			System.out.printf(Locale.ROOT,
					"Whole run: procIo %.1f KiB/s vs iostat avg rkB/s over same %d reads.%n", procIoKiBps,
					READ_COUNT);
		} else {
			System.out.println("Burst mode: iostat 1s avg smears many reads; use readsPerSec=1 (or 2) to compare.");
		}
		if (avgKiB <= 0.5) {
			System.out.println("delta~0: all from page cache; drop caches and re-run.");
		} else if (avgKiB <= 16) {
			System.out.println("~4-16 KiB/read: typical random mmap fault (~1-4 pages), NOT full read_ahead_kb.");
		} else if (avgKiB >= 3000) {
			System.out.println("~MB/read: readahead or large IO per fault; check read_ahead_kb.");
		} else {
			System.out.println("middle range: partial readahead; compare with iostat rkB/r.");
		}
	}

	private static ProcIo readProcIo() {
		final Path p = Paths.get("/proc/self/io");
		if (!Files.isReadable(p)) {
			return new ProcIo(-1, -1);
		}
		long readBytes = -1;
		long syscr = -1;
		try {
			for (String line : Files.readAllLines(p, StandardCharsets.US_ASCII)) {
				if (line.startsWith("read_bytes:")) {
					readBytes = Long.parseLong(line.substring("read_bytes:".length()).trim());
				} else if (line.startsWith("syscr:")) {
					syscr = Long.parseLong(line.substring("syscr:".length()).trim());
				}
			}
		} catch (IOException e) {
			return new ProcIo(-1, -1);
		}
		return new ProcIo(readBytes, syscr);
	}

	private static void printHeader(Path indexDir, long fileSizeBytes, long stepBytes, int reads,
			double readsPerSec) {
		System.out.println("=== MmapRandomReadBench (Lucene MMapDirectory) ===");
		System.out.printf(Locale.ROOT, "java.version=%s%n", System.getProperty("java.version"));
		System.out.printf(Locale.ROOT, "indexDir=%s%n", indexDir);
		System.out.printf(Locale.ROOT, "dataFile=%s%n", DATA_FILE_NAME);
		System.out.printf(Locale.ROOT, "fileSize=%d bytes (%.0f MiB)%n", fileSizeBytes,
				fileSizeBytes / 1024.0 / 1024.0);
		System.out.printf(Locale.ROOT, "step=%d bytes (%.0f MiB) reads=%d%n", stepBytes,
				stepBytes / 1024.0 / 1024.0, reads);
		System.out.printf(Locale.ROOT, "readsPerSec=%.3f (%s)%n", readsPerSec,
				readsPerSec > 0 ? "paced" : "burst");
	}

	private MmapRandomReadBench() {
	}
}
