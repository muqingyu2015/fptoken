package cn.lxdb.plugins.muqingyu.fptoken.temp;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;

/**
 * 定时临时文件清理服务（与 marking 模块同源实现）。
 * <p>
 * 1. 创建按小时分组的临时目录<br>
 * 2. 定期清理过期临时文件（保留最近约 15 小时）<br>
 * 3. 线程池异步执行清理
 * <p>
 * 构造时不自动启动；首次 {@link #startCleanupService()} 启动后台线程（5 分钟周期）。
 */
public class ScheduledTempCleanup {

	private static final String DIR_MARK_NAME = "hourhash";
	private static final long CLEANUP_INTERVAL_MS = 300_000L;
	private static final int HISTORY_HOURS_TO_KEEP = 15;
	private static final long TWO_HOURS_MS = 2L * 60L * 60L * 1000L;
	private static final long TEN_MINUTES_MS = 10L * 60L * 1000L;

	private final ExecutorService executorService = Executors.newCachedThreadPool();
	public final String baseDirectoryPath;
	private final AtomicBoolean needStart = new AtomicBoolean(true);
	private final Logger logger;

	public ScheduledTempCleanup(Logger logger, String directoryPath) {
		this.logger = logger;
		this.baseDirectoryPath = directoryPath;
	}

	public File createTmpDirectory(String ruleName) throws IOException {
		final long timestamp = MillisecondClock.CLOCK.now();
		final String yyyyMMddHHmm = TimeFormatter.get_yyyyMMddHHmm().format(new Date(timestamp));
		final String yyyyMMddHHm = yyyyMMddHHmm.substring(0, yyyyMMddHHmm.length() - 1);
		final String dirName = ruleName + "_" + yyyyMMddHHmm + "_" + UUID.randomUUID();
		final File tempDir = new File(this.baseDirectoryPath + "/" + DIR_MARK_NAME + "/" + yyyyMMddHHm, dirName);
		if (!tempDir.mkdirs()) {
			throw new IOException("Failed to create temp directory: " + tempDir.getAbsolutePath());
		}
		logger.info("Created temp directory:" + tempDir);
		return tempDir;
	}

	private void initializeBaseDirectory() {
		final String directoryPath = this.baseDirectoryPath;
		final File baseDir = new File(directoryPath);
		final boolean dirCreated = baseDir.mkdirs();
		final long startTime = MillisecondClock.CLOCK.now();
		logger.info("Initialize base directory：" + dirCreated + "  " + directoryPath);
		final ArrayList<File> oldDirsToClean = new ArrayList<>();
		try {
			final String[] existingDirs = baseDir.list();
			if (existingDirs != null) {
				for (String dirName : existingDirs) {
					if (dirName.startsWith(DIR_MARK_NAME)) {
						final File oldDir = new File(baseDir, dirName);
						final String tmpDirName = dirName + "_tmp_"
								+ UUID.randomUUID().toString().replace("-", "").toLowerCase();
						final File tempDir = new File(baseDir, tmpDirName);
						if (oldDir.renameTo(tempDir)) {
							oldDirsToClean.add(tempDir);
						}
					}
				}
			}
		} catch (Throwable e) {
			logger.info("ee", e);
		} finally {
			logger.info("marking init finish：" + dirCreated + "  " + directoryPath + " timediff:"
					+ (MillisecondClock.CLOCK.now() - startTime));
		}
		executorService.execute(() -> {
			for (File f : oldDirsToClean) {
				logger.info("deleteRecursively:" + f.getAbsolutePath());
				RecursiveFileDeleter.deleteRecursively(logger, f);
			}
		});
	}

	private void cleanOldHourlyDirectories() {
		try {
			final long ts = MillisecondClock.CLOCK.now();
			for (int i = 1; i < HISTORY_HOURS_TO_KEEP; i++) {
				final String yyyyMMddHHmm = TimeFormatter.get_yyyyMMddHHmm()
						.format(new Date(ts - TWO_HOURS_MS - TEN_MINUTES_MS * i));
				final String yyyyMMddHHm = yyyyMMddHHmm.substring(0, yyyyMMddHHmm.length() - 1);
				final File target = new File(baseDirectoryPath + "/" + DIR_MARK_NAME + "/" + yyyyMMddHHm);
				RecursiveFileDeleter.deleteRecursively(logger, target);
				logger.info("clean " + baseDirectoryPath + "/" + DIR_MARK_NAME + "/" + yyyyMMddHHm);
			}
		} catch (Throwable e) {
			logger.error("msg", e);
		}
	}

	private void startPeriodicCleanupTask() {
		executorService.execute(() -> {
			while (true) {
				executorService.execute(this::cleanOldHourlyDirectories);
				try {
					Thread.sleep(CLEANUP_INTERVAL_MS);
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
					return;
				}
			}
		});
	}

	/** 启动清理服务（进程内仅生效一次）。 */
	public void startCleanupService() {
		if (!needStart.compareAndSet(true, false)) {
			return;
		}
		initializeBaseDirectory();
		startPeriodicCleanupTask();
	}
}
