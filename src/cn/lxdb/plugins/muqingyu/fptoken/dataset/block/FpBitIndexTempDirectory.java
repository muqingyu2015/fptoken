package cn.lxdb.plugins.muqingyu.fptoken.dataset.block;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.slf4j.Logger;

import cn.lxdb.plugins.muqingyu.fptoken.temp.RecursiveFileDeleter;
import cn.lxdb.plugins.muqingyu.fptoken.temp.ScheduledTempCleanup;
import cn.lucene.lxdb.params.LxdbLogerEncrypt;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpLog;

/**
 * FP bitindex 段内合并写用的本地临时目录（对齐 {@code MarkingTempDirectory}）。
 * <p>
 * 根目录 {@link #baseDirector} 下由 {@link ScheduledTempCleanup} 创建各字段/段会话子目录；
 * 首次 {@link #startCleanupService()} 启动后台线程，周期性清扫残留垃圾目录。
 * <p>
 * 单次字段写：{@link #openSession(String)} → merge 完成后 {@link Session#close()} 立即删本会话目录。
 */
public final class FpBitIndexTempDirectory {

	public static final Logger LOG = LxdbLogerEncrypt.getLogger("mqy.fptoken");

	private static final String BASE_PATH_SUFFIX = "/lxfs/lxtmp/fptoken/";

	/** 单例，与 MarkingTempDirectory.INS 同模式 */
	public static final FpBitIndexTempDirectory INS = new FpBitIndexTempDirectory();

	private final ScheduledTempCleanup cleanup;
	public final String baseDirector;
	private volatile boolean cleanupServiceStarted;

	private FpBitIndexTempDirectory() {
		final String storePath = resolveStorePath();
		String mark = System.getProperty("lxdb.dn.mark");
		if (mark == null || mark.isEmpty()) {
			mark = "dnx";
		}
		this.baseDirector = String.valueOf(storePath).trim() + BASE_PATH_SUFFIX + mark + "/";
		this.cleanup = new ScheduledTempCleanup(LOG, this.baseDirector);
	}

	private static String resolveStorePath() {
		try {
			return cn.lucene.lxdb.pool.LxdbConfig.getEnvPath("LXDB_STORE_PATH");
		} catch (Throwable ignored) {
			final String p = System.getProperty("LXDB_STORE_PATH");
			if (p != null && !p.isEmpty()) {
				return p;
			}
			return System.getProperty("java.io.tmpdir", ".");
		}
	}

	/**
	 * 启动定时清理服务（进程内仅启动一次后台线程）。
	 * 首次 {@link #openSession} 时自动调用。
	 */
	public void startCleanupService() {
		if (cleanupServiceStarted) {
			return;
		}
		synchronized (this) {
			if (!cleanupServiceStarted) {
				cleanup.startCleanupService();
				cleanupServiceStarted = true;
				final StringBuilder sb = FpLog.kv();
				FpLog.append(sb, "event", "bitindexTempCleanupServiceStarted");
				FpLog.append(sb, "baseDirector", baseDirector);
				FpLog.infoLine(LOG, FpLog.TAG_BITINDEX, sb);
			}
		}
	}

	/** 为本字段一次 writeTerms 打开暂存会话。 */
	public Session openSession(String fieldLabel) throws IOException {
		startCleanupService();
		return new Session(fieldLabel);
	}

	static String sanitize(String fieldLabel) {
		if (fieldLabel == null || fieldLabel.isEmpty()) {
			return "field";
		}
		final StringBuilder sb = new StringBuilder(fieldLabel.length());
		for (int i = 0; i < fieldLabel.length(); i++) {
			final char c = fieldLabel.charAt(i);
			if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '-' || c == '_') {
				sb.append(c);
			} else {
				sb.append('_');
			}
		}
		return sb.length() == 0 ? "field" : sb.toString();
	}

	/** 单次 writeTerms 的暂存目录；close 后删除本会话。 */
	public final class Session implements AutoCloseable {

		private final File sessionRoot;

		Session(String fieldLabel) throws IOException {
			this.sessionRoot = cleanup.createTmpDirectory(sanitize(fieldLabel));
		}

		public Path createGroupDir(int groupId) throws IOException {
			return Files.createDirectories(sessionRoot.toPath().resolve("g_" + groupId));
		}

		public Directory openGroupDirectory(int groupId) throws IOException {
			return FSDirectory.open(createGroupDir(groupId));
		}

		public Path sessionRootPath() {
			return sessionRoot.toPath();
		}

		@Override
		public void close() {
			if (sessionRoot == null || !sessionRoot.exists()) {
				return;
			}
			RecursiveFileDeleter.deleteRecursively(LOG, sessionRoot);
			if (!sessionRoot.exists()) {
				final StringBuilder sb = FpLog.kv();
				FpLog.append(sb, "event", "bitindexSessionCleaned");
				FpLog.append(sb, "path", sessionRoot.getAbsolutePath());
				FpLog.infoLine(LOG, FpLog.TAG_BITINDEX, sb);
			} else {
				final StringBuilder sbWarn = FpLog.kv();
				FpLog.append(sbWarn, "event", "bitindexSessionCleanupIncomplete");
				FpLog.append(sbWarn, "path", sessionRoot.getAbsolutePath());
				FpLog.append(sbWarn, "note", "ScheduledTempCleanup will retry");
				LOG.warn(FpLog.line(FpLog.TAG_BITINDEX, sbWarn));
			}
		}
	}
}
