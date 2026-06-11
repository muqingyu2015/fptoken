package cn.lxdb.plugins.muqingyu.fptoken.temp;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.slf4j.Logger;

/**
 * 递归删除目录/文件。
 */
public final class RecursiveFileDeleter {

	private RecursiveFileDeleter() {
	}

	public static void deleteRecursively(Logger logger, File file) {
		if (file == null) {
			if (logger != null) {
				logger.error("Input file is null");
			}
			return;
		}
		if (!file.exists()) {
			if (logger != null) {
				logger.warn("Target does not exist: {}", file.getAbsolutePath());
			}
			return;
		}
		try {
			if (file.isFile()) {
				if (!file.delete() && logger != null) {
					logger.warn("Failed to delete file: {}", file.getAbsolutePath());
				}
				return;
			}
			final Path root = file.toPath();
			Files.walk(root).sorted((a, b) -> b.compareTo(a)).forEach(p -> {
				try {
					Files.deleteIfExists(p);
				} catch (IOException e) {
					if (logger != null) {
						logger.warn("Failed to delete: {}", p, e);
					}
				}
			});
		} catch (Throwable e) {
			if (logger != null) {
				logger.error("deleteRecursively failed: {}", file.getAbsolutePath(), e);
			}
		}
	}
}
