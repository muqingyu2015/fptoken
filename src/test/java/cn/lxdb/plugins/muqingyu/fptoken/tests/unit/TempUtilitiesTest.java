package cn.lxdb.plugins.muqingyu.fptoken.tests.unit;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.nio.file.Files;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.lxdb.plugins.muqingyu.fptoken.temp.MillisecondClock;
import cn.lxdb.plugins.muqingyu.fptoken.temp.RecursiveFileDeleter;
import cn.lxdb.plugins.muqingyu.fptoken.temp.ScheduledTempCleanup;
import cn.lxdb.plugins.muqingyu.fptoken.temp.TimeFormatter;

class TempUtilitiesTest {

	private static final Logger LOG = LoggerFactory.getLogger(TempUtilitiesTest.class);

	@Test
	void millisecondClock_nowPositive() {
		assertTrue(MillisecondClock.CLOCK.now() > 0);
	}

	@Test
	void timeFormatter_yyyyMMddHHmm_hasDigits() {
		String s = TimeFormatter.get_yyyyMMddHHmm().format(new java.util.Date(MillisecondClock.CLOCK.now()));
		assertNotNull(s);
		assertTrue(s.length() >= 10);
	}

	@Test
	void recursiveFileDeleter_deletesTree(@TempDir File root) throws Exception {
		File sub = new File(root, "a/b.txt");
		Files.createDirectories(sub.getParentFile().toPath());
		Files.writeString(sub.toPath(), "x");
		RecursiveFileDeleter.deleteRecursively(LOG, root);
		assertFalse(root.exists());
	}

	@Test
	void scheduledTempCleanup_createTmpDirectory(@TempDir File base) throws Exception {
		ScheduledTempCleanup cleanup = new ScheduledTempCleanup(LOG, base.getAbsolutePath() + "/");
		File dir = cleanup.createTmpDirectory("ut");
		assertTrue(dir.isDirectory());
		assertTrue(dir.getAbsolutePath().contains("hourhash"));
	}
}
