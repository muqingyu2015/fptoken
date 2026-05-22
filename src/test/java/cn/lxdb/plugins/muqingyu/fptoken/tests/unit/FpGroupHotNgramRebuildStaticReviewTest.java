package cn.lxdb.plugins.muqingyu.fptoken.tests.unit;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;

/**
 * 静态回归：热词重建含 maxDown 层级与 common 内 merge skip。
 */
class FpGroupHotNgramRebuildStaticReviewTest {

	@Test
	void rebuild_includesMaxDownLevelsAndMergeSkipByParentPrefix() throws Exception {
		Path root = Path.of(System.getProperty("user.dir"));
		Path rebuild = root.resolve(
				"src/cn/lxdb/plugins/muqingyu/fptoken/dataset/block/FpGroupHotNgramRebuild.java");
		Path group = root.resolve(
				"src/cn/lxdb/plugins/muqingyu/fptoken/dataset/block/FpGroupDataRebuild.java");
		String rebuildText = Files.readString(rebuild, StandardCharsets.UTF_8);
		String groupText = Files.readString(group, StandardCharsets.UTF_8);

		assertTrue(rebuildText.contains("computeMaxDownLevels"),
				"expected hotTermToLevel / maxDown computation");
		assertTrue(rebuildText.contains("markParentPrefixesSkippedInCommonTerm"),
				"expected per-common merge skip by parent maxDown");
		assertTrue(rebuildText.contains("hotTermToLevel"),
				"expected hotTermToLevel in rebuild javadoc");
		assertTrue(groupText.contains("hotTermToLevel"),
				"expected hotTermToLevel map on FpGroupDataRebuild");
	}
}
