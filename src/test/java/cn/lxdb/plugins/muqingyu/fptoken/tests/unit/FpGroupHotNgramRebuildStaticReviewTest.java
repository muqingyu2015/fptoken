package cn.lxdb.plugins.muqingyu.fptoken.tests.unit;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;

/**
 * 静态回归：热词重建含层级剔除与 hotTermToLevel 维护。
 */
class FpGroupHotNgramRebuildStaticReviewTest {

	@Test
	void rebuild_includesHierarchyStrippingAndHotTermToLevel() throws Exception {
		Path root = Path.of(System.getProperty("user.dir"));
		Path rebuild = root.resolve(
				"src/cn/lxdb/plugins/muqingyu/fptoken/dataset/block/FpGroupHotNgramRebuild.java");
		Path group = root.resolve(
				"src/cn/lxdb/plugins/muqingyu/fptoken/dataset/block/FpGroupDataRebuild.java");
		String rebuildText = Files.readString(rebuild, StandardCharsets.UTF_8);
		String groupText = Files.readString(group, StandardCharsets.UTF_8);

		assertTrue(rebuildText.contains("applyHierarchyDocStripping"),
				"expected hierarchy strip pass after mergeCommonDocsIntoFinalHot");
		assertTrue(rebuildText.contains("fillHotTermLevels"),
				"expected hotTermToLevel population");
		assertTrue(rebuildText.contains("removeAllDocsPresentIn"),
				"expected parent doc strip via FPDocList");
		assertTrue(groupText.contains("hotTermToLevel"),
				"expected hotTermToLevel map on FpGroupDataRebuild");
	}
}
