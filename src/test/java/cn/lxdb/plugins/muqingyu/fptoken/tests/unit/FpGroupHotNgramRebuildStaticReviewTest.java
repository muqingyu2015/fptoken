package cn.lxdb.plugins.muqingyu.fptoken.tests.unit;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;

/**
 * 静态回归：记录 {@code buildFinalHotTerms} 对旧热词 doc 列表的处理方式，便于与集成测试对照。
 */
class FpGroupHotNgramRebuildStaticReviewTest {

	@Test
	void buildFinalHotTerms_replacesExistingHotWithEmptyDocLists_documentedPattern() throws Exception {
		Path root = Path.of(System.getProperty("user.dir"));
		Path src = root.resolve(
				"src/cn/lxdb/plugins/muqingyu/fptoken/dataset/block/FpGroupHotNgramRebuild.java");
		String text = Files.readString(src, StandardCharsets.UTF_8);
		assertTrue(text.contains("out.put(e.getKey(), new FPDocList(maxDoc))"),
				"expected pattern that drops prior hot FPDocList contents before mergeCommonDocsIntoFinalHot");
	}
}
