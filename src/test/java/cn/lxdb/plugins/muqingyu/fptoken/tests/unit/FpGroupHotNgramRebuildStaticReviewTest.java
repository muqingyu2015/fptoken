package cn.lxdb.plugins.muqingyu.fptoken.tests.unit;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;

/**
 * 静态回归：buildFinalHotTerms 对旧热词先占位空 FPDocList，再由 mergeCommonDocsIntoFinalHot 从 common 合并 doc。
 */
class FpGroupHotNgramRebuildStaticReviewTest {

	@Test
	void buildFinalHotTerms_seedsEmptyDocLists_beforeCommonMerge() throws Exception {
		Path root = Path.of(System.getProperty("user.dir"));
		Path src = root.resolve(
				"src/cn/lxdb/plugins/muqingyu/fptoken/dataset/block/FpGroupHotNgramRebuild.java");
		String text = Files.readString(src, StandardCharsets.UTF_8);
		assertTrue(text.contains("out.put(e.getKey(), new FPDocList(maxDoc))"),
				"expected: old hot keys kept with empty lists until mergeCommonDocsIntoFinalHot");
		assertTrue(text.contains("mergeCommonDocsIntoFinalHot"),
				"expected: doc postings filled from common n-gram merge");
	}
}
