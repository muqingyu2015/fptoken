package cn.lxdb.plugins.muqingyu.fptoken.tests.unit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.IOException;

import org.apache.lucene.util.BytesRef;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import cn.lxdb.plugins.muqingyu.fptoken.dataset.block.FpGroupDataRebuild;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.block.FpGroupHotNgramRebuild;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FPDocList;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpTermKey;

/**
 * 集成：高阈值且无 common 时，execute 不应清空热词表键；doc 列表由 mergeCommonDocsIntoFinalHot 从 common 回填。
 */
@Tag("lxdb-runtime")
class FpGroupHotNgramRebuildIntegrationTest {

	@Test
	void execute_highThreshold_noCommon_keepsHotKeys_docListFilledOnlyFromCommon() throws IOException {
		FpGroupDataRebuild group = new FpGroupDataRebuild(100);
		FpTermKey hotKey = FpTermKey.copyOf(new BytesRef(new byte[] { 0x0A, 0x0B }));
		FPDocList hotDocs = new FPDocList(100);
		hotDocs.addDoc(42);
		group.hotTermMapInternal().put(hotKey, hotDocs);

		FpGroupHotNgramRebuild.execute(group, null, 999_999);

		FPDocList after = group.hotTermMapInternal().get(hotKey);
		assertNotNull(after, "hot term key should remain in map after rebuild");
		assertEquals(0, after.docsize(),
				"postings are rebuilt from common via mergeCommonDocsIntoFinalHot, not copied from pre-rebuild hot");
	}
}
