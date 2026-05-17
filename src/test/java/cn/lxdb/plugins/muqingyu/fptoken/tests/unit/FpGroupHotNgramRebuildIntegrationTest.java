package cn.lxdb.plugins.muqingyu.fptoken.tests.unit;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;

import org.apache.lucene.util.BytesRef;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import cn.lxdb.plugins.muqingyu.fptoken.dataset.block.FpGroupDataRebuild;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.block.FpGroupHotNgramRebuild;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FPDocList;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpTermKey;

/**
 * 集成级：验证重建后是否保留分析阶段已存在的 hot posting。
 */
@Tag("lxdb-runtime")
class FpGroupHotNgramRebuildIntegrationTest {

	@Test
	void execute_preservesExistingHotDocList_whenNoCommonOverlap() throws IOException {
		FpGroupDataRebuild group = new FpGroupDataRebuild(100);
		FpTermKey hotKey = FpTermKey.copyOf(new BytesRef(new byte[] { 0x0A, 0x0B }));
		FPDocList hotDocs = new FPDocList(100);
		hotDocs.addDoc(42);
		group.hotTermMapInternal().put(hotKey, hotDocs);

		FpGroupHotNgramRebuild.execute(group, null, 999_999);

		FPDocList after = group.hotTermMapInternal().get(hotKey);
		assertEquals(1, after.docsize(), "pre-existing hot docs should survive rebuild");
	}
}
