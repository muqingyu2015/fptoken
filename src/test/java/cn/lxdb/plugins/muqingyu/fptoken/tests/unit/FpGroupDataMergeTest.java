package cn.lxdb.plugins.muqingyu.fptoken.tests.unit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.lucene.util.BytesRef;
import org.junit.jupiter.api.Test;

import cn.lxdb.plugins.muqingyu.fptoken.dataset.block.FpGroupDataOriginal;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.block.FpGroupDataRebuild;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FPDocList;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpTermKey;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpTokenTermLayout;
import cn.lxdb.plugins.muqingyu.fptoken.tests.support.FpTestColumnNames;
import cn.lxdb.plugins.muqingyu.fptoken.tests.support.FpTestFixtures;

/** {@link FpGroupDataOriginal#mergeIntoRebuild} 降级合并。 */
class FpGroupDataMergeTest {

	@Test
	void mergeIntoRebuild_unionsDistinctDocs() throws Exception {
		final FpGroupDataRebuild target = new FpGroupDataRebuild(100);
		final FpGroupDataOriginal original = new FpGroupDataOriginal(100);

		FpTestFixtures.putCommonTerm(target, "alpha", 1, 3);
		putOriginalCommon(original, "beta", 2, 2, 4);

		original.mergeIntoRebuild(target);

		assertEquals(4, target.distinctDocUnion.cardinality());
	}

	@Test
	void mergeIntoRebuild_unionsSamePayloadDocs() throws Exception {
		final FpGroupDataRebuild target = new FpGroupDataRebuild(100);
		final FpGroupDataOriginal original = new FpGroupDataOriginal(100);

		putOriginalCommon(original, "shared", 1, 1, 2, 3, 4);

		original.mergeIntoRebuild(target);

		final FPDocList merged = target.commonTermToDocs.get(FpTestFixtures.termKey("shared"));
		assertTrue(merged != null);
		assertEquals(Arrays.asList(1, 2, 3, 4), sortedDocs(merged));
	}

	@Test
	void mergeIntoRebuild_clearsOriginalGroup() throws Exception {
		final FpGroupDataRebuild target = new FpGroupDataRebuild(64);
		final FpGroupDataOriginal original = new FpGroupDataOriginal(64);
		putOriginalCommon(original, "x", 1, 5);

		original.mergeIntoRebuild(target);

		assertEquals(0, original.termCount());
	}

	/** Original 组键带 FP 头（与 {@link FpGroupDataOriginal#ingestTermPostings} 一致）。 */
	private static void putOriginalCommon(FpGroupDataOriginal group, String payload, int termIndex, int... docs) {
		final byte[] buf = new byte[256];
		final BytesRef fullTerm = new BytesRef(buf);
		FpTokenTermLayout.make_fp_term(fullTerm, FpTestColumnNames.DEFAULT, (short) 0, 1, (byte) 1, false, termIndex,
				false, (byte) 0, new BytesRef(payload.getBytes(java.nio.charset.StandardCharsets.US_ASCII)));
		final FPDocList list = new FPDocList(group.maxDocInternal());
		for (int doc : docs) {
			group.distinctDocUnion.set(doc);
			list.addDoc(doc);
		}
		group.commonTermMapInternal().put(FpTermKey.copyOf(fullTerm), list);
	}

	private static List<Integer> sortedDocs(FPDocList list) throws Exception {
		final List<Integer> docs = new ArrayList<>();
		list.foreach(docs::add);
		docs.sort(Integer::compareTo);
		return docs;
	}
}
