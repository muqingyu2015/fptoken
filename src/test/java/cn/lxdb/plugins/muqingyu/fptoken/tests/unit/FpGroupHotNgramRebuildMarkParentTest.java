package cn.lxdb.plugins.muqingyu.fptoken.tests.unit;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Constructor;
import java.util.HashMap;

import org.apache.lucene.util.BytesRef;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import cn.lxdb.plugins.muqingyu.fptoken.config.Lucene80FPSearchConfig;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.block.FpGroupHotNgramRebuild;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FPDocList;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpStatNgram;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpTermKey;

@Tag("lxdb-runtime")
class FpGroupHotNgramRebuildMarkParentTest {

	@Test
	void markParentPrefixesSkippedInCommonTerm_marksWithinBudget() throws Exception {
		final FpStatNgram stat = new FpStatNgram();
		@SuppressWarnings("rawtypes")
		final HashMap hotMergeTable = new HashMap();
		@SuppressWarnings("unchecked")
		final HashMap<FpTermKey, Integer>[] budgetByLen = new HashMap[Lucene80FPSearchConfig.NGRAM_MAX + 1];
		for (int i = 0; i < budgetByLen.length; i++) {
			budgetByLen[i] = new HashMap<>();
		}
		final boolean[] anchorLenHasBudget = new boolean[Lucene80FPSearchConfig.NGRAM_MAX + 1];

		final byte[] bytes = "abcd".getBytes();
		final FpTermKey parent = FpTermKey.copyOf(new BytesRef(bytes, 0, 2));
		budgetByLen[2].put(parent, 3);
		anchorLenHasBudget[2] = true;
		hotMergeTable.put(parent, newHotMergeSlot(0));

		final boolean[] merged = new boolean[1];
		final BytesRef childSlice = new BytesRef(bytes, 0, 4);
		FpGroupHotNgramRebuild.markParentPrefixesSkippedInCommonTerm(stat, childSlice, merged, hotMergeTable,
				budgetByLen, anchorLenHasBudget);
		assertTrue(merged[0]);
		assertTrue(stat.ngram_level_ok > 0);
	}

	private static Object newHotMergeSlot(int ord) throws Exception {
		Class<?> slotClass = Class.forName(
				"cn.lxdb.plugins.muqingyu.fptoken.dataset.block.FpGroupHotNgramRebuild$HotMergeSlot");
		Constructor<?> ctor = slotClass.getDeclaredConstructor(FPDocList.class, int.class);
		ctor.setAccessible(true);
		return ctor.newInstance(new FPDocList(16), ord);
	}
}
