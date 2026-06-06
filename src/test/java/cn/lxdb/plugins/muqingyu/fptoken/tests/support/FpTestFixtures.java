package cn.lxdb.plugins.muqingyu.fptoken.tests.support;

import org.apache.lucene.util.BytesRef;

import cn.lxdb.plugins.muqingyu.fptoken.dataset.block.FpGroupDataRebuild;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FPDocList;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpTermKey;

/** 单测用最小 FP 组数据构造。 */
public final class FpTestFixtures {

	private FpTestFixtures() {
	}

	public static FpTermKey termKey(byte[] payload) {
		return FpTermKey.copyOf(new BytesRef(payload));
	}

	public static FpTermKey termKey(String ascii) {
		return termKey(ascii.getBytes(java.nio.charset.StandardCharsets.US_ASCII));
	}

	/** 向 rebuild 组写入一条 common 载荷及其 doc 列表。 */
	public static void putCommonTerm(FpGroupDataRebuild group, byte[] payload, int... docs) {
		final FPDocList list = new FPDocList(group.maxDoc);
		for (int doc : docs) {
			if (doc >= 0 && doc < group.maxDoc) {
				group.distinctDocUnion.set(doc);
				list.addDoc(doc);
			}
		}
		group.commonTermToDocs.put(termKey(payload), list);
	}

	public static void putCommonTerm(FpGroupDataRebuild group, String asciiPayload, int... docs) {
		putCommonTerm(group, asciiPayload.getBytes(java.nio.charset.StandardCharsets.US_ASCII), docs);
	}

	/**
	 * 构造 {@code count} 条 common 词，载荷均含子串 {@code sharedNgram}，用于触发 hot 挖掘阈值。
	 */
	public static void putCommonTermsSharingNgram(FpGroupDataRebuild group, String sharedNgram, int count) {
		for (int i = 0; i < count; i++) {
			putCommonTerm(group, "P" + sharedNgram + "S" + i, i + 1);
		}
	}

	public static int[] docs(int... values) {
		return values;
	}
}
