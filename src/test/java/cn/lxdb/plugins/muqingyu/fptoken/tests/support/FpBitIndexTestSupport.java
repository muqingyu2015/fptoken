package cn.lxdb.plugins.muqingyu.fptoken.tests.support;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.BytesRef;

import cn.lxdb.plugins.muqingyu.fptoken.config.FpTokenBlockLevelPolicy;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.block.FpGroupHotNgramBitIndex;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.block.FpGroupHotNgramRebuild;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.block.FpGroupHotNgramRebuild.CommonTermSortEntry;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FPDocList;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpBlockInfo;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpStatNgram;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpTermKey;
import cn.lxdb.plugins.muqingyu.fptoken.ngram.Counter;
import cn.lxdb.plugins.muqingyu.fptoken.pool.FpHashMapPoolHub;
import cn.lxdb.plugins.muqingyu.fptoken.pool.FpHashMapPoolIds;

/** 构建 bitindex / ngram 统计测试夹具（不依赖 patched {@code FpGroupDataRebuild}）。 */
public final class FpBitIndexTestSupport {

	private FpBitIndexTestSupport() {
	}

	public static FpTermKey termKey(String payload) {
		return FpTermKey.copyOf(new BytesRef(payload.getBytes(StandardCharsets.UTF_8)));
	}

	public static void addCommonTerm(HashMap<FpTermKey, FPDocList> common, int maxDoc, String payload, int... docs) {
		final FPDocList docList = new FPDocList(maxDoc);
		for (int doc : docs) {
			docList.addDoc(doc);
		}
		common.put(termKey(payload), docList);
	}

	/** 生成 N 条均含子串 {@code ab} 的 common term（不同 term 键，便于累加 ngram 计数）。 */
	public static void addCommonTermsSharingNgram(HashMap<FpTermKey, FPDocList> common, int maxDoc, int count,
			String prefix, String suffix) {
		for (int i = 0; i < count; i++) {
			addCommonTerm(common, maxDoc, prefix + "ab" + suffix + (char) ('a' + (i % 26)), i);
		}
	}

	public static FpGroupHotNgramBitIndex buildBitIndexFromCommonPayloads(List<String> commonPayloads)
			throws Exception {
		final int maxDoc = 512;
		final HashMap<FpTermKey, FPDocList> hotMap = new HashMap<>();
		final ArrayList<Entry<FpTermKey, FPDocList>> hotOrdered = new ArrayList<>();
		final ArrayList<CommonTermSortEntry> commonOrder = new ArrayList<>();
		int ord = 0;
		final Constructor<CommonTermSortEntry> entryCtor = CommonTermSortEntry.class.getDeclaredConstructor(
				FpTermKey.class, FPDocList.class, int.class, int.class);
		entryCtor.setAccessible(true);
		for (String payload : commonPayloads) {
			final FpTermKey key = termKey(payload);
			final FPDocList docs = new FPDocList(maxDoc);
			docs.addDoc(ord);
			commonOrder.add(entryCtor.newInstance(key, docs, Integer.MAX_VALUE, ord++));
		}
		return FpGroupHotNgramBitIndex.execute1(FpTokenBlockLevelPolicy.BLOCK_LEVEL_LOW, null, hotMap, hotOrdered,
				commonOrder);
	}

	public static final class RamFlushedBitIndex {
		public final Directory dir;
		public final FpBlockInfo blockInfo;

		public RamFlushedBitIndex(Directory dir, FpBlockInfo blockInfo) {
			this.dir = dir;
			this.blockInfo = blockInfo;
		}
	}

	public static RamFlushedBitIndex flushBitIndexToRam(FpGroupHotNgramBitIndex bitIndex, int docCount)
			throws IOException {
		final Directory dir = new RAMDirectory();
		final FpBlockInfo blockInfo;
		try (IndexOutput out = dir.createOutput("bits", IOContext.DEFAULT)) {
			blockInfo = bitIndex.flushto(out, "test", new BytesRef("col_bfp"), docCount);
		}
		return new RamFlushedBitIndex(dir, blockInfo);
	}

	public static IndexInput openRamBits(Directory dir) throws IOException {
		return dir.openInput("bits", IOContext.DEFAULT);
	}

	public static void resetPools() {
		FpHashMapPoolHub.resetForTests();
	}

	public static int poolLeased(int poolId) {
		if (!FpHashMapPoolHub.isRegistered(poolId)) {
			return 0;
		}
		return FpHashMapPoolHub.stats(poolId).leasedSize();
	}

	public static int defaultLowThreshold() {
		return FpTokenBlockLevelPolicy.get_common_to_hot_threshold(FpTokenBlockLevelPolicy.BLOCK_LEVEL_LOW);
	}

	public static HashMap<FpTermKey, Counter> invokeCountNgram(int hotThreshold, FpStatNgram stat,
			HashMap<FpTermKey, FPDocList> common, HashMap<FpTermKey, Counter> out) throws ReflectiveOperationException {
		final var lease = FpHashMapPoolHub.borrow(FpHashMapPoolIds.ngramOccurrenceCount, FpTermKey.class,
				Counter.class, 256);
		final HashMap<FpTermKey, Counter> map = lease.map();
		try {
			final Method method = FpGroupHotNgramRebuild.class.getDeclaredMethod("countNgramOccurrencesInCommon",
					int.class, FpStatNgram.class, HashMap.class, HashMap.class);
			method.setAccessible(true);
			method.invoke(null, hotThreshold, stat, common, map);
			if (out != null) {
				out.putAll(map);
			}
			return map;
		} finally {
			FpHashMapPoolHub.release(lease);
		}
	}

	public static HashMap<FpTermKey, FPDocList> invokeBuildHotTerms(int hotThreshold, int maxDoc,
			HashMap<FpTermKey, Counter> counts) throws ReflectiveOperationException {
		final HashMap<FpTermKey, FPDocList> hotTerms = new HashMap<>();
		final HashMap<FpTermKey, Object> anchorIndex = new HashMap<>();
		final Method method = FpGroupHotNgramRebuild.class.getDeclaredMethod("buildHotTermsAndAnchorTierIndex",
				HashMap.class, FpStatNgram.class, HashMap.class, long.class, int.class, HashMap.class);
		method.setAccessible(true);
		method.invoke(null, hotTerms, new FpStatNgram(), counts, (long) hotThreshold, maxDoc, anchorIndex);
		return hotTerms;
	}

	public static List<String> hotPayloads(HashMap<FpTermKey, FPDocList> hotTerms) {
		final ArrayList<String> out = new ArrayList<>();
		for (FpTermKey key : hotTerms.keySet()) {
			out.add(key.bytesRef().utf8ToString());
		}
		return out;
	}
}
