package cn.lxdb.plugins.muqingyu.fptoken.tests.support;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import org.apache.lucene.util.BytesRef;
import org.junit.jupiter.params.provider.Arguments;

import cn.lxdb.plugins.muqingyu.fptoken.config.Lucene80FPSearchConfig;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.block.FpGroupDataRebuild;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.block.FpGroupHotNgramBitIndex;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.block.FpGroupHotNgramRebuild;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FPDocList;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpBlockInfo;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpTermKey;

/** 参数化测试用例源（读写对齐场景批量生成）。 */
public final class FpParameterizedTestSources {

	private FpParameterizedTestSources() {
	}

	/** 6 个 ngram 长度档 × 256 桶 = 1536。 */
	public static Stream<Arguments> allNgramLengthIndexAndBucket() {
		final List<Arguments> out = new ArrayList<>(Lucene80FPSearchConfig.NGRAM_MAX * Lucene80FPSearchConfig.BUCKETS);
		for (int li = 0; li < Lucene80FPSearchConfig.NGRAM_MAX; li++) {
			for (int bucket = 0; bucket < Lucene80FPSearchConfig.BUCKETS; bucket++) {
				out.add(Arguments.of(li, bucket));
			}
		}
		return out.stream();
	}

	/** 单字节桶：0..255，与 {@link FpGroupHotNgramBitIndex#bucketIndex} 写读一致。 */
	public static Stream<Arguments> allUnsignedBytes() {
		final List<Arguments> out = new ArrayList<>(256);
		for (int b = 0; b < 256; b++) {
			out.add(Arguments.of(b));
		}
		return out.stream();
	}

	/** 50×10×2 = 1000 组 FP 头写读参数。 */
	public static Stream<Arguments> layoutRoundTripCases() {
		final List<Arguments> out = new ArrayList<>(1000);
		for (int groupId = 0; groupId < 50; groupId++) {
			for (int termIndex = 1; termIndex <= 10; termIndex++) {
				for (boolean hot : new boolean[] { true, false }) {
					out.add(Arguments.of(groupId, termIndex, hot));
				}
			}
		}
		return out.stream();
	}

	/** 12B 查询前缀与 13B 全头前 12 字节一致：256×8 = 2048。 */
	public static Stream<Arguments> searchPrefixAlignmentCases() {
		final List<Arguments> out = new ArrayList<>(2048);
		for (int groupId = 0; groupId < 256; groupId++) {
			for (int termIndex = 1; termIndex <= 8; termIndex++) {
				out.add(Arguments.of(groupId, termIndex));
			}
		}
		return out.stream();
	}

	/** rebuild 写 + 位图 + search 读：150 组种子。 */
	public static Stream<Arguments> rebuildSearchAlignmentSeeds() {
		final List<Arguments> out = new ArrayList<>(150);
		for (int seed = 0; seed < 150; seed++) {
			out.add(Arguments.of(seed));
		}
		return out.stream();
	}

	/** 透传写编码：100 组 group 重映射。 */
	public static Stream<Arguments> originalPassthroughGroupRemapCases() {
		final List<Arguments> out = new ArrayList<>(100);
		for (int oldGroup = 1; oldGroup <= 100; oldGroup++) {
			out.add(Arguments.of(oldGroup, oldGroup + 10_000));
		}
		return out.stream();
	}

	/**
	 * 构建与 {@link FpSearchWriteReadAlignmentTest#search_findsDocsWrittenByRebuildFlush} 相同结构的
	 * rebuild 组 + 位图 + 内存倒排（需完整 main 编译与 lib/ 补丁 Lucene）。
	 */
	public static FpRebuildSeedIndex buildIndexForSeed(int seed) throws Exception {
		final int maxDoc = 64;
		final FpGroupDataRebuild group = new FpGroupDataRebuild(maxDoc);
		final int[] expectedDocs = fillGroupForSeed(group, maxDoc, seed);
		final byte[] payload = payloadForSeed(seed);

		FpGroupHotNgramRebuild.execute(group, null, 2);
		ensureHotTermMetadata(group);
		final FpGroupHotNgramBitIndex bits = FpGroupHotNgramBitIndex.execute((byte) 1, group);
		final FpBlockInfo info = new FpBlockInfo();
		info.targetLevel = 1;
		final int groupId = 1000 + seed;
		final FpWriteReadTestIndex.Built built = FpWriteReadTestIndex.fromRebuildGroup(group, groupId, (byte) 1, bits,
				info);
		return new FpRebuildSeedIndex(built, payload, expectedDocs);
	}

	static int[] fillGroupForSeed(FpGroupDataRebuild group, int maxDoc, int seed) {
		final byte[] payload = payloadForSeed(seed);
		final FPDocList docs = new FPDocList(maxDoc);
		final int d0 = (seed * 3) % maxDoc;
		final int d1 = (seed * 3 + 1) % maxDoc;
		docs.addDoc(d0);
		if (d1 != d0) {
			docs.addDoc(d1);
		}
		group.commonTermMapInternal().put(FpTermKey.copyOf(new BytesRef(payload)), docs);

		final byte[] payload2 = payloadForSeed(seed + 17);
		int d2 = -1;
		if (!Arrays.equals(payload, payload2)) {
			final FPDocList docs2 = new FPDocList(maxDoc);
			d2 = (seed * 5 + 2) % maxDoc;
			docs2.addDoc(d2);
			group.commonTermMapInternal().put(FpTermKey.copyOf(new BytesRef(payload2)), docs2);
		}

		if (d2 < 0) {
			return d1 != d0 ? new int[] { d0, d1 } : new int[] { d0 };
		}
		return new int[] { d0, d1, d2 };
	}

	/** 热词缺 level 时补 0，避免 {@link FpTermFlushEncoding} 与生产 flushto 空指针。 */
	static void ensureHotTermMetadata(FpGroupDataRebuild group) {
		for (FpTermKey key : group.hotTermMapInternal().keySet()) {
			if (!group.hotTermToLevelInternal().containsKey(key)) {
				group.hotTermToLevelInternal().put(key, 0);
			}
		}
	}

	public static byte[] payloadForSeed(int seed) {
		final int len = 4 + (seed % 3);
		final byte[] p = new byte[len];
		for (int i = 0; i < len; i++) {
			p[i] = (byte) ((seed + i * 31) & 0xFF);
		}
		return p;
	}

	/** 查询用切片：优先 2 字节 ngram，与位图 {@code length-1} 档一致。 */
	public static BytesRef primarySearchSlice(byte[] payload) {
		if (payload.length >= 2) {
			return new BytesRef(payload, 0, 2);
		}
		return new BytesRef(payload, 0, payload.length);
	}

	public static BytesRef firstNgramSlice(byte[] payload, int n) {
		if (payload.length < n) {
			return new BytesRef(payload);
		}
		return new BytesRef(payload, 0, n);
	}
}
