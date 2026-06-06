package cn.lxdb.plugins.muqingyu.fptoken.dataset.block;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RAMOutputStream;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.StringHelper;
import org.slf4j.Logger;

import cn.lucene.lxdb.params.LxdbLogerEncrypt;
import cn.lxdb.plugins.muqingyu.fptoken.config.Lucene80FPSearchConfig;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpBlockInfo;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpTermKey;

/**
 * 组内热词 / 普通词 ngram 精确倒排索引（v3，{@link FpBlockInfo#FORMAT_VERSION} = 3）。
 *
 * <p><b>职责</b>：把组内 hot/common 载荷的所有合法 ngram 切片映射到
 * {@code bucketIndex(ngram bytes)} → {@code orderList}（term 在组内的序号），
 * 供 {@link cn.lxdb.plugins.muqingyu.fptoken.api.FpSearch} 按查询 slice 定位 order 再 seek 倒排。
 *
 * <p><b>与 v2 的区别</b>：废弃 FixedBitSet + 固定 256/512 桶掩码；改为整型域 murmurhash bucket +
 * 稀疏 sorted posting；查询侧可按 bucket 跳跃读盘，避免无效 bit 扫描。
 *
 * <h3>内存结构（构建期 / 全量读入后）</h3>
 * <pre>
 * FpGroupHotNgramBitIndex
 * ├── hot: TierIndex          // 热词 tier
 * ├── common: TierIndex       // 普通词 tier（跳过与 hot 重复的 slice）
 * ├── hotCount, commonCount   // 组内 term 数（元数据）
 * └── sparse: boolean         // true = 仅含 selective 加载的 bucket
 *
 * TierIndex
 * └── rows[6]: LenRow         // lenIdx 0..5 对应 ngram 长度 1..6（{@link Lucene80FPSearchConfig#NGRAM_MAX}）
 *
 * LenRow（全量实例）
 * ├── sortedKeys[int]         // bucketIndex 升序、无重复
 * ├── entryMeta[int]          // 与 sortedKeys 同下标；见下方 entryMeta 编码
 * └── orderArena[byte]        // 多条 order 的 vint 增量序列池（单条 order 内联在 entryMeta）
 *
 * LenRow（稀疏实例，selective 读 / viewSelective）
 * └── sparseOrders: Map&lt;bucketIndex, int[] orders&gt;
 * </pre>
 *
 * <h3>磁盘布局（段内连续两块，由 {@link FpBlockInfo} 定位）</h3>
 * <pre>
 * [ fpBanksHot ──────────────────────────────────────────────── )
 *   Hot Tier
 * [ fpBanksCommon ───────────────────────────────────────────── )
 *   Common Tier
 *
 * 每个 Tier（magic = {@link #TIER_MAGIC} 'FPTR'）：
 * ┌─────────────────────────────────────────────────────────────┐
 * │ magic: int                                                  │
 * │ lenRowOffset[6]: long × 6   // 相对 tier 起点，指向各 LenRow │
 * ├─────────────────────────────────────────────────────────────┤
 * │ LenRow(len=1)  @ lenRowOffset[0]                             │
 * │ LenRow(len=2)  @ lenRowOffset[1]                             │
 * │ ...                                                         │
 * │ LenRow(len=6)  @ lenRowOffset[5]                             │
 * └─────────────────────────────────────────────────────────────┘
 *
 * 每个 LenRow（entryCount = 该长度下非空 bucket 数）：
 * ┌─────────────────────────────────────────────────────────────┐
 * │ entryCount: int                                             │
 * │ skip[count]: (bucketIndex: int, keysStartRel: long) × N     │  N = ceil(entryCount / 128)
 * │ sortedKeys[entryCount]: int                                 │  bucketIndex 升序
 * │ entryMeta[entryCount]: int                                  │
 * │ arenaLen: int                                               │
 * │ orderArena[arenaLen]: byte                                  │
 * └─────────────────────────────────────────────────────────────┘
 *   keysStartRel / metaStartRel / arena 偏移均相对 <b>LenRow 起点</b>（selective 跳跃读依赖此约定）。
 *
 * skip 表：每 {@link #SKIP_INTERVAL}（128）条 posting 一条，记录该段首 bucket 与 keys 区相对偏移；
 * selective 读时对 bucketIndex 二分定位段，再在段内顺序扫描 ≤128 个 key。
 * </pre>
 *
 * <h3>entryMeta 编码（int，低 1 位为 tag）</h3>
 * <pre>
 * tag = 0 ({@link #ENTRY_TAG_SINGLE})：单 order，value = (order &lt;&lt; 1) | 0
 * tag = 1 ({@link #ENTRY_TAG_MULTI}) ：多 order，value = (arenaOffset &lt;&lt; 1) | 1
 *   arena 内：vint(count) + vint(delta order)*  （order 升序、增量 vint 压缩）
 * </pre>
 *
 * <h3>bucketKey（上游 {@code Terms#fpBits(..., long[], long[])}  selective 参数）</h3>
 * <pre>
 * long key = packBucketKey(lenIdx, bucketIndex)
 *          = (lenIdx &lt;&lt; 32) | (bucketIndex &amp; 0xFFFFFFFFL)
 *
 * bucketIndex = murmurhash3_x86_32(ngram bytes)   // 整型域，不再 mod 512/256
 * lenIdx      = ngramLen - 1                      // 1..6 → 0..5
 * </pre>
 *
 * <h3>典型调用链</h3>
 * <pre>
 * 写段：FpGroupDataRebuild → execute() → flushto() → FpBlockInfo
 * 查：  FpSearch.selectiveKeysForSlices(slices)
 *       → terms.fpBits(indexId, groupId, hotKeys, commonKeys)
 *       → readfromBanksSelective(in, blkinfo, hotKeys, commonKeys)
 *       → lookupHotOrders(slice) / lookupCommonOrders(slice) → order[] → seek 倒排
 * </pre>
 *
 * @see FpBlockInfo
 * @see cn.lxdb.plugins.muqingyu.fptoken.api.FpSearch
 */
public final class FpGroupHotNgramBitIndex {
	public static final Logger LOG = LxdbLogerEncrypt.getLogger("mqy.fptoken");

	/** skip 表采样间隔：每 128 条 posting 一条 (bucketIndex, keysStartRel)。 */
	static final int SKIP_INTERVAL = 128;
	/** Tier 魔数 {@code 'FPTR'}，用于校验 hot/common 段边界。 */
	static final int TIER_MAGIC = 0x46505452; // 'FPTR'
	/** entryMeta 低 bit：单 order 内联在 meta 高 31 位。 */
	static final int ENTRY_TAG_SINGLE = 0;
	/** entryMeta 低 bit：多 order，高 31 位为 orderArena 内 vint 列表偏移。 */
	static final int ENTRY_TAG_MULTI = 1;

	private static final int[] EMPTY_ORDERS = new int[0];

	private final TierIndex hot;
	private final TierIndex common;
	private final int hotCount;
	private final int commonCount;
	private final int targetlevel;
	private final boolean sparse;

	private FpGroupHotNgramBitIndex(int targetlevel, TierIndex hot, TierIndex common, int hotCount, int commonCount,
			boolean sparse) {
		this.targetlevel = targetlevel;
		this.hot = hot;
		this.common = common;
		this.hotCount = hotCount;
		this.commonCount = commonCount;
		this.sparse = sparse;
	}

	/** lenIdx 放高 32 位，bucketIndex 放低 32 位。 */
	public static long packBucketKey(int lenIdx, int bucketIndex) {
		return ((long) lenIdx << 32) | (bucketIndex & 0xFFFFFFFFL);
	}

	public static int unpackLenIdx(long key) {
		return (int) (key >>> 32);
	}

	public static int unpackBucketIndex(long key) {
		return (int) key;
	}

	/** 统一 bucketIndex：整型域 murmurhash，不再截断到 512/256。 */
	public static int bucketIndex(byte[] buf, int off, int len) {
		if (len <= 0) {
			return 0;
		}
		return StringHelper.murmurhash3_x86_32(buf, off, len, 0);
	}

	public static int bucketIndex(BytesRef ref) {
		return bucketIndex(ref.bytes, ref.offset, ref.length);
	}


	/**
	 * 从 {@link FpGroupDataRebuild} 构建全量内存索引。
	 * <ul>
	 *   <li>hot：对每条 hot 载荷的所有 ngram 窗口打标（含重复窗口）</li>
	 *   <li>common：对每条 common 载荷的去重 ngram 打标，且跳过已出现在 hot 的 slice</li>
	 * </ul>
	 */
	public static FpGroupHotNgramBitIndex execute(int targetLevel, FpGroupDataRebuild group) {
		final int h = group.hotTermOrderSize();
		final int c = group.commonTermOrderSize();

		final TierIndex hotTier = new TierIndex(false);
		final TierIndex commonTier = new TierIndex(false);

		final TreeMap<FpTermKey, Integer> hotOrder = group.hotTermOrderInternal();
		final TreeMap<FpTermKey, Integer> commonOrder = group.commonTermOrderInternal();

		final HashSet<FpTermKey> hotKeySet = new HashSet<>(Math.max(16, h * 2));
		for (FpTermKey hotKey : hotOrder.keySet()) {
			hotKeySet.add(hotKey);
		}

		for (Entry<FpTermKey, Integer> e : hotOrder.entrySet()) {
			markHotNgramsForPayload(hotTier, e.getKey().bytesRef(), e.getValue().intValue());
		}
		for (Entry<FpTermKey, Integer> e : commonOrder.entrySet()) {
			markCommonNgramsByUniqueSlices(hotKeySet, commonTier, e.getKey().bytesRef(), e.getValue().intValue());
		}

		hotTier.finalizeRows();
		commonTier.finalizeRows();
		return new FpGroupHotNgramBitIndex(targetLevel, hotTier, commonTier, h, c, false);
	}

	/** 顺序写出 hot tier + common tier，返回 {@link FpBlockInfo}（含 fpBanksHot / fpBanksCommon 偏移）。 */
	public FpBlockInfo flushto(IndexOutput out, String from, BytesRef fieldInfo, int docCount) throws IOException {
		final long fpBanksHot = out.getFilePointer();
		final int hotArenaBytes = hot.writeTier(out);
		final long fpBanksCommon = out.getFilePointer();
		final int commonArenaBytes = common.writeTier(out);

		final int bytesPerHotSerialized = (int) (fpBanksCommon - fpBanksHot);
		final int bytesPerCommonSerialized = (int) (out.getFilePointer() - fpBanksCommon);

		final FpBlockInfo info = new FpBlockInfo(fpBanksHot, fpBanksCommon, bytesPerHotSerialized,
				bytesPerCommonSerialized, hotArenaBytes, commonArenaBytes, hotCount, commonCount, this.targetlevel,
				fieldInfo, docCount);

		LOG.info("[fp_bitindex] flush phase=" + from + " perLength=" + formatFlushStats(hot, common) + " block=" + info);
		return info;
	}

	/** 全量读 hot + common 两个 tier（{@code load* = null} 的便捷入口）。 */
	public static FpGroupHotNgramBitIndex readfrom(IndexInput in, FpBlockInfo blkinfo) throws IOException {
		return readfromBanksSelective(in, blkinfo, (long[]) null, (long[]) null);
	}

	/**
	 * 按 bucketKey 选择性读盘。
	 * <ul>
	 *   <li>{@code loadHotKeys/loadCommonKeys} 均为 null → 等同 {@link #readfrom}</li>
	 *   <li>非 null → 仅对 listed bucket 做 skip 定位 + 读 orderList，返回 {@code sparse=true} 实例</li>
	 *   <li>空数组 → 对应 tier 为空 sparse（不读盘）</li>
	 * </ul>
	 */
	public static FpGroupHotNgramBitIndex readfromBanksSelective(IndexInput in, FpBlockInfo blkinfo,
			long[] loadHotKeys, long[] loadCommonKeys) throws IOException {
		if (loadHotKeys == null && loadCommonKeys == null) {
			in.seek(blkinfo.fpBanksHot);
			final TierIndex hotTier = TierIndex.readTier(in, false);
			in.seek(blkinfo.fpBanksCommon);
			final TierIndex commonTier = TierIndex.readTier(in, false);
			return new FpGroupHotNgramBitIndex(blkinfo.targetLevel, hotTier, commonTier, blkinfo.hotCount,
					blkinfo.commonCount, false);
		}
		final TierIndex hotTier = loadHotKeys == null || loadHotKeys.length == 0 ? TierIndex.emptySparse()
				: TierIndex.readSelective(in, blkinfo.fpBanksHot, loadHotKeys);
		final TierIndex commonTier = loadCommonKeys == null || loadCommonKeys.length == 0 ? TierIndex.emptySparse()
				: TierIndex.readSelective(in, blkinfo.fpBanksCommon, loadCommonKeys);
		return new FpGroupHotNgramBitIndex(blkinfo.targetLevel, hotTier, commonTier, blkinfo.hotCount,
				blkinfo.commonCount, true);
	}

	/**
	 * 兼容旧 {@code boolean[][]} 签名：非 null 亦视为全量读（掩码已废弃）。
	 */
	public static FpGroupHotNgramBitIndex readfromBanksSelective(IndexInput in, FpBlockInfo blkinfo,
			boolean[][] loadHot, boolean[][] loadCommon) throws IOException {
		return readfrom(in, blkinfo);
	}

//	/**
//	 * 上游 {@code fpBits(..., int[], int[])} 对接：{@code load*} 为 {@code [lenIdx, bucketIndex, lenIdx, bucketIndex, ...]} 交错数组；
//	 * 若仅传 bucketIndex 单数组，请改用 {@link #readfromBanksSelective(IndexInput, FpBlockInfo, long[], long[])}。
//	 */
//	public static FpGroupHotNgramBitIndex readfromBanksSelective(IndexInput in, FpBlockInfo blkinfo, int[] loadHot,
//			int[] loadCommon) throws IOException {
//		if (loadHot == null && loadCommon == null) {
//			return readfrom(in, blkinfo);
//		}
//		return readfromBanksSelective(in, blkinfo, keysFromIntPairs(loadHot), keysFromIntPairs(loadCommon));
//	}

//	static long[] keysFromIntPairs(int[] lenAndBucketPairs) {
//		if (lenAndBucketPairs == null) {
//			return null;
//		}
//		if ((lenAndBucketPairs.length & 1) != 0) {
//			throw new IllegalArgumentException("load array length must be even [lenIdx,bucketIndex,...]");
//		}
//		final long[] keys = new long[lenAndBucketPairs.length / 2];
//		for (int i = 0; i < keys.length; i++) {
//			keys[i] = packBucketKey(lenAndBucketPairs[i * 2], lenAndBucketPairs[i * 2 + 1]);
//		}
//		return keys;
//	}

	/** 单个查询 slice → 一个 packed bucketKey。 */
	public static long[] selectiveKeysForSlice(BytesRef slice) {
		final int lenIdx = slice.length - 1;
		if (lenIdx < 0 || lenIdx >= Lucene80FPSearchConfig.NGRAM_MAX) {
			return EMPTY_LONG;
		}
		return new long[] { packBucketKey(lenIdx, bucketIndex(slice)) };
	}

	/**
	 * 多个查询 slice → 去重后的 bucketKey 数组（供 {@code Terms#fpBits(..., long[], long[])}）。
	 * hot / common 通常传同一组 keys。
	 */
	public static long[] selectiveKeysForSlices(BytesRef[] slices) {
		if (slices == null || slices.length == 0) {
			return EMPTY_LONG;
		}
		final long[] buf = new long[slices.length];
		int n = 0;
		for (BytesRef slice : slices) {
			if (slice == null || slice.length < Lucene80FPSearchConfig.NGRAM_MIN
					|| slice.length > Lucene80FPSearchConfig.NGRAM_MAX) {
				continue;
			}
			final long key = packBucketKey(slice.length - 1, bucketIndex(slice));
			boolean dup = false;
			for (int i = 0; i < n; i++) {
				if (buf[i] == key) {
					dup = true;
					break;
				}
			}
			if (!dup) {
				buf[n++] = key;
			}
		}
		return n == buf.length ? buf : Arrays.copyOf(buf, n);
	}

	private static final long[] EMPTY_LONG = new long[0];

	/** 查询 slice 在 hot tier 中命中的 order 列表；无命中返回空数组。 */
	public int[] lookupHotOrders(BytesRef slice) {
		return lookupOrders(hot, slice);
	}

	/** 查询 slice 在 common tier 中命中的 order 列表；无命中返回空数组。 */
	public int[] lookupCommonOrders(BytesRef slice) {
		return lookupOrders(common, slice);
	}

	public int[] lookupHotOrders(int lenIdx, int bucket) {
		return hot.lookup(lenIdx, bucket);
	}

	public int[] lookupCommonOrders(int lenIdx, int bucket) {
		return common.lookup(lenIdx, bucket);
	}

	private static int[] lookupOrders(TierIndex tier, BytesRef slice) {
		if (slice == null || slice.length < Lucene80FPSearchConfig.NGRAM_MIN
				|| slice.length > Lucene80FPSearchConfig.NGRAM_MAX) {
			return EMPTY_ORDERS;
		}
		return tier.lookup(slice.length - 1, bucketIndex(slice));
	}

	private static void markHotNgramsForPayload(TierIndex tier, BytesRef payload, int order) {
		if (order < 1) {
			return;
		}
		final int payloadLen = payload.length;
		if (payloadLen <= 0) {
			return;
		}
		final int base = payload.offset;
		final BytesRef sliceScratch = new BytesRef();
		sliceScratch.bytes = payload.bytes;
		for (int start = 0; start < payloadLen; start++) {
			for (int n = Lucene80FPSearchConfig.NGRAM_MIN; n <= Lucene80FPSearchConfig.NGRAM_MAX
					&& start + n <= payloadLen; n++) {
				sliceScratch.offset = base + start;
				sliceScratch.length = n;
				tier.add(n - 1, bucketIndex(sliceScratch), order);
			}
		}
	}

	private static void markCommonNgramsByUniqueSlices(HashSet<FpTermKey> hotKeySet, TierIndex tier, BytesRef payload,
			int order) {
		if (order < 1) {
			return;
		}
		final int payloadLen = payload.length;
		if (payloadLen <= 0) {
			return;
		}
		final int base = payload.offset;
		final BytesRef sliceScratch = new BytesRef();
		sliceScratch.bytes = payload.bytes;
		final int uniqueCap = Math.max(16, Math.min(payloadLen * 4, 4096));
		final HashSet<FpTermKey> uniqueSlices = new HashSet<>(uniqueCap);
		for (int start = 0; start < payloadLen; start++) {
			for (int n = Lucene80FPSearchConfig.NGRAM_MIN; n <= Lucene80FPSearchConfig.NGRAM_MAX
					&& start + n <= payloadLen; n++) {
				sliceScratch.offset = base + start;
				sliceScratch.length = n;
				if (uniqueSlices.contains(FpTermKey.viewOf(sliceScratch))) {
					continue;
				}
				uniqueSlices.add(FpTermKey.copyOf(sliceScratch));
			}
		}
		for (FpTermKey key : uniqueSlices) {
			if (hotKeySet.contains(key)) {
				continue;
			}
			final BytesRef br = key.bytesRef();
			tier.add(br.length - 1, bucketIndex(br), order);
		}
	}

	private static String formatFlushStats(TierIndex hotTier, TierIndex commonTier) {
		final StringBuilder sb = new StringBuilder(256);
		for (int li = 0; li < Lucene80FPSearchConfig.NGRAM_MAX; li++) {
			sb.append(" len").append(li + 1).append("{hot entries=").append(hotTier.rows[li].entryCount())
					.append(" | common entries=").append(commonTier.rows[li].entryCount()).append('}');
		}
		return sb.toString();
	}

	public int getHotCount() {
		return hotCount;
	}

	public int getCommonCount() {
		return commonCount;
	}

	public int getHotNumBits() {
		return hot.maxArenaBytes();
	}

	public int getCommonNumBits() {
		return common.maxArenaBytes();
	}

	public boolean isSparse() {
		return sparse;
	}

	/** 内存全量实例上截取 selective 视图（测试或上游仅内存缓存时可用）。 */
	public FpGroupHotNgramBitIndex viewSelective(long[] hotKeys, long[] commonKeys) {
		return new FpGroupHotNgramBitIndex(targetlevel, hot.viewSelective(hotKeys), common.viewSelective(commonKeys),
				hotCount, commonCount, true);
	}

	// -------------------------------------------------------------------------
	// TierIndex — 一个 tier（hot 或 common）：6 行 LenRow，每行一种 ngram 长度
	// -------------------------------------------------------------------------

	/**
	 * 单套 tier 的内存 / 磁盘载体。
	 * <pre>
	 * rows[0]  len=1  sorted bucketIndex → orders
	 * rows[1]  len=2  ...
	 * ...
	 * rows[5]  len=6
	 * </pre>
	 */
	static final class TierIndex {
		final LenRow[] rows = new LenRow[Lucene80FPSearchConfig.NGRAM_MAX];

		TierIndex(boolean sparse) {
			for (int i = 0; i < rows.length; i++) {
				rows[i] = new LenRow(sparse);
			}
		}

		static TierIndex emptySparse() {
			return new TierIndex(true);
		}

		void add(int lenIdx, int bucket, int order) {
			rows[lenIdx].add(bucket, order);
		}

		void finalizeRows() {
			for (LenRow row : rows) {
				try {
					row.finalizeRow();
				} catch (IOException e) {
					throw new AssertionError(e);
				}
			}
		}

		int maxArenaBytes() {
			int max = 0;
			for (LenRow row : rows) {
				max = Math.max(max, row.orderArena == null ? 0 : row.orderArena.length);
			}
			return max;
		}

		int[] lookup(int lenIdx, int bucket) {
			return rows[lenIdx].lookup(bucket);
		}

		TierIndex viewSelective(long[] keys) {
			if (keys == null || keys.length == 0) {
				return emptySparse();
			}
			final TierIndex sparseTier = emptySparse();
			for (long key : keys) {
				final int lenIdx = unpackLenIdx(key);
				final int bucket = unpackBucketIndex(key);
				final int[] orders = lookup(lenIdx, bucket);
				if (orders.length > 0) {
					sparseTier.rows[lenIdx].putSparse(bucket, orders);
				}
			}
			return sparseTier;
		}

		/** 先写 RAM 缓冲（回填 lenRowOffset），再一次性写入 {@link DataOutput}。 */
		int writeTier(DataOutput out) throws IOException {
			final RAMOutputStream ram = new RAMOutputStream();
			ram.writeInt(TIER_MAGIC);
			final int lenTablePos = 4;
			for (int i = 0; i < rows.length; i++) {
				ram.writeLong(0L);
			}
			final long[] lenOffsets = new long[rows.length];
			for (int i = 0; i < rows.length; i++) {
				lenOffsets[i] = ram.getFilePointer();
				rows[i].writeLenRow(ram);
			}
			final int len = Math.toIntExact(ram.getFilePointer());
			final byte[] buf = new byte[len];
			ram.writeTo(buf, 0);
			for (int i = 0; i < lenOffsets.length; i++) {
				putLong(buf, lenTablePos + i * 8, lenOffsets[i]);
			}
			out.writeBytes(buf, 0, len);
			int totalArena = 0;
			for (LenRow row : rows) {
				totalArena += row.orderArena == null ? 0 : row.orderArena.length;
			}
			return totalArena;
		}

		private static void putLong(byte[] data, int pos, long v) {
			data[pos] = (byte) (v >>> 56);
			data[pos + 1] = (byte) (v >>> 48);
			data[pos + 2] = (byte) (v >>> 40);
			data[pos + 3] = (byte) (v >>> 32);
			data[pos + 4] = (byte) (v >>> 24);
			data[pos + 5] = (byte) (v >>> 16);
			data[pos + 6] = (byte) (v >>> 8);
			data[pos + 7] = (byte) v;
		}

		static TierIndex readTier(DataInput in, boolean sparse) throws IOException {
			final int magic = in.readInt();
			if (magic != TIER_MAGIC) {
				throw new IOException("unexpected tier magic: " + magic);
			}
			for (int i = 0; i < Lucene80FPSearchConfig.NGRAM_MAX; i++) {
				in.readLong();
			}
			final TierIndex tier = sparse ? emptySparse() : new TierIndex(false);
			for (int i = 0; i < tier.rows.length; i++) {
				tier.rows[i] = LenRow.readLenRow(in, sparse);
			}
			return tier;
		}

		/**
		 * selective 读：只读 len 行目录 + 各 key 对应 LenRow 内的单个 bucket。
		 * 不对未请求的 LenRow 做 IO。
		 */
		static TierIndex readSelective(IndexInput in, long tierOffset, long[] keys) throws IOException {
			in.seek(tierOffset);
			final int magic = in.readInt();
			if (magic != TIER_MAGIC) {
				throw new IOException("unexpected tier magic: " + magic);
			}
			final long[] lenRowOffsets = new long[Lucene80FPSearchConfig.NGRAM_MAX];
			for (int i = 0; i < lenRowOffsets.length; i++) {
				lenRowOffsets[i] = in.readLong();
			}
			final TierIndex tier = emptySparse();
			for (long key : keys) {
				final int lenIdx = unpackLenIdx(key);
				final int bucket = unpackBucketIndex(key);
				if (lenIdx < 0 || lenIdx >= Lucene80FPSearchConfig.NGRAM_MAX) {
					continue;
				}
				in.seek(tierOffset + lenRowOffsets[lenIdx]);
				final int[] orders = LenRow.readOrdersForBucket(in, bucket);
				if (orders.length > 0) {
					tier.rows[lenIdx].putSparse(bucket, orders);
				}
			}
			return tier;
		}
	}

	// -------------------------------------------------------------------------
	// LenRow — 单一 ngram 长度下的 sorted posting 表 + orderArena
	// -------------------------------------------------------------------------

	/**
	 * 一种 ngram 长度对应一行 posting。
	 * <p>构建期用 {@link TreeMap} 聚合；{@link #finalizeRow()} 后变为 sortedKeys + entryMeta + orderArena。
	 * 稀疏实例仅保留 {@link #sparseOrders}。
	 */
	static final class LenRow {
		private TreeMap<Integer, IntList> buildMap;
		private HashMap<Integer, int[]> sparseOrders;

		int[] sortedKeys;
		int[] entryMeta;
		byte[] orderArena;

		LenRow(boolean sparse) {
			if (sparse) {
				sparseOrders = new HashMap<>();
			} else {
				buildMap = new TreeMap<>();
			}
		}

		void add(int bucket, int order) {
			if (buildMap == null) {
				return;
			}
			IntList list = buildMap.get(bucket);
			if (list == null) {
				list = new IntList();
				buildMap.put(bucket, list);
			}
			list.add(order);
		}

		void putSparse(int bucket, int[] orders) {
			sparseOrders.put(bucket, orders);
		}

		int entryCount() {
			if (sparseOrders != null) {
				return sparseOrders.size();
			}
			return sortedKeys == null ? 0 : sortedKeys.length;
		}

		/** 构建结束：TreeMap → 排序数组 + 压缩 order 列表。 */
		void finalizeRow() throws IOException {
			if (buildMap == null || buildMap.isEmpty()) {
				sortedKeys = EMPTY_INT;
				entryMeta = EMPTY_INT;
				orderArena = EMPTY_BYTES;
				return;
			}
			final int n = buildMap.size();
			sortedKeys = new int[n];
			entryMeta = new int[n];
			final RAMOutputStream arenaOut = new RAMOutputStream();
			int i = 0;
			for (Entry<Integer, IntList> e : buildMap.entrySet()) {
				sortedKeys[i] = e.getKey().intValue();
				final int[] orders = e.getValue().toSortedArray();
				if (orders.length == 1) {
					entryMeta[i] = (orders[0] << 1) | ENTRY_TAG_SINGLE;
				} else {
					final int arenaOffset = Math.toIntExact(arenaOut.getFilePointer());
					writeOrderList(arenaOut, orders);
					entryMeta[i] = (arenaOffset << 1) | ENTRY_TAG_MULTI;
				}
				i++;
			}
			orderArena = new byte[Math.toIntExact(arenaOut.getFilePointer())];
			arenaOut.writeTo(orderArena, 0);
			buildMap = null;
		}

		int[] lookup(int bucket) {
			if (sparseOrders != null) {
				final int[] orders = sparseOrders.get(bucket);
				return orders == null ? EMPTY_ORDERS : orders;
			}
			if (sortedKeys == null || sortedKeys.length == 0) {
				return EMPTY_ORDERS;
			}
			final int idx = Arrays.binarySearch(sortedKeys, bucket);
			if (idx < 0) {
				return EMPTY_ORDERS;
			}
			return decodeOrders(entryMeta[idx]);
		}

		int[] decodeOrders(int meta) {
			if ((meta & 1) == ENTRY_TAG_SINGLE) {
				return new int[] { meta >>> 1 };
			}
			final int arenaOffset = meta >>> 1;
			return readOrderList(orderArena, arenaOffset);
		}

		void writeLenRow(DataOutput out) throws IOException {
			final int entryCount = sortedKeys == null ? 0 : sortedKeys.length;
			final long lenRowStart = getOutputFilePointer(out);
			out.writeInt(entryCount);
			if (entryCount == 0) {
				return;
			}
			final int skipCount = (entryCount + SKIP_INTERVAL - 1) / SKIP_INTERVAL;
			final long keysStartRel = getOutputFilePointer(out) + (long) skipCount * 12L - lenRowStart;
			for (int s = 0; s < skipCount; s++) {
				final int entryIdx = s * SKIP_INTERVAL;
				out.writeInt(sortedKeys[entryIdx]);
				out.writeLong(keysStartRel + (long) entryIdx * 4L);
			}
			for (int k = 0; k < entryCount; k++) {
				out.writeInt(sortedKeys[k]);
			}
			for (int k = 0; k < entryCount; k++) {
				out.writeInt(entryMeta[k]);
			}
			out.writeInt(orderArena == null ? 0 : orderArena.length);
			if (orderArena != null && orderArena.length > 0) {
				out.writeBytes(orderArena, 0, orderArena.length);
			}
		}

		private static long getOutputFilePointer(DataOutput out) {
			if (out instanceof RAMOutputStream) {
				return ((RAMOutputStream) out).getFilePointer();
			}
			return out instanceof IndexOutput ? ((IndexOutput) out).getFilePointer() : 0L;
		}

		static LenRow readLenRow(DataInput in, boolean sparse) throws IOException {
			final LenRow row = new LenRow(sparse);
			final int entryCount = in.readInt();
			if (entryCount == 0) {
				row.sortedKeys = EMPTY_INT;
				row.entryMeta = EMPTY_INT;
				row.orderArena = EMPTY_BYTES;
				return row;
			}
			final int skipCount = (entryCount + SKIP_INTERVAL - 1) / SKIP_INTERVAL;
			for (int s = 0; s < skipCount; s++) {
				in.readInt();
				in.readLong();
			}
			row.sortedKeys = new int[entryCount];
			for (int i = 0; i < entryCount; i++) {
				row.sortedKeys[i] = in.readInt();
			}
			row.entryMeta = new int[entryCount];
			for (int i = 0; i < entryCount; i++) {
				row.entryMeta[i] = in.readInt();
			}
			final int arenaLen = in.readInt();
			if (arenaLen <= 0) {
				row.orderArena = EMPTY_BYTES;
			} else {
				row.orderArena = new byte[arenaLen];
				in.readBytes(row.orderArena, 0, arenaLen);
			}
			return row;
		}

		/**
		 * selective 跳跃读：skip 二分定位 128 条段 → 段内线性找 bucket → 读 meta / arena。
		 * 偏移均相对 LenRow 起点（与 {@link #writeLenRow} 中 keysStartRel 一致）。
		 */
		static int[] readOrdersForBucket(IndexInput in, int bucket) throws IOException {
			final long lenRowStart = in.getFilePointer();
			final int entryCount = in.readInt();
			if (entryCount == 0) {
				return EMPTY_ORDERS;
			}
			final int skipCount = (entryCount + SKIP_INTERVAL - 1) / SKIP_INTERVAL;
			final long skipTableStart = in.getFilePointer();
			int segment = 0;
			for (int s = 0; s < skipCount; s++) {
				in.seek(skipTableStart + (long) s * 12L);
				final int skipKey = in.readInt();
				if (skipKey <= bucket) {
					segment = s;
				}
			}
			in.seek(skipTableStart + (long) segment * 12L);
			in.readInt();
			final long keysStartRel = in.readLong();
			final int segStart = segment * SKIP_INTERVAL;
			final int segEnd = Math.min(segStart + SKIP_INTERVAL, entryCount);
			int found = -1;
			for (int i = segStart; i < segEnd; i++) {
				in.seek(lenRowStart + keysStartRel + (long) i * 4L);
				final int key = in.readInt();
				if (key == bucket) {
					found = i;
					break;
				}
				if (key > bucket) {
					break;
				}
			}
			if (found < 0) {
				return EMPTY_ORDERS;
			}
			final long metaStartRel = keysStartRel + (long) entryCount * 4L;
			in.seek(lenRowStart + metaStartRel + (long) found * 4L);
			final int meta = in.readInt();
			if ((meta & 1) == ENTRY_TAG_SINGLE) {
				return new int[] { meta >>> 1 };
			}
			final int arenaOffset = meta >>> 1;
			in.seek(lenRowStart + metaStartRel + (long) entryCount * 4L);
			final int arenaLen = in.readInt();
			if (arenaLen <= 0) {
				return EMPTY_ORDERS;
			}
			final byte[] arena = new byte[arenaLen];
			in.readBytes(arena, 0, arenaLen);
			return readOrderList(arena, arenaOffset);
		}
	}

	/** orderArena 内一条列表：vint(count) + 升序 order 的 vint 增量。 */
	static void writeOrderList(DataOutput out, int[] orders) throws IOException {
		out.writeVInt(orders.length);
		int prev = 0;
		for (int order : orders) {
			out.writeVInt(order - prev);
			prev = order;
		}
	}

	static int[] readOrderList(byte[] arena, int offset) {
		try {
			int pos = offset;
			final int count = readVIntAt(arena, pos);
			pos = nextVIntPos;
			if (count <= 0) {
				return EMPTY_ORDERS;
			}
			final int[] orders = new int[count];
			int prev = 0;
			for (int i = 0; i < count; i++) {
				final int delta = readVIntAt(arena, pos);
				pos = nextVIntPos;
				prev += delta;
				orders[i] = prev;
			}
			return orders;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private static int nextVIntPos;

	private static int readVIntAt(byte[] bytes, int offset) throws IOException {
		int shift = 0;
		int value = 0;
		int pos = offset;
		while (true) {
			if (pos >= bytes.length) {
				throw new IOException("vint past end");
			}
			final byte b = bytes[pos++];
			value |= (b & 0x7F) << shift;
			if ((b & 0x80) == 0) {
				nextVIntPos = pos;
				return value;
			}
			shift += 7;
			if (shift > 35) {
				throw new IOException("vint too long");
			}
		}
	}

	static final class IntList {
		private int[] data = new int[4];
		private int size;

		void add(int v) {
			if (size == data.length) {
				data = ArrayUtil.grow(data);
			}
			data[size++] = v;
		}

		int[] toSortedArray() {
			final int[] copy = Arrays.copyOf(data, size);
			Arrays.sort(copy);
			int w = 1;
			for (int i = 1; i < copy.length; i++) {
				if (copy[i] != copy[i - 1]) {
					copy[w++] = copy[i];
				}
			}
			return Arrays.copyOf(copy, w);
		}
	}

	private static final int[] EMPTY_INT = new int[0];
	private static final byte[] EMPTY_BYTES = new byte[0];
}
