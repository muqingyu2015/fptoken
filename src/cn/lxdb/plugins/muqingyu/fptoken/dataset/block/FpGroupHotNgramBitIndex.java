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
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpLog;
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
 * LenRow（稀疏 / selective 查询实例）
 * └── sparseOrders: HashMap&lt;bucket, order[]&gt;  // readSelective 用 skip 跳跃读盘后填入；lookup 纯内存
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

	/** skip 表采样间隔：每 128 条 posting 一条 (anchorKey, keysPtrRel)；见 {@link LenRow#readOrdersForBucket}。 */
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

		final StringBuilder sb = FpLog.kv();
		FpLog.append(sb, "event", "flush");
		FpLog.append(sb, "phase", from);
		FpLog.append(sb, "perLength", formatFlushStats(hot, common));
		FpLog.append(sb, "hotCount", hotCount);
		FpLog.append(sb, "commonCount", commonCount);
		FpLog.append(sb, "targetLevel", "L" + targetlevel);
		FpLog.append(sb, "block", info);
		FpLog.infoLine(LOG, FpLog.TAG_BITINDEX, sb);
		return info;
	}

	/** 全量读 hot + common 两个 tier（{@code load* = null} 的便捷入口）。 */
	public static FpGroupHotNgramBitIndex readfrom(IndexInput in, FpBlockInfo blkinfo) throws IOException {
		return readfromBanksSelective(in, blkinfo, (long[]) null, (long[]) null);
	}

	/**
	 * 按 bucketKey 选择性读盘，并在返回前把各 bucket 的 orderList 解析进内存。
	 * <ul>
	 *   <li>{@code loadHotKeys/loadCommonKeys} 均为 null → 等同 {@link #readfrom}（全量 tier）</li>
	 *   <li>非 null → 仅对 listed bucket 做 skip 跳跃读盘，结果写入 {@link LenRow#sparseOrders}；
	 *       之后 {@link #lookupHotOrders}/{@link #lookupCommonOrders} 纯内存查表</li>
	 *   <li>空数组 → 对应 tier 为空 sparse（不读盘）</li>
	 * </ul>
	 *
	 * <p>不持有 {@link IndexInput}：IO 在本方法内完成，{@code fpBits} 返回后 Lucene 可安全关闭底层流。
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
		final IndexInput disk = in.clone();
		try {
			final TierIndex hotTier = loadHotKeys == null || loadHotKeys.length == 0 ? TierIndex.emptySparse()
					: TierIndex.readSelective(disk, blkinfo.fpBanksHot, loadHotKeys);
			final TierIndex commonTier = loadCommonKeys == null || loadCommonKeys.length == 0 ? TierIndex.emptySparse()
					: TierIndex.readSelective(disk, blkinfo.fpBanksCommon, loadCommonKeys);
			return new FpGroupHotNgramBitIndex(blkinfo.targetLevel, hotTier, commonTier, blkinfo.hotCount,
					blkinfo.commonCount, true);
		} finally {
			disk.close();
		}
	}

	/**
	 * 兼容旧 {@code boolean[][]} 签名：非 null 亦视为全量读（掩码已废弃）。
	 */
	public static FpGroupHotNgramBitIndex readfromBanksSelective(IndexInput in, FpBlockInfo blkinfo,
			boolean[][] loadHot, boolean[][] loadCommon) throws IOException {
		return readfrom(in, blkinfo);
	}

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

	/** 查询 slice 在 hot tier 中命中的 order 列表；selective 实例在 {@link #readfromBanksSelective} 时已预加载。 */
	public int[] lookupHotOrders(BytesRef slice) {
		return lookupOrders(hot, slice);
	}

	/** 查询 slice 在 common tier 中命中的 order 列表；selective 实例在 {@link #readfromBanksSelective} 时已预加载。 */
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

	/** true 表示由 selective 读或 {@link #viewSelective} 构造，仅含部分 bucket。 */
	public boolean isSparse() {
		return sparse;
	}

	/** 当前 hot tier 已加载的 bucket 条数（sparse 实例可能远小于 {@link #getHotCount()}）。 */
	public int loadedHotBucketCount() {
		return hot.totalEntryCount();
	}

	/** 当前 common tier 已加载的 bucket 条数。 */
	public int loadedCommonBucketCount() {
		return common.totalEntryCount();
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
	 * 单套 tier 的内存 / 磁盘载体（hot 或 common 各一份）。
	 *
	 * <p><b>逻辑结构</b>：固定 6 行 {@link LenRow}，{@code rows[lenIdx]} 只存该长度的 ngram
	 *（len = lenIdx + 1，范围 1..{@link Lucene80FPSearchConfig#NGRAM_MAX}）。
	 * 每行是一张「bucketIndex → order[]」升序 posting 表。
	 *
	 * <p><b>两种实例形态</b>
	 * <ul>
	 *   <li><b>全量</b>（{@code sparse=false}）：构建期 / {@link #readTier} 后，
	 *       每行有完整的 sortedKeys + entryMeta + orderArena</li>
	 *   <li><b>稀疏</b>（{@code sparse=true}）：{@link #readSelective} 用 skip 跳跃读 {@code keys} 对应 bucket 的 orderList 到 {@link LenRow#sparseOrders}；
	 *       或 {@link #viewSelective} 从全量内存实例截取</li>
	 * </ul>
	 *
	 * <p><b>磁盘上 Tier 的布局</b>（{@link #writeTier} 写出，{@link #readTier} / {@link #readSelective} 读入）
	 * <pre>
	 * tierOffset ─┬─ magic: int ('FPTR')
	 *             ├─ lenRowOffset[0..5]: long × 6   // 各 LenRow 相对 tierOffset 的字节偏移
	 *             ├─ LenRow(len=1)  @ lenRowOffset[0]
	 *             ├─ LenRow(len=2)  @ lenRowOffset[1]
	 *             └─ ...
	 * </pre>
	 *
	 * <p><b>全量读 vs selective 读</b>
	 * <ul>
	 *   <li>{@link #readTier}：顺序读完 6 个 LenRow（跳过 skip 表内容但把整个 keys/meta/arena 载入内存）</li>
	 *   <li>{@link #readSelective}：只读 tier 头 + 查询 keys 涉及到的 LenRow 中的<b>单个 bucket</b>，
	 *       利用 skip 表做 O(段数 + 128) 跳跃定位，避免读整张 LenRow</li>
	 * </ul>
	 *
	 * @see DiskLenRow#lookupOrders           skip 跳跃 + 按需 seek（仅 readSelective 预取阶段）
	 * @see #readSelective                    按 bucketKey 预取 orderList，不持有 IndexInput
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

		int totalEntryCount() {
			int n = 0;
			for (LenRow row : rows) {
				n += row.entryCount();
			}
			return n;
		}

		int[] lookup(int lenIdx, int bucket) {
			return rows[lenIdx].lookup(bucket);
		}

		TierIndex viewSelective(long[] keys) {
			if (keys == null || keys.length == 0) {
				return emptySparse();
			}
			// 内存全量实例上的 selective 视图：不走磁盘 skip，直接 binarySearch sortedKeys
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
			// 跳过 lenRow 目录表（6 × long）；readLenRow 会顺序消费后续字节流
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
		 * <b>Selective 读 tier</b>：只加载查询需要的 bucket，返回 {@code sparse=true} 实例。
		 *
		 * <p><b>调用场景</b>：{@code Terms#fpBits(..., loadHotKeys, loadCommonKeys)} 传入非 null 的
		 * bucketKey 数组时，由 Lucene {@code FieldReader} 打开 termsbit 文件后调用本方法，避免把整段
		 * hot/common tier 读入内存。
		 *
		 * <p><b>参数</b>
		 * <ul>
		 *   <li>{@code tierOffset} — 本 tier 在 termsbit 文件中的绝对偏移（来自 {@link FpBlockInfo#fpBanksHot}
		 *       或 {@link FpBlockInfo#fpBanksCommon}）</li>
		 *   <li>{@code keys} —  packed bucketKey 数组，每个 long = {@link #packBucketKey(lenIdx, bucketIndex)}，
		 *       通常由 {@link cn.lxdb.plugins.muqingyu.fptoken.api.FpSearch} 从查询 slice 推导</li>
		 * </ul>
		 *
		 * <p><b>IO 步骤</b>（对每个 key 重复，key 之间互不干扰，因 {@link IndexInput} 已 clone）
		 * <ol>
		 *   <li>seek {@code tierOffset}，校验 magic，读 6 个 lenRowOffset</li>
		 *   <li>unpack key → lenIdx + bucketIndex</li>
		 *   <li>seek {@code tierOffset + lenRowOffsets[lenIdx]} 到对应 LenRow 起点</li>
	 *   <li>对每个涉及的 lenIdx 打开 {@link DiskLenRow}（skip 表 + min/max bucket，局部变量）</li>
	 *   <li>对 {@code keys} 中每个 bucket 跳跃读 orderList，写入 {@link LenRow#sparseOrders}</li>
	 * </ol>
	 *
	 * <p>调用方 {@link #readfromBanksSelective} 负责 clone/close {@link IndexInput}；本 tier 返回后 lookup 纯内存。
	 */
		static TierIndex readSelective(IndexInput disk, long tierOffset, long[] keys) throws IOException {
			if (keys == null || keys.length == 0) {
				return emptySparse();
			}
			disk.seek(tierOffset);
			final int magic = disk.readInt();
			if (magic != TIER_MAGIC) {
				throw new IOException("unexpected tier magic: " + magic);
			}
			final long[] lenRowOffsets = new long[Lucene80FPSearchConfig.NGRAM_MAX];
			for (int i = 0; i < lenRowOffsets.length; i++) {
				lenRowOffsets[i] = disk.readLong();
			}
			final TierIndex tier = emptySparse();
			final DiskLenRow[] diskRows = new DiskLenRow[Lucene80FPSearchConfig.NGRAM_MAX];
			for (long key : keys) {
				final int lenIdx = unpackLenIdx(key);
				if (lenIdx < 0 || lenIdx >= Lucene80FPSearchConfig.NGRAM_MAX) {
					continue;
				}
				if (diskRows[lenIdx] == null) {
					disk.seek(tierOffset + lenRowOffsets[lenIdx]);
					diskRows[lenIdx] = DiskLenRow.open(disk);
				}
				final int bucket = unpackBucketIndex(key);
				final int[] orders = diskRows[lenIdx].lookupOrders(bucket);
				if (orders.length > 0) {
					tier.rows[lenIdx].putSparse(bucket, orders);
				}
			}
			return tier;
		}
	}

	// -------------------------------------------------------------------------
	// DiskLenRow — readSelective 预取阶段：skip 表在内存，keys/meta/arena 按需 seek（局部变量，不挂实例）
	// -------------------------------------------------------------------------

	/**
	 * LenRow 的磁盘视图。open 时只读 entryCount、skip 表、min/max bucket、各区偏移；不读 keys/meta/arena 正文。
	 */
	static final class DiskLenRow {
		final IndexInput in;
		final long lenRowStart;
		final int entryCount;
		final int minBucket;
		final int maxBucket;
		final int skipCount;
		final int[] skipAnchors;
		final long[] skipKeysPtrRel;
		final long keysBaseRel;
		final long metaStartRel;
		final long arenaDataStart;

		private DiskLenRow(IndexInput in, long lenRowStart, int entryCount, int minBucket, int maxBucket,
				int skipCount, int[] skipAnchors, long[] skipKeysPtrRel, long keysBaseRel, long metaStartRel,
				long arenaDataStart) {
			this.in = in;
			this.lenRowStart = lenRowStart;
			this.entryCount = entryCount;
			this.minBucket = minBucket;
			this.maxBucket = maxBucket;
			this.skipCount = skipCount;
			this.skipAnchors = skipAnchors;
			this.skipKeysPtrRel = skipKeysPtrRel;
			this.keysBaseRel = keysBaseRel;
			this.metaStartRel = metaStartRel;
			this.arenaDataStart = arenaDataStart;
		}

		static DiskLenRow open(IndexInput in) throws IOException {
			final long lenRowStart = in.getFilePointer();
			final int entryCount = in.readInt();
			if (entryCount == 0) {
				return new DiskLenRow(in, lenRowStart, 0, 0, 0, 0, EMPTY_INT, EMPTY_LONG, 0L, 0L, 0L);
			}
			final int skipCount = (entryCount + SKIP_INTERVAL - 1) / SKIP_INTERVAL;
			final long skipTableStart = in.getFilePointer();
			final int[] skipAnchors = new int[skipCount];
			final long[] skipKeysPtrRel = new long[skipCount];
			for (int s = 0; s < skipCount; s++) {
				in.seek(skipTableStart + (long) s * 12L);
				skipAnchors[s] = in.readInt();
				skipKeysPtrRel[s] = in.readLong();
			}
			final long keysBaseRel = skipKeysPtrRel[0];
			final long metaStartRel = keysBaseRel + (long) entryCount * 4L;
			in.seek(lenRowStart + keysBaseRel + (long) (entryCount - 1) * 4L);
			final int maxBucket = in.readInt();
			in.seek(lenRowStart + metaStartRel + (long) entryCount * 4L);
			final int arenaLen = in.readInt();
			final long arenaDataStart = in.getFilePointer();
			if (arenaLen > 0) {
				in.seek(arenaDataStart + arenaLen);
			}
			return new DiskLenRow(in, lenRowStart, entryCount, skipAnchors[0], maxBucket, skipCount, skipAnchors,
					skipKeysPtrRel, keysBaseRel, metaStartRel, arenaDataStart);
		}

		int[] lookupOrders(int bucket) throws IOException {
			if (entryCount == 0 || bucket < minBucket || bucket > maxBucket) {
				return EMPTY_ORDERS;
			}
			int segment = 0;
			int lo = 0;
			int hi = skipCount - 1;
			while (lo <= hi) {
				final int mid = (lo + hi) >>> 1;
				if (skipAnchors[mid] <= bucket) {
					segment = mid;
					lo = mid + 1;
				} else {
					hi = mid - 1;
				}
			}
			if (segment + 1 < skipCount && bucket >= skipAnchors[segment + 1]) {
				return EMPTY_ORDERS;
			}
			final int segStart = segment * SKIP_INTERVAL;
			final int segEnd = Math.min(segStart + SKIP_INTERVAL, entryCount);
			final long segKeysBaseRel = skipKeysPtrRel[segment] - (long) segStart * 4L;
			int found = -1;
			for (int i = segStart; i < segEnd; i++) {
				in.seek(lenRowStart + segKeysBaseRel + (long) i * 4L);
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
			in.seek(lenRowStart + metaStartRel + (long) found * 4L);
			final int meta = in.readInt();
			if ((meta & 1) == ENTRY_TAG_SINGLE) {
				return new int[] { meta >>> 1 };
			}
			return readOrderListFromInput(in, arenaDataStart + (meta >>> 1));
		}
	}

	static int[] readOrderListFromInput(IndexInput in, long offset) throws IOException {
		in.seek(offset);
		final int count = in.readVInt();
		if (count <= 0) {
			return EMPTY_ORDERS;
		}
		final int[] orders = new int[count];
		int prev = 0;
		for (int i = 0; i < count; i++) {
			prev += in.readVInt();
			orders[i] = prev;
		}
		return orders;
	}

	// -------------------------------------------------------------------------
	// LenRow — 单一 ngram 长度下的 sorted posting 表 + orderArena
	// -------------------------------------------------------------------------

	/**
	 * 一种 ngram 长度对应一行 posting（LenRow = Length Row）。
	 *
	 * <p><b>语义</b>：{@code sortedKeys[i]} 是第 i 个 bucket 的 murmurhash；
	 * {@code entryMeta[i]} 指向该 bucket 下所有 term order（组内序号）。
	 * keys 严格升序，lookup / skip 读均依赖此性质。
	 *
	 * <p><b>磁盘布局</b>（所有相对偏移均相对 <b>LenRow 起点</b> {@code lenRowStart}）
	 * <pre>
	 * ┌──────────────────────────────────────────────────────────────────────────┐
	 * │ entryCount: int                          // 非空 bucket 个数 N             │
	 * ├──────────────────────────────────────────────────────────────────────────┤
	 * │ skip 表（N &gt; 0 时存在，共 skipCount = ceil(N/128) 条，每条 12 字节）      │
	 * │   skip[s] = ( anchorKey: int, keysPtrRel: long )                         │
	 * │   anchorKey  = sortedKeys[s × 128]     // 该段最小 bucket，用于粗定位      │
	 * │   keysPtrRel = keys[s×128] 相对 LenRow 起点的字节偏移（见 writeLenRow）   │
	 * ├──────────────────────────────────────────────────────────────────────────┤
	 * │ sortedKeys[N]   : int × N                // bucketIndex 升序               │
	 * │ entryMeta[N]    : int × N                // order 或 arena 指针            │
	 * │ arenaLen        : int                                                    │
	 * │ orderArena      : byte × arenaLen        // 多 order 的 vint 压缩池        │
	 * └──────────────────────────────────────────────────────────────────────────┘
	 *
	 * 相对偏移关系（读 selective 时用）：
	 *   keysBaseRel  = sortedKeys[0] 相对 lenRowStart 的偏移
	 *   keys[i]      @ lenRowStart + keysBaseRel + i×4
	 *   meta[i]      @ lenRowStart + keysBaseRel + N×4 + i×4
	 *   arena        @ lenRowStart + keysBaseRel + N×4 + N×4  （先读 arenaLen）
	 * </pre>
	 *
	 * <p><b>skip 表设计动机</b>：生产 LenRow 常有数千个 bucket（L2/L3 segment）。
	 * selective 查询只关心 1 个 bucket，不能线性扫完整张 keys 数组。
	 * skip 表把 N 个 key 切成每段最多 {@link FpGroupHotNgramBitIndex#SKIP_INTERVAL}（128）个：
	 * 先 O(skipCount) 找到 bucket 落在哪一段，再在该段内最多比较 128 次。
	 *
	 * <p><b>示例</b>（N=300，查找 bucket=K）
	 * <pre>
	 * skip[0]: anchor=sortedKeys[0],   ptr→keys[0]
	 * skip[1]: anchor=sortedKeys[128], ptr→keys[128]
	 * skip[2]: anchor=sortedKeys[256], ptr→keys[256]
	 *
	 * 1) 扫描 skip：找最大的 s 使 anchorKey ≤ K  → 落入 segment=1（128..255）
	 * 2) keysBaseRel = keysPtrRel - segStart×4     → 还原 keys[0] 的起点
	 * 3) 在 i∈[128,255] 内顺序读 keys[i]，直到 key==K 或 key&gt;K
	 * 4) 用 found 下标读 meta[found]，再解码 order（单条内联或读 arena）
	 * </pre>
	 *
	 * <p>构建期用 {@link TreeMap} 聚合；{@link #finalizeRow()} 后变为 sortedKeys + entryMeta + orderArena。
	 * selective 读在 {@link TierIndex#readSelective} 预取到 {@link #sparseOrders}；{@link #viewSelective} 从全量实例截取。
	 */
	static final class LenRow {
		private TreeMap<Integer, IntList> buildMap;
		private HashMap<Integer, int[]> sparseOrders;

		int[] sortedKeys;
		int[] entryMeta;
		byte[] orderArena;

		LenRow(boolean sparse) {
			if (!sparse) {
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
			if (sparseOrders == null) {
				sparseOrders = new HashMap<>();
			}
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

		/** 写出 LenRow 到 {@link DataOutput}（flush 阶段，{@link TierIndex#writeTier} 调用）。 */
		void writeLenRow(DataOutput out) throws IOException {
			final int entryCount = sortedKeys == null ? 0 : sortedKeys.length;
			final long lenRowStart = getOutputFilePointer(out);
			out.writeInt(entryCount);
			if (entryCount == 0) {
				return;
			}
			final int skipCount = (entryCount + SKIP_INTERVAL - 1) / SKIP_INTERVAL;
			// sortedKeys[0] 将在 skip 表之后写出；此处预计算其相对 LenRow 起点的偏移
			final long keysStartRel = getOutputFilePointer(out) + (long) skipCount * 12L - lenRowStart;
			for (int s = 0; s < skipCount; s++) {
				final int entryIdx = s * SKIP_INTERVAL;
				// 段锚点：该段第一个 bucket 的值（sortedKeys 升序，故为段内最小 key）
				out.writeInt(sortedKeys[entryIdx]);
				// 段锚点在 sortedKeys 数组中的字节偏移（相对 lenRowStart，指向 keys[entryIdx] 而非 keys[0]）
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

		/**
		 * 全量读 LenRow（{@link TierIndex#readTier} 路径）。
		 * skip 表内容读入后直接丢弃——全量路径本来就要读完整 keys/meta/arena。
		 */
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

		/** selective 单 bucket 跳跃读（测试或独立调用）；生产由 {@link TierIndex#readSelective} 预取到 {@link #sparseOrders}。 */
		static int[] readOrdersForBucket(IndexInput in, int bucket) throws IOException {
			return DiskLenRow.open(in).lookupOrders(bucket);
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
