package cn.lxdb.plugins.muqingyu.fptoken.dataset.block;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.RAMOutputStream;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.StringHelper;
import org.slf4j.Logger;

import cn.lucene.proguard.keep.lxdb.common.CLMillisecondClock;
import cn.lucene.lxdb.params.LxdbLogerEncrypt;
import cn.lxdb.plugins.muqingyu.fptoken.config.FpTokenBlockLevelPolicy;
import cn.lxdb.plugins.muqingyu.fptoken.config.Lucene80FPSearchConfig;
import cn.lxdb.plugins.muqingyu.fptoken.pool.FpHashMapPoolHub;
import cn.lxdb.plugins.muqingyu.fptoken.pool.FpHashMapPoolIds;
import cn.lxdb.plugins.muqingyu.fptoken.pool.FpHashMapPoolLease;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.block.FpGroupHotNgramRebuild.CommonTermSortEntry;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FPDocList;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpBlockInfo;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpLog;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpTermKey;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpTermKeyHash;

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
 * └── orderBytes[byte]        // 多条 order 的 vint 增量序列池（单条 order 内联在 entryMeta）
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
 * │ orderBytes[arenaLen]: byte                                  │
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
 * bucketIndex = len≤4 时大端字节拼 int；len≥5 时 murmurhash3_x86_32
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
	/** 日志记录器，使用加密/脱敏日志工具获取 */
	public static final Logger LOG = LxdbLogerEncrypt.getLogger("mqy.fptoken");

	/** skip 表采样间隔：每 128 条 posting 一条 (anchorKey, keysPtrRel)；见 {@link LenRow#readOrdersForBucket}。 */
	static final int SKIP_INTERVAL = 16;
	/** skip 表每条：min(int) + max(int) + keysPtrRel(long) */
	static final int SKIP_ENTRY_BYTES = 16;
	/** Tier 魔数 {@code 'FPTR'}，用于校验 hot/common 段边界。 */
	static final int TIER_MAGIC = 0x46505452; // 'FPTR'
	/** entryMeta 低 bit 标记：单 order 内联在 meta 高 31 位。 */
	static final int ENTRY_TAG_SINGLE = 0;
	/** entryMeta 低 bit 标记：多 order，高 31 位为 orderBytes 内 vint 列表偏移。 */
	static final int ENTRY_TAG_MULTI = 1;

	/** 空 order 数组常量，避免频繁分配 */
	private static final int[] EMPTY_ORDERS = new int[0];

	/** 热词 tier 索引 */
	private final TierIndex hot;
	/** 普通词 tier 索引 */
	private final TierIndex common;
	/** 组内热词 term 数量 */
	private final int hotCount;
	/** 组内普通词 term 数量 */
	private final int commonCount;
	/** 目标层级（如 L1/L2/L3） */
	private final int targetlevel;
	/** 是否为稀疏模式（仅加载了部分 bucket） */
	private final boolean sparse;

	/**
	 * 私有构造函数，仅通过静态工厂方法创建实例
	 *
	 * @param targetlevel 目标层级
	 * @param hot         热词 tier 索引
	 * @param common      普通词 tier 索引
	 * @param hotCount    热词 term 数
	 * @param commonCount 普通词 term 数
	 * @param sparse      是否为稀疏加载模式
	 */
	private FpGroupHotNgramBitIndex(int targetlevel, TierIndex hot, TierIndex common, int hotCount, int commonCount,
			boolean sparse) {
		this.targetlevel = targetlevel;
		this.hot = hot;
		this.common = common;
		this.hotCount = hotCount;
		this.commonCount = commonCount;
		this.sparse = sparse;
	}

	/**
	 * 归还 hot/common tier 中仍持有的池化 HashMap。
	 * 构建路径在 {@link TierIndex#finalizeRows()} 已释放 buildMap；稀疏 selective 读路径需在 lookup 结束后调用。
	 */
	public void releasePooledMaps() {
		hot.releasePooledMaps();
		common.releasePooledMaps();
	}

	/**
	 * 将 lenIdx 和 bucketIndex 打包为一个 long 型 key。
	 * lenIdx 放高 32 位，bucketIndex 放低 32 位。
	 *
	 * @param lenIdx      ngram 长度索引 (0..5)
	 * @param bucketIndex bucket 哈希值
	 * @return 打包后的 long key
	 */
	public static long packBucketKey(int lenIdx, int bucketIndex) {
		return ((long) lenIdx << 32) | (bucketIndex & 0xFFFFFFFFL);
	}

	/**
	 * 从打包的 key 中解出 lenIdx（高 32 位）
	 *
	 * @param key 打包后的 bucket key
	 * @return ngram 长度索引
	 */
	public static int unpackLenIdx(long key) {
		return (int) (key >>> 32);
	}

	/**
	 * 从打包的 key 中解出 bucketIndex（低 32 位）
	 *
	 * @param key 打包后的 bucket key
	 * @return bucket 哈希值
	 */
	public static int unpackBucketIndex(long key) {
		return (int) key;
	}

	/**
	 * 统一计算 bucketIndex：
	 * 1~4 字节直接大端拼接成 int（无需 hash，保留原始字节序）；
	 * 5~6 字节使用 murmurhash3_x86_32 哈希。
	 *
	 * @param buf 字节数组
	 * @param off 起始偏移
	 * @param len 字节长度
	 * @return bucket 索引值
	 */
	public static int bucketIndex(byte[] buf, int off, int len) {
		if (len <= 0) {
			return 0;
		}
		switch (len) {
			case 1:
				// 1 字节：直接取低 8 位
				return buf[off] & 0xFF;
			case 2:
				// 2 字节：大端拼接
				return ((buf[off] & 0xFF) << 8) | (buf[off + 1] & 0xFF);
			case 3:
				// 3 字节：大端拼接
				return ((buf[off] & 0xFF) << 16) | ((buf[off + 1] & 0xFF) << 8) | (buf[off + 2] & 0xFF);
			case 4:
				// 4 字节：大端拼接
				return ((buf[off] & 0xFF) << 24) | ((buf[off + 1] & 0xFF) << 16) | ((buf[off + 2] & 0xFF) << 8)
						| (buf[off + 3] & 0xFF);
			default:
				// 5 字节及以上：使用 murmurhash3 哈希
				return StringHelper.murmurhash3_x86_32(buf, off, len, 0);
		}
	}

	/**
	 * BytesRef 版本的 bucketIndex 计算便捷方法
	 *
	 * @param ref BytesRef 引用
	 * @return bucket 索引值
	 */
	public static int bucketIndex(BytesRef ref) {
		return bucketIndex(ref.bytes, ref.offset, ref.length);
	}


	/**
	 * 从 {@link FpGroupDataRebuild} 构建全量内存索引。
	 * <ul>
	 *   <li>hot：对每条 hot 载荷的所有 ngram 窗口打标（含重复窗口）</li>
	 *   <li>common：对每条 common 载荷的去重 ngram 打标，且跳过已出现在 hot 的 slice</li>
	 * </ul>
	 *
	 * @param targetLevel 目标层级
	 * @param group       组数据重建对象，包含 hot/common term order 信息
	 * @return 构建完成的全量内存索引实例
	 */
	public static FpGroupHotNgramBitIndex execute1(int targetLevel, FpGroupDataRebuild group,HashMap<FpTermKey, FPDocList> hotTermToDocs1,
			final ArrayList<java.util.Map.Entry<FpTermKey, FPDocList>> hot_ordered,ArrayList<CommonTermSortEntry> commonTermFlushOrder1) {
		// 获取热词和普通词的 term 数量
		final int h = hot_ordered.size();
		final int c = commonTermFlushOrder1.size();

		// 初始化 hot 和 common 两个 tier（非稀疏模式）
		final TierIndex hotTier = new TierIndex(false);
		final TierIndex commonTier = new TierIndex(false);

	
		// 构建热词 key 集合，用于 common 阶段去重（跳过已在 hot 中出现的 slice）
	
		int order = 1;

		// 遍历所有热词，将其 ngram 切片标记到 hot tier
		for (final java.util.Map.Entry<FpTermKey, FPDocList> e : hot_ordered) {
//			
//			 final FpTermKey key = e.getKey();
//			    final FPDocList val = e.getValue();
			    final Integer index=Integer.valueOf(order++);
			markHotNgramsForPayload(hotTier, e.getKey().bytesRef(),index);
		}
		// 遍历所有普通词，将其去重后的 ngram 切片标记到 common tier（排除 hot 已有的）
		final FpHashMapPoolLease<FpTermKey, Boolean> uniqueSlicesLease = FpHashMapPoolHub.borrow(
				FpHashMapPoolIds.commonPayloadUniqueSlices, FpTermKey.class, Boolean.class, FpTokenBlockLevelPolicy.HASH_MAP_TOKEN_SIZE);
		final HashMap<FpTermKey, Boolean> uniqueSlices = uniqueSlicesLease.map();
		
		try {
		order = 1;
		for (CommonTermSortEntry entry : commonTermFlushOrder1) {
		    final Integer ord=Integer.valueOf(order++);
			markCommonNgramsByUniqueSlices1(uniqueSlices,hotTermToDocs1, commonTier, entry.key.bytesRef(), ord);

		}
		}finally {
			FpHashMapPoolHub.release(uniqueSlicesLease);
		}

	

		// 完成构建：将 HashMap 转为排序数组 + 压缩 order 列表
		hotTier.finalizeRows();
		commonTier.finalizeRows();
		return new FpGroupHotNgramBitIndex(targetLevel, hotTier, commonTier, h, c, false);
	}

	/**
	 * 顺序写出 hot tier + common tier 到磁盘，返回 {@link FpBlockInfo}（含 fpBanksHot / fpBanksCommon 偏移）。
	 *
	 * @param out       输出流
	 * @param from      调用阶段标识（用于日志）
	 * @param fieldInfo 字段信息
	 * @param docCount  文档数
	 * @return 块信息对象，记录各段偏移和大小
	 * @throws IOException IO 异常
	 */
	public FpBlockInfo flushto(IndexOutput out, String from, BytesRef fieldInfo, int docCount) throws IOException {
		final long tsBegin = CLMillisecondClock.CLOCK.now();
		// 记录 hot tier 起始文件指针
		final long fpBanksHot = out.getFilePointer();
		// 写出 hot tier，返回 arena 字节数
		final int hotArenaBytes = hot.writeTier(out);
		// 记录 common tier 起始文件指针
		final long fpBanksCommon = out.getFilePointer();
		// 写出 common tier，返回 arena 字节数
		final int commonArenaBytes = common.writeTier(out);

		// 计算各 tier 序列化后的字节数
		final int bytesPerHotSerialized = (int) (fpBanksCommon - fpBanksHot);
		final int bytesPerCommonSerialized = (int) (out.getFilePointer() - fpBanksCommon);

		// 构造块信息对象
		final FpBlockInfo info = new FpBlockInfo(fpBanksHot, fpBanksCommon, bytesPerHotSerialized,
				bytesPerCommonSerialized, hotArenaBytes, commonArenaBytes, hotCount, commonCount, this.targetlevel,
				fieldInfo, docCount);

		// 构建并输出 flush 日志
		final StringBuilder sb = FpLog.kv();
		FpLog.append(sb, "event", "flush");
		FpLog.append(sb, "phase", from);
		FpLog.append(sb, "perLength", formatFlushStats(hot, common));
		FpLog.append(sb, "hotCount", hotCount);
		FpLog.append(sb, "commonCount", commonCount);
		FpLog.append(sb, "targetLevel", "L" + targetlevel);
		FpLog.append(sb, "block", info);
		final long flushMs = CLMillisecondClock.CLOCK.now() - tsBegin;
		FpLog.append(sb, "ms", flushMs);
		FpLog.infoLineSampled(LOG, FpLog.TAG_BITINDEX, sb, flushMs);
		return info;
	}

	/** 将 hot/common 各 LenRow 分片写入临时目录（skip+keys+meta 与 arena 分离，按 lenIdx 分文件）。 */
	void stageToTemp(Directory dir) throws IOException {
		stageTierToTemp(dir, "hot", hot);
		stageTierToTemp(dir, "common", common);
	}

	private static void stageTierToTemp(Directory dir, String tierName, TierIndex tier) throws IOException {
		for (int lenIdx = 0; lenIdx < Lucene80FPSearchConfig.NGRAM_MAX; lenIdx++) {
			final LenRow row = tier.rows[lenIdx];
			final int entryCount = row.sortedKeys == null ? 0 : row.sortedKeys.length;
			if (entryCount == 0) {
				FpBitIndexDiag.stageLenRow(LOG, tierName, lenIdx, 0);
				continue;
			}
			try (IndexOutput skipOut = dir.createOutput(
					FpBitIndexSegmentStaging.fileName(tierName, FpBitIndexSegmentStaging.PART_SKIP, lenIdx),
					IOContext.DEFAULT)) {
				row.writeLenRowSkipOnly(skipOut);
			}
			try (IndexOutput keysOut = dir.createOutput(
					FpBitIndexSegmentStaging.fileName(tierName, FpBitIndexSegmentStaging.PART_KEYS, lenIdx),
					IOContext.DEFAULT)) {
				row.writeLenRowKeysOnly(keysOut);
			}
			try (IndexOutput orderOut = dir.createOutput(
					FpBitIndexSegmentStaging.fileName(tierName, FpBitIndexSegmentStaging.PART_ORDER, lenIdx),
					IOContext.DEFAULT)) {
				row.writeLenRowOrderOnly(orderOut);
			}
			FpBitIndexDiag.stageLenRow(LOG, tierName, lenIdx, entryCount);
		}
	}

	int targetLevelForWrite() {
		return targetlevel;
	}

	int hotCountForWrite() {
		return hotCount;
	}

	int commonCountForWrite() {
		return commonCount;
	}

	int hotOrderBytesForWrite() {
		return hot.maxOrderBytes();
	}

	int commonOrderBytesForWrite() {
		return common.maxOrderBytes();
	}

	/**
	 * 全量读 hot + common 两个 tier（{@code load* = null} 的便捷入口）。
	 *
	 * @param in      输入流
	 * @param blkinfo 块信息
	 * @return 全量内存索引实例
	 * @throws IOException IO 异常
	 */
	public static FpGroupHotNgramBitIndex readfrom(IndexInput in, FpBlockInfo blkinfo) throws IOException {
		in.seek(blkinfo.fpBanksHot);
		final int hotMagic = in.readInt();
		if (hotMagic != FpBitIndexSegmentStaging.TIER_DIR_MAGIC_HOT) {
			throw new IOException("unsupported bitindex hot magic: 0x" + Integer.toHexString(hotMagic)
					+ " (expected v5 tier directory 0x"
					+ Integer.toHexString(FpBitIndexSegmentStaging.TIER_DIR_MAGIC_HOT) + ")");
		}
		return readfromTierDirectory(in, blkinfo, hotMagic);
	}

	private static FpGroupHotNgramBitIndex readfromTierDirectory(IndexInput in, FpBlockInfo blkinfo, int hotMagic)
			throws IOException {
		final TierIndex hotTier = TierIndex.readTierDirectory(in, blkinfo.fpBanksHot, hotMagic, false,
				blkinfo.bytesPerHotSerialized);
		in.seek(blkinfo.fpBanksCommon);
		final int commonMagic = in.readInt();
		final TierIndex commonTier = TierIndex.readTierDirectory(in, blkinfo.fpBanksCommon, commonMagic, false,
				blkinfo.bytesPerCommonSerialized);
		return new FpGroupHotNgramBitIndex(blkinfo.targetLevel, hotTier, commonTier, blkinfo.hotCount,
				blkinfo.commonCount, false);
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
	 *
	 * @param in             输入流
	 * @param blkinfo        块信息
	 * @param loadHotKeys    需要加载的热词 bucket key 数组，null 表示全量读取
	 * @param loadCommonKeys 需要加载的普通词 bucket key 数组，null 表示全量读取
	 * @return 索引实例（全量或稀疏）
	 * @throws IOException IO 异常
	 */
	public static FpGroupHotNgramBitIndex readfromBanksSelective(IndexInput in, FpBlockInfo blkinfo,
			long[] loadHotKeys, long[] loadCommonKeys) throws IOException {
		// 两个 key 数组都为 null 时，走全量读取路径,在索引合并阶段FpGroupDataOriginal会用到
		if (loadHotKeys == null && loadCommonKeys == null) {
			return readfrom(in, blkinfo);
		}
		// clone 输入流以避免影响外部流的指针位置
		final IndexInput disk = in.clone();
		try {
			disk.seek(blkinfo.fpBanksHot);
			final int hotMagic = disk.readInt();
			if (hotMagic != FpBitIndexSegmentStaging.TIER_DIR_MAGIC_HOT) {
				throw new IOException("unsupported bitindex hot magic: 0x" + Integer.toHexString(hotMagic)
						+ " (expected v5 tier directory)");
			}
			final TierIndex hotTier = loadHotKeys == null || loadHotKeys.length == 0 ? TierIndex.emptySparse()
					: TierIndex.readSelectiveTierDirectory(disk, blkinfo.fpBanksHot, hotMagic, loadHotKeys,
							blkinfo.bytesPerHotSerialized, "hot");
			disk.seek(blkinfo.fpBanksCommon);
			final int commonMagic = disk.readInt();
			final TierIndex commonTier = loadCommonKeys == null || loadCommonKeys.length == 0
					? TierIndex.emptySparse()
					: TierIndex.readSelectiveTierDirectory(disk, blkinfo.fpBanksCommon, commonMagic,
							loadCommonKeys, blkinfo.bytesPerCommonSerialized, "common");
			return new FpGroupHotNgramBitIndex(blkinfo.targetLevel, hotTier, commonTier, blkinfo.hotCount,
					blkinfo.commonCount, true);
		} finally {
			// 确保 clone 的流被关闭
			disk.close();
		}
	}


	/**
	 * 将单个查询 slice 转换为一个 packed bucketKey。
	 *
	 * @param slice 查询切片
	 * @return 包含单个 bucketKey 的数组，长度不合法时返回空数组
	 */
	public static long[] selectiveKeysForSlice(BytesRef slice) {
		// lenIdx = ngram 长度 - 1
		final int lenIdx = slice.length - 1;
		// 长度超出合法范围则返回空数组
		if (lenIdx < 0 || lenIdx >= Lucene80FPSearchConfig.NGRAM_MAX) {
			return EMPTY_LONG;
		}
		return new long[] { packBucketKey(lenIdx, bucketIndex(slice)) };
	}

	/**
	 * 多个查询 slice → 去重后的 bucketKey 数组（供 {@code Terms#fpBits(..., long[], long[])}）。
	 * hot / common 通常传同一组 keys。
	 *
	 * @param slices 查询切片数组
	 * @return 去重后的 bucketKey 数组
	 */
	public static long[] selectiveKeysForSlices(BytesRef[] slices) {
		if (slices == null || slices.length == 0) {
			return EMPTY_LONG;
		}
		final long[] buf = new long[slices.length];
		int n = 0;
		for (BytesRef slice : slices) {
			// 跳过 null 或长度不在合法范围内的 slice
			if (slice == null || slice.length < Lucene80FPSearchConfig.NGRAM_MIN
					|| slice.length > Lucene80FPSearchConfig.NGRAM_MAX) {
				continue;
			}
			// 计算当前 slice 的 packed bucketKey
			final long key = packBucketKey(slice.length - 1, bucketIndex(slice));
			// 线性去重检查（通常 slice 数量较少，线性扫描即可）
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
		// 如果实际数量小于缓冲区大小，截取有效部分
		return n == buf.length ? buf : Arrays.copyOf(buf, n);
	}

	/** 空 long 数组常量 */
	private static final long[] EMPTY_LONG = new long[0];

	/**
	 * 查询 slice 在 hot tier 中命中的 order 列表；
	 * selective 实例在 {@link #readfromBanksSelective} 时已预加载。
	 *
	 * @param slice 查询切片
	 * @return 命中的 order 数组，未命中返回空数组
	 */
	public int[] lookupHotOrders(BytesRef slice) {
		return lookupOrders(hot, slice);
	}

	/**
	 * 查询 slice 在 common tier 中命中的 order 列表；
	 * selective 实例在 {@link #readfromBanksSelective} 时已预加载。
	 *
	 * @param slice 查询切片
	 * @return 命中的 order 数组，未命中返回空数组
	 */
	public int[] lookupCommonOrders(BytesRef slice) {
		return lookupOrders(common, slice);
	}

//	/**
//	 * 通过 lenIdx 和 bucket 直接查找 hot tier 中的 order 列表
//	 *
//	 * @param lenIdx ngram 长度索引
//	 * @param bucket bucket 哈希值
//	 * @return order 数组
//	 */
//	public int[] lookupHotOrders(int lenIdx, int bucket) {
//		return hot.lookup(lenIdx, bucket);
//	}

//	/**
//	 * 通过 lenIdx 和 bucket 直接查找 common tier 中的 order 列表
//	 *
//	 * @param lenIdx ngram 长度索引
//	 * @param bucket bucket 哈希值
//	 * @return order 数组
//	 */
//	public int[] lookupCommonOrders(int lenIdx, int bucket) {
//		return common.lookup(lenIdx, bucket);
//	}

	/**
	 * 在指定 tier 中按 slice 查找 order 列表的内部实现
	 *
	 * @param tier  目标 tier
	 * @param slice 查询切片
	 * @return order 数组，非法输入或未命中返回空数组
	 */
	private static int[] lookupOrders(TierIndex tier, BytesRef slice) {
		// 校验 slice 合法性
		if (slice == null || slice.length < Lucene80FPSearchConfig.NGRAM_MIN
				|| slice.length > Lucene80FPSearchConfig.NGRAM_MAX) {
			return EMPTY_ORDERS;
		}
		return tier.lookup(slice.length - 1, bucketIndex(slice));
	}

	/**
	 * 将一条热词 payload 的所有 ngram 窗口标记到指定 tier。
	 * 对 payload 中每个起始位置、每种合法 ngram 长度，计算 bucketIndex 并添加 order。
	 *
	 * @param tier    目标 tier
	 * @param payload 热词载荷字节
	 * @param order   该 term 在组内的序号
	 */
	private static void markHotNgramsForPayload(TierIndex tier, BytesRef payload, int order) {
		if (order < 1) {
			return;
		}
		final int payloadLen = payload.length;
		if (payloadLen <= 0) {
			return;
		}
		final int base = payload.offset;
		// 复用 BytesRef 作为滑动窗口，避免重复分配
		final BytesRef sliceScratch = new BytesRef();
		sliceScratch.bytes = payload.bytes;
		// 双重循环：外层遍历起始位置，内层遍历 ngram 长度
		for (int start = 0; start < payloadLen; start++) {
			for (int n = Lucene80FPSearchConfig.NGRAM_MIN; n <= Lucene80FPSearchConfig.NGRAM_MAX
					&& start + n <= payloadLen; n++) {
				sliceScratch.offset = base + start;
				sliceScratch.length = n;
				tier.add(n - 1, bucketIndex(sliceScratch), order);
			}
		}
	}

	private static final Boolean UNIQUE_SLICE_MARKER = Boolean.TRUE;

	/**
	 * 将一条普通词 payload 的去重 ngram 切片标记到 common tier，跳过 hot 已有 slice。
	 * <p>
	 * 每个起始位置按 ngram 长度从长到短扫描：若较长 slice 已在 hot 中，则同起点更短 slice 也视为 hot，直接 {@code break}。
	 */
	private static void markCommonNgramsByUniqueSlices1(final HashMap<FpTermKey, Boolean> uniqueSlices,
			HashMap<FpTermKey, FPDocList> hotTermToDocs, TierIndex tier, BytesRef payload, int order) {
		if (order < 1) {
			return;
		}
		final int payloadLen = payload.length;
		if (payloadLen <= 0) {
			return;
		}

		uniqueSlices.clear();
		final int base = payload.offset;
		final byte[] bytes = payload.bytes;
		final BytesRef sliceScratch = new BytesRef();
		sliceScratch.bytes = bytes;
		for (int start = 0; start < payloadLen; start++) {
			final int maxN = Math.min(Lucene80FPSearchConfig.NGRAM_MAX, payloadLen - start);
			for (int n = maxN; n >= Lucene80FPSearchConfig.NGRAM_MIN; n--) {
				sliceScratch.offset = base + start;
				sliceScratch.length = n;
				final int h = FpTermKeyHash.hashOf(bytes, base + start, n);
				final FpTermKey key = FpTermKey.viewOf(sliceScratch, h);
				if (hotTermToDocs.containsKey(key)) {
					break;
				}
				if (uniqueSlices.containsKey(key)) {
					continue;
				}
				final BytesRef sliceScratch_copy = new BytesRef();
				sliceScratch_copy.bytes = bytes;
				sliceScratch_copy.offset = sliceScratch.offset;
				sliceScratch_copy.length = sliceScratch.length;
				final FpTermKey storedKey = FpTermKey.viewOf(sliceScratch_copy, key.hashCode());
				uniqueSlices.put(storedKey, UNIQUE_SLICE_MARKER);
				tier.add(n - 1, bucketIndex(sliceScratch), order);
			}
		}
	}

	/**
	 * 格式化 flush 统计信息字符串，按 ngram 长度分别列出 hot/common 的 entry 数量
	 *
	 * @param hotTier    热词 tier
	 * @param commonTier 普通词 tier
	 * @return 格式化的统计字符串
	 */
	private static String formatFlushStats(TierIndex hotTier, TierIndex commonTier) {
		final StringBuilder sb = new StringBuilder(256);
		for (int li = 0; li < Lucene80FPSearchConfig.NGRAM_MAX; li++) {
			sb.append(" len").append(li + 1).append("{hot entries=").append(hotTier.rows[li].entryCount())
					.append(" | common entries=").append(commonTier.rows[li].entryCount()).append('}');
		}
		return sb.toString();
	}

//	/** 获取组内热词 term 数量 */
//	public int getHotCount() {
//		return hotCount;
//	}

//	/** 获取组内普通词 term 数量 */
//	public int getCommonCount() {
//		return commonCount;
//	}

//	/** 获取 hot tier 最大 arena 字节数 */
//	public int getHotNumBits() {
//		return hot.maxArenaBytes();
//	}
//
//	/** 获取 common tier 最大 arena 字节数 */
//	public int getCommonNumBits() {
//		return common.maxArenaBytes();
//	}

//	/**
//	 * 是否为稀疏模式。
//	 * true 表示由 selective 读或 {@link #viewSelective} 构造，仅含部分 bucket。
//	 */
//	public boolean isSparse() {
//		return sparse;
//	}

//	/** 当前 hot tier 已加载的 bucket 条数（sparse 实例可能远小于 {@link #getHotCount()}）。 */
//	public int loadedHotBucketCount() {
//		return hot.totalEntryCount();
//	}

//	/** 当前 common tier 已加载的 bucket 条数。 */
//	public int loadedCommonBucketCount() {
//		return common.totalEntryCount();
//	}

//	/**
//	 * 在全量内存实例上截取 selective 视图（测试或上游仅内存缓存时可用）。
//	 * 不会触发磁盘 IO，直接从内存中筛选指定 key 对应的 order。
//	 *
//	 * @param hotKeys    需要保留的热词 bucket key 数组
//	 * @param commonKeys 需要保留的普通词 bucket key 数组
//	 * @return 新的稀疏模式索引实例
//	 */
//	public FpGroupHotNgramBitIndex viewSelective(long[] hotKeys, long[] commonKeys) {
//		return new FpGroupHotNgramBitIndex(targetlevel, hot.viewSelective(hotKeys), common.viewSelective(commonKeys),
//				hotCount, commonCount, true);
//	}

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
	 *       每行有完整的 sortedKeys + entryMeta + orderBytes</li>
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
		/** 6 行 LenRow，下标 0..5 对应 ngram 长度 1..6 */
		final LenRow[] rows = new LenRow[Lucene80FPSearchConfig.NGRAM_MAX];

		/**
		 * 构造函数
		 *
		 * @param sparse true 为稀疏模式（不初始化 buildMap），false 为全量构建模式
		 */
		TierIndex(boolean sparse) {
			for (int i = 0; i < rows.length; i++) {
				rows[i] = new LenRow(sparse);
			}
		}

		/** 创建一个空的稀疏 TierIndex 实例 */
		static TierIndex emptySparse() {
			return new TierIndex(true);
		}

		/**
		 * 向指定长度行的指定 bucket 添加一个 order
		 *
		 * @param lenIdx ngram 长度索引
		 * @param bucket bucket 哈希值
		 * @param order  term 序号
		 */
		void add(int lenIdx, int bucket, int order) {
			rows[lenIdx].add(bucket, order);
		}

		/** 完成所有行的构建：将 HashMap 转为排序数组 + 压缩结构 */
		void finalizeRows() {
			for (LenRow row : rows) {
				try {
					row.finalizeRow();
				} catch (IOException e) {
					throw new AssertionError(e);
				}
			}
		}

		/** 归还各行仍持有的池化 HashMap（稀疏 selective 读路径）。 */
		void releasePooledMaps() {
			for (LenRow row : rows) {
				row.releasePooledMaps();
			}
		}

//		/** 返回所有行中最大的 orderBytes 字节数 */
		int maxOrderBytes() {
			int max = 0;
			for (LenRow row : rows) {
				max = Math.max(max, row.orderBytes == null ? 0 : row.orderBytes.length);
			}
			return max;
		}

//		/** 返回所有行的 entry 总数 */
//		int totalEntryCount() {
//			int n = 0;
//			for (LenRow row : rows) {
//				n += row.entryCount();
//			}
//			return n;
//		}

		/**
		 * 在指定长度行中查找 bucket 对应的 order 列表
		 *
		 * @param lenIdx ngram 长度索引
		 * @param bucket bucket 哈希值
		 * @return order 数组
		 */
		int[] lookup(int lenIdx, int bucket) {
			return rows[lenIdx].lookup(bucket);
		}

//		/**
//		 * 从全量内存实例中截取 selective 视图。
//		 * 不走磁盘 skip，直接在 sortedKeys 上二分查找。
//		 *
//		 * @param keys 需要保留的 packed bucketKey 数组
//		 * @return 新的稀疏 TierIndex
//		 */
//		TierIndex viewSelective(long[] keys) {
//			if (keys == null || keys.length == 0) {
//				return emptySparse();
//			}
//			// 内存全量实例上的 selective 视图：不走磁盘 skip，直接 binarySearch sortedKeys
//			final TierIndex sparseTier = emptySparse();
//			for (long key : keys) {
//				final int lenIdx = unpackLenIdx(key);
//				final int bucket = unpackBucketIndex(key);
//				// 从全量实例中查找对应 order
//				final int[] orders = lookup(lenIdx, bucket);
//				if (orders.length > 0) {
//					// 将找到的 order 放入稀疏实例
//					sparseTier.rows[lenIdx].putSparse(bucket, orders);
//				}
//			}
//			return sparseTier;
//		}

		/**
		 * 将整个 tier 写入磁盘。
		 * 先写入 RAM 缓冲（回填 lenRowOffset），再一次性写入 {@link DataOutput}。
		 *
		 * <p>写入内容（按照磁盘布局顺序）：
		 * <ul>
		 *   <li>magic: int ('FPTR')</li>
		 *   <li>lenRowOffset[6]: long × 6</li>
		 *   <li>LenRow(len=1..6) 各自的数据</li>
		 * </ul>
		 *
		 * @param out 输出流
		 * @return 所有行 orderBytes 的总字节数
		 * @throws IOException IO 异常
		 */
		int writeTier(DataOutput out) throws IOException {
			// 使用 RAM 缓冲以便回填 lenRowOffset
			final RAMOutputStream ram = new RAMOutputStream();
			// 写入 tier 魔数
			ram.writeInt(TIER_MAGIC);
			// 预留 6 个 long 的位置用于存放各 LenRow 的偏移
			final int lenTablePos = 4;
			for (int i = 0; i < rows.length; i++) {
				ram.writeLong(0L);
			}
			// 依次写出每个 LenRow，并记录其偏移
			final long[] lenOffsets = new long[rows.length];
			for (int i = 0; i < rows.length; i++) {
				lenOffsets[i] = ram.getFilePointer();
				rows[i].writeLenRow(ram);
			}
			// 将 RAM 缓冲内容拷贝到字节数组
			final int len = Math.toIntExact(ram.getFilePointer());
			final byte[] buf = new byte[len];
			ram.writeTo(buf, 0);
			// 回填各 LenRow 的偏移到字节数组头部
			for (int i = 0; i < lenOffsets.length; i++) {
				putLong(buf, lenTablePos + i * 8, lenOffsets[i]);
			}
			// 一次性写入目标输出流
			out.writeBytes(buf, 0, len);
			// 统计所有行 arena 总字节数
			int totalArena = 0;
			for (LenRow row : rows) {
				totalArena += row.orderBytes == null ? 0 : row.orderBytes.length;
			}
			return totalArena;
		}

		/**
		 * 手动将 long 值以大端序写入字节数组指定位置
		 *
		 * @param data 目标字节数组
		 * @param pos  写入起始位置
		 * @param v    要写入的 long 值
		 */
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

		/**
		 * 全量读取一个 tier。
		 * 校验 magic 后顺序读取 6 个 LenRow（skip 表内容被跳过但 keys/meta/arena 全部载入内存）。
		 *
		 * @param in     输入流
		 * @param sparse 是否以稀疏模式读取（通常全量读为 false）
		 * @return 读取完成的 TierIndex
		 * @throws IOException IO 异常或 magic 校验失败
		 */
		static TierIndex readTier(DataInput in, boolean sparse) throws IOException {
			// 读取并校验 tier 魔数
			final int magic = in.readInt();
			if (magic != TIER_MAGIC) {
				throw new IOException("unexpected tier magic: " + magic);
			}
			// 跳过 lenRow 目录表（6 × long）；readLenRow 会顺序消费后续字节流
			for (int i = 0; i < Lucene80FPSearchConfig.NGRAM_MAX; i++) {
				in.readLong();
			}
			// 根据 sparse 参数创建对应模式的 TierIndex
			final TierIndex tier = sparse ? emptySparse() : new TierIndex(false);
			// 顺序读取每个 LenRow
			for (int i = 0; i < tier.rows.length; i++) {
				tier.rows[i] = LenRow.readLenRow(in, sparse);
			}
			return tier;
		}

		static TierIndex readTierDirectory(IndexInput disk, long tierDirOffset, int magic, boolean sparse,
				int tierDirectorySerializedBytes) throws IOException {
			if (tierDirectorySerializedBytes != FpBitIndexSegmentStaging.tierDirectorySerializedBytes()) {
				throw new IOException("unsupported tier directory bytes: " + tierDirectorySerializedBytes);
			}
			final TierIndex tier = sparse ? emptySparse() : new TierIndex(false);
			for (int i = 0; i < tier.rows.length; i++) {
				final TierLenOffsets offsets = readTierOffsetsForLen(disk, tierDirOffset, magic, i);
				tier.rows[i] = LenRow.readLenRowSplit(disk, offsets.skipOff, offsets.keysOff, offsets.orderOff,
						sparse);
			}
			return tier;
		}

		/**
		 * v5 selective：按需读 lenIdx 槽；Bloom shard 与 skip 延迟加载。
		 */
		static TierIndex readSelectiveTierDirectory(IndexInput disk, long tierDirOffset, int magic, long[] keys,
				int tierDirectorySerializedBytes, String tierLabel) throws IOException {
			if (keys == null || keys.length == 0) {
				return emptySparse();
			}
			if (tierDirectorySerializedBytes != FpBitIndexSegmentStaging.tierDirectorySerializedBytes()) {
				throw new IOException("unsupported tier directory bytes: " + tierDirectorySerializedBytes);
			}
			final TierIndex tier = emptySparse();
			final DiskLenRow[] diskRows = new DiskLenRow[Lucene80FPSearchConfig.NGRAM_MAX];
			for (long key : keys) {
				final int lenIdx = unpackLenIdx(key);
				if (lenIdx < 0 || lenIdx >= Lucene80FPSearchConfig.NGRAM_MAX) {
					continue;
				}
				if (diskRows[lenIdx] == null) {
					final TierLenOffsets offsets = readTierOffsetsForLen(disk, tierDirOffset, magic, lenIdx);
					FpBitIndexDiag.selectiveTierRead(LOG, tierLabel, lenIdx, offsets.skipOff, offsets.keysOff,
							offsets.orderOff);
					if (offsets.skipOff <= 0 && offsets.keysOff <= 0) {
						continue;
					}
					diskRows[lenIdx] = DiskLenRow.openAt(disk, lenIdx, offsets.skipOff, offsets.keysOff,
							offsets.orderOff);
				}
				final int bucket = unpackBucketIndex(key);
				final int[] orders = diskRows[lenIdx].lookupOrders(bucket);
				if (orders.length > 0) {
					tier.rows[lenIdx].putSparse(bucket, orders);
				}
			}
			return tier;
		}

		private static TierLenOffsets readTierOffsetsForLen(IndexInput disk, long tierDirOffset, int magic, int lenIdx)
				throws IOException {
			if (lenIdx < 0 || lenIdx >= Lucene80FPSearchConfig.NGRAM_MAX) {
				throw new IOException("invalid lenIdx: " + lenIdx);
			}
			disk.seek(tierDirOffset);
			if (disk.readInt() != magic) {
				throw new IOException("unexpected tier directory magic: " + magic);
			}
			final long slotBase = tierDirOffset + 4L + (long) lenIdx * FpBitIndexSegmentStaging.TIER_DIR_LONGS_PER_LEN
					* Long.BYTES;
			disk.seek(slotBase);
			final TierLenOffsets o = new TierLenOffsets();
			o.skipOff = disk.readLong();
			o.keysOff = disk.readLong();
			o.orderOff = disk.readLong();
			return o;
		}

		static final class TierLenOffsets {
			long skipOff;
			long keysOff;
			long orderOff;
		}

	}

	// DiskLenRow — v6：skip(min/max) 延迟加载；keys/order 分池
	// -------------------------------------------------------------------------

	static final class DiskLenRow {
		final IndexInput in;
		final int lenIdx;
		final long skipAbs;
		final long keysAbs;
		final long orderDataStart;

		int entryCount = -1;
		int skipCount;
		int[] skipMin;
		int[] skipMax;
		long[] skipKeysPtrRel;
		boolean skipLoaded;

		private DiskLenRow(IndexInput in, int lenIdx, long skipAbs, long keysAbs, long orderDataStart) {
			this.in = in;
			this.lenIdx = lenIdx;
			this.skipAbs = skipAbs;
			this.keysAbs = keysAbs;
			this.orderDataStart = orderDataStart;
		}

		static DiskLenRow openAt(IndexInput in, int lenIdx, long skipAbs, long keysAbs, long orderAbs) {
			if (skipAbs <= 0 && keysAbs <= 0) {
				return new DiskLenRow(in, lenIdx, 0L, 0L, orderAbs);
			}
			return new DiskLenRow(in, lenIdx, skipAbs, keysAbs, orderAbs);
		}

		private int ensureEntryCount() throws IOException {
			if (entryCount >= 0) {
				return entryCount;
			}
			if (skipAbs <= 0) {
				entryCount = 0;
				return 0;
			}
			in.seek(skipAbs);
			entryCount = in.readInt();
			return entryCount;
		}

		private void ensureSkipLoaded() throws IOException {
			if (skipLoaded || skipAbs <= 0) {
				return;
			}
			final int n = ensureEntryCount();
			if (n == 0) {
				skipLoaded = true;
				return;
			}
			skipCount = (n + SKIP_INTERVAL - 1) / SKIP_INTERVAL;
			skipMin = new int[skipCount];
			skipMax = new int[skipCount];
			skipKeysPtrRel = new long[skipCount];
			final long skipTableStart = skipAbs + 4L;
			for (int s = 0; s < skipCount; s++) {
				in.seek(skipTableStart + (long) s * SKIP_ENTRY_BYTES);
				skipMin[s] = in.readInt();
				skipMax[s] = in.readInt();
				skipKeysPtrRel[s] = in.readLong();
			}
			skipLoaded = true;
			FpBitIndexDiag.skipLazyLoad(LOG, lenIdx, n, skipCount, skipMin[0], skipMax[skipCount - 1]);
		}

		int[] lookupOrders(int bucket) throws IOException {
			if (ensureEntryCount() == 0) {
				return EMPTY_ORDERS;
			}
			ensureSkipLoaded();
			if (bucket < skipMin[0] || bucket > skipMax[skipCount - 1]) {
				FpBitIndexDiag.lookupOrders(LOG, lenIdx, bucket, "skipReject", 0);
				return EMPTY_ORDERS;
			}
			int segment = 0;
			int lo = 0;
			int hi = skipCount - 1;
			while (lo <= hi) {
				final int mid = (lo + hi) >>> 1;
				if (skipMin[mid] <= bucket) {
					segment = mid;
					lo = mid + 1;
				} else {
					hi = mid - 1;
				}
			}
			if (segment + 1 < skipCount && bucket >= skipMin[segment + 1]) {
				FpBitIndexDiag.lookupOrders(LOG, lenIdx, bucket, "skipGap", 0);
				return EMPTY_ORDERS;
			}
			if (bucket < skipMin[segment] || bucket > skipMax[segment]) {
				FpBitIndexDiag.lookupOrders(LOG, lenIdx, bucket, "skipReject", 0);
				return EMPTY_ORDERS;
			}
			final int segStart = segment * SKIP_INTERVAL;
			final int segEnd = Math.min(segStart + SKIP_INTERVAL, entryCount);
			final long segKeysBase = keysAbs + skipKeysPtrRel[segment] - (long) segStart * 4L;
			int found = -1;
			for (int i = segStart; i < segEnd; i++) {
				in.seek(segKeysBase + (long) i * 4L);
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
				FpBitIndexDiag.lookupOrders(LOG, lenIdx, bucket, "keyMiss", 0);
				return EMPTY_ORDERS;
			}
			in.seek(keysAbs + (long) entryCount * 4L + (long) found * 4L);
			final int meta = in.readInt();
			if ((meta & 1) == ENTRY_TAG_SINGLE) {
				FpBitIndexDiag.lookupOrders(LOG, lenIdx, bucket, "hitSingle", 1);
				return new int[] { meta >>> 1 };
			}
			final int[] orders = readOrderListFromInput(in, orderDataStart + (meta >>> 1));
			FpBitIndexDiag.lookupOrders(LOG, lenIdx, bucket, "hitOrder", orders.length);
			return orders;
		}
	}

	/**
	 * 从 IndexInput 的指定偏移处读取 vint 压缩的 order 列表。
	 * 格式：vint(count) + vint(delta order) × count（增量编码，升序）。
	 *
	 * @param in     输入流
	 * @param offset arena 内的字节偏移
	 * @return 解码后的 order 数组
	 * @throws IOException IO 异常
	 */
	static int[] readOrderListFromInput(IndexInput in, long offset) throws IOException {
		in.seek(offset);
		// 读取 order 数量
		final int count = in.readVInt();
		if (count <= 0) {
			return EMPTY_ORDERS;
		}
		final int[] orders = new int[count];
		int prev = 0;
		// 逐个读取增量值并累加还原绝对 order
		for (int i = 0; i < count; i++) {
			prev += in.readVInt();
			orders[i] = prev;
		}
		return orders;
	}

	// -------------------------------------------------------------------------
	// LenRow — 单一 ngram 长度下的 sorted posting 表 + orderBytes
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
	 * │ orderBytes      : byte × arenaLen        // 多 order 的 vint 压缩池        │
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
	 * <p>构建期用 {@link HashMap} 聚合；{@link #finalizeRow()} 后变为 sortedKeys + entryMeta + orderBytes。
	 * selective 读在 {@link TierIndex#readSelective} 预取到 {@link #sparseOrders}；{@link #viewSelective} 从全量实例截取。
	 */
	static final class LenRow {
		/** 构建期租约；{@link #finalizeRow()} 后归还池 */
		private FpHashMapPoolLease<Integer, IntList> buildMapLease;
		/** 构建期使用的 HashMap（bucket → order 列表），finalize 后 null */
		private HashMap<Integer, IntList> buildMap;
		/** 稀疏读租约；{@link #releasePooledMaps()} 归还池 */
		private FpHashMapPoolLease<Integer, int[]> sparseOrdersLease;
		/** 稀疏模式下存储已加载的 bucket → order 映射 */
		private HashMap<Integer, int[]> sparseOrders;

		/** 排序后的 bucket 数组（全量模式） */
		int[] sortedKeys;
		/** 与 sortedKeys 对应的 entryMeta 数组（全量模式） */
		int[] entryMeta;
		/** 多 order 的 vint 增量压缩池（全量模式） */
		byte[] orderBytes;

		/**
		 * 构造函数
		 *
		 * @param sparse true 为稀疏模式（不初始化 buildMap），false 为全量构建模式
		 */
		LenRow(boolean sparse) {
		}

		private void ensureBuildMap() {
			if (buildMap != null) {
				return;
			}
			buildMapLease = FpHashMapPoolHub.borrow(FpHashMapPoolIds.lenRowBuildMap, Integer.class, IntList.class,
					FpTokenBlockLevelPolicy.HASH_MAP_DEFAULT_SIZE);
			buildMap = buildMapLease.map();
		}

		private void ensureSparseOrders() {
			if (sparseOrders != null) {
				return;
			}
			sparseOrdersLease = FpHashMapPoolHub.borrow(FpHashMapPoolIds.lenRowSparseOrders, Integer.class,
					int[].class, 64);
			sparseOrders = sparseOrdersLease.map();
		}

		/** 归还本行借用的 HashMap（构建 map 应在 {@link #finalizeRow()} 已释放；稀疏 map 由调用方在用完索引后调用）。 */
		void releasePooledMaps() {
			releaseSparseOrdersLease();
		}

		private void releaseBuildMapLease() {
			if (buildMapLease != null) {
				FpHashMapPoolHub.release(buildMapLease);
				buildMapLease = null;
				buildMap = null;
			}
		}

		private void releaseSparseOrdersLease() {
			if (sparseOrdersLease != null) {
				FpHashMapPoolHub.release(sparseOrdersLease);
				sparseOrdersLease = null;
				sparseOrders = null;
			}
		}

		/**
		 * 构建期：向指定 bucket 添加一个 order
		 *
		 * @param bucket bucket 哈希值
		 * @param order  term 序号
		 */
		void add(int bucket, int order) {
			ensureBuildMap();
			IntList list = buildMap.get(bucket);
			if (list == null) {
				list = new IntList();
				buildMap.put(bucket, list);
			}
			list.add(order);
		}

		/**
		 * 稀疏模式：直接放入已解析的 bucket → order 映射
		 *
		 * @param bucket bucket 哈希值
		 * @param orders 该 bucket 对应的 order 数组
		 */
		void putSparse(int bucket, int[] orders) {
			ensureSparseOrders();
			sparseOrders.put(bucket, orders);
		}

		/**
		 * 返回当前行的 entry 数量。
		 * 稀疏模式返回 sparseOrders 大小，全量模式返回 sortedKeys 长度。
		 */
		int entryCount() {
			if (sparseOrders != null) {
				return sparseOrders.size();
			}
			return sortedKeys == null ? 0 : sortedKeys.length;
		}

		/**
		 * 构建结束：将 HashMap 转换为排序数组 + 压缩 order 列表。
		 * <ul>
		 *   <li>sortedKeys：bucket 升序数组</li>
		 *   <li>entryMeta：单 order 内联 / 多 order 指向 arena 偏移</li>
		 *   <li>orderBytes：多 order 的 vint 增量压缩字节池</li>
		 * </ul>
		 *
		 * @throws IOException RAMOutputStream 写入异常（理论上不应发生）
		 */
		void finalizeRow() throws IOException {
			// 空行处理
			if (buildMap == null || buildMap.isEmpty()) {
				sortedKeys = EMPTY_INT;
				entryMeta = EMPTY_INT;
				orderBytes = EMPTY_BYTES;
				releaseBuildMapLease();
				return;
			}
			final int n = buildMap.size();
			sortedKeys = new int[n];
			entryMeta = new int[n];
			int keyIdx = 0;
			for (Integer bucketKey : buildMap.keySet()) {
				sortedKeys[keyIdx++] = bucketKey.intValue();
			}
			Arrays.sort(sortedKeys);
			final RAMOutputStream orderOut = new RAMOutputStream();
			for (int i = 0; i < n; i++) {
				final IntList list = buildMap.get(sortedKeys[i]);
				// 将 order 列表转为排序去重数组
				final int[] orders = list.toSortedArray();
				if (orders.length == 1) {
					// 单 order：内联到 entryMeta 高 31 位，低 1 位为 TAG_SINGLE
					entryMeta[i] = (orders[0] << 1) | ENTRY_TAG_SINGLE;
				} else {
					// 多 order：写入 arena，entryMeta 存储偏移，低 1 位为 TAG_MULTI
					final int orderOffset = Math.toIntExact(orderOut.getFilePointer());
					writeOrderList(orderOut, orders);
					entryMeta[i] = (orderOffset << 1) | ENTRY_TAG_MULTI;
				}
			}
			// 将 arena 缓冲拷贝到字节数组
			orderBytes = new byte[Math.toIntExact(orderOut.getFilePointer())];
			orderOut.writeTo(orderBytes, 0);
			releaseBuildMapLease();
		}

		/**
		 * 查找指定 bucket 对应的 order 列表。
		 * 稀疏模式直接查 HashMap；全量模式二分查找 sortedKeys 后解码 entryMeta。
		 *
		 * @param bucket 目标 bucket 哈希值
		 * @return order 数组，未找到返回空数组
		 */
		int[] lookup(int bucket) {
			// 稀疏模式：HashMap 直接查找
			if (sparseOrders != null) {
				final int[] orders = sparseOrders.get(bucket);
				return orders == null ? EMPTY_ORDERS : orders;
			}
			// 全量模式：二分查找
			if (sortedKeys == null || sortedKeys.length == 0) {
				return EMPTY_ORDERS;
			}
			final int idx = Arrays.binarySearch(sortedKeys, bucket);
			if (idx < 0) {
				return EMPTY_ORDERS;
			}
			return decodeOrders(entryMeta[idx]);
		}

		/**
		 * 根据 entryMeta 解码 order 列表。
		 * 单 order 直接从 meta 提取；多 order 从 orderBytes 读取 vint 列表。
		 *
		 * @param meta entryMeta 值
		 * @return 解码后的 order 数组
		 */
		int[] decodeOrders(int meta) {
			if ((meta & 1) == ENTRY_TAG_SINGLE) {
				// 单 order：高 31 位即为 order 值
				return new int[] { meta >>> 1 };
			}
			// 多 order：高 31 位为 arena 偏移
			final int orderOffset = meta >>> 1;
			return readOrderList(orderBytes, orderOffset);
		}

		/**
		 * 将 LenRow 写出到 {@link DataOutput}（flush 阶段，由 {@link TierIndex#writeTier} 调用）。
		 * 按照磁盘布局依次写出：entryCount → skip 表 → sortedKeys → entryMeta → arenaLen → orderBytes。
		 *
		 * @param out 输出流
		 * @throws IOException IO 异常
		 */
		void writeLenRow(DataOutput out) throws IOException {
			final int entryCount = sortedKeys == null ? 0 : sortedKeys.length;
			// 记录 LenRow 起始位置（用于计算相对偏移）
			final long lenRowStart = getOutputFilePointer(out);
			// 写出 entry 数量
			out.writeInt(entryCount);
			if (entryCount == 0) {
				return;
			}
			// 计算 skip 表条目数
			final int skipCount = (entryCount + SKIP_INTERVAL - 1) / SKIP_INTERVAL;
			// 预计算 sortedKeys[0] 相对 LenRow 起点的偏移（skip 表之后紧接 keys）
			final long keysStartRel = getOutputFilePointer(out) + (long) skipCount * SKIP_ENTRY_BYTES - lenRowStart;
			for (int s = 0; s < skipCount; s++) {
				final int entryIdx = s * SKIP_INTERVAL;
				final int segEndIdx = Math.min(entryIdx + SKIP_INTERVAL, entryCount) - 1;
				out.writeInt(sortedKeys[entryIdx]);
				out.writeInt(sortedKeys[segEndIdx]);
				out.writeLong(keysStartRel + (long) entryIdx * 4L);
			}
			// 写出所有 sortedKeys
			for (int k = 0; k < entryCount; k++) {
				out.writeInt(sortedKeys[k]);
			}
			// 写出所有 entryMeta
			for (int k = 0; k < entryCount; k++) {
				out.writeInt(entryMeta[k]);
			}
			// 写出 arena 长度和数据
			out.writeInt(orderBytes == null ? 0 : orderBytes.length);
			if (orderBytes != null && orderBytes.length > 0) {
				out.writeBytes(orderBytes, 0, orderBytes.length);
			}
		}

		/** v6：仅 skip 池 — entryCount + skip(min,max,keysPtrRel)。 */
		void writeLenRowSkipOnly(DataOutput out) throws IOException {
			final int n = sortedKeys == null ? 0 : sortedKeys.length;
			out.writeInt(n);
			if (n == 0) {
				return;
			}
			writeSkipTable(out, n, 0L);
		}

		private void writeSkipTable(DataOutput out, int entryCount, long keysPtrRelBase) throws IOException {
			final int skipCount = (entryCount + SKIP_INTERVAL - 1) / SKIP_INTERVAL;
			for (int s = 0; s < skipCount; s++) {
				final int entryIdx = s * SKIP_INTERVAL;
				final int segEndIdx = Math.min(entryIdx + SKIP_INTERVAL, entryCount) - 1;
				out.writeInt(sortedKeys[entryIdx]);
				out.writeInt(sortedKeys[segEndIdx]);
				out.writeLong(keysPtrRelBase + (long) entryIdx * 4L);
			}
		}

		/** v6：keys 池 — sortedKeys + entryMeta + orderLen。 */
		void writeLenRowKeysOnly(DataOutput out) throws IOException {
			final int entryCount = sortedKeys == null ? 0 : sortedKeys.length;
			if (entryCount == 0) {
				return;
			}
			for (int k = 0; k < entryCount; k++) {
				out.writeInt(sortedKeys[k]);
			}
			for (int k = 0; k < entryCount; k++) {
				out.writeInt(entryMeta[k]);
			}
			out.writeInt(orderBytes == null ? 0 : orderBytes.length);
		}

		/** v6：仅写出 order 多值池正文。 */
		void writeLenRowOrderOnly(DataOutput out) throws IOException {
			if (orderBytes != null && orderBytes.length > 0) {
				out.writeBytes(orderBytes, 0, orderBytes.length);
			}
		}

		/** v6 全量读：skip / keys / order 分池。 */
		static LenRow readLenRowSplit(IndexInput in, long skipAbs, long keysAbs, long orderAbs, boolean sparse)
				throws IOException {
			final LenRow row = new LenRow(sparse);
			if (skipAbs <= 0 && keysAbs <= 0) {
				row.sortedKeys = EMPTY_INT;
				row.entryMeta = EMPTY_INT;
				row.orderBytes = EMPTY_BYTES;
				return row;
			}
			int entryCount = 0;
			if (skipAbs > 0) {
				in.seek(skipAbs);
				entryCount = in.readInt();
				if (entryCount > 0) {
					final int skipCount = (entryCount + SKIP_INTERVAL - 1) / SKIP_INTERVAL;
					for (int s = 0; s < skipCount; s++) {
						in.readInt();
						in.readInt();
						in.readLong();
					}
				}
			}
			if (entryCount == 0 || keysAbs <= 0) {
				row.sortedKeys = EMPTY_INT;
				row.entryMeta = EMPTY_INT;
				row.orderBytes = EMPTY_BYTES;
				return row;
			}
			in.seek(keysAbs);
			row.sortedKeys = new int[entryCount];
			for (int i = 0; i < entryCount; i++) {
				row.sortedKeys[i] = in.readInt();
			}
			row.entryMeta = new int[entryCount];
			for (int i = 0; i < entryCount; i++) {
				row.entryMeta[i] = in.readInt();
			}
			final int orderLen = in.readInt();
			if (orderLen <= 0 || orderAbs <= 0) {
				row.orderBytes = EMPTY_BYTES;
			} else {
				in.seek(orderAbs);
				row.orderBytes = new byte[orderLen];
				in.readBytes(row.orderBytes, 0, orderLen);
			}
			return row;
		}

		/**
		 * 获取 DataOutput 的当前文件指针位置。
		 * 兼容 RAMOutputStream 和 IndexOutput 两种类型。
		 *
		 * @param out 输出流
		 * @return 当前文件指针位置
		 */
		private static long getOutputFilePointer(DataOutput out) {
			if (out instanceof RAMOutputStream) {
				return ((RAMOutputStream) out).getFilePointer();
			}
			return out instanceof IndexOutput ? ((IndexOutput) out).getFilePointer() : 0L;
		}

		/**
		 * 全量读 LenRow（{@link TierIndex#readTier} 路径）。
		 * skip 表内容读入后直接丢弃——全量路径本来就要读完整 keys/meta/arena。
		 *
		 * @param in     输入流
		 * @param sparse 是否以稀疏模式读取
		 * @return 读取完成的 LenRow
		 * @throws IOException IO 异常
		 */
		static LenRow readLenRow(DataInput in, boolean sparse) throws IOException {
			final LenRow row = new LenRow(sparse);
			// 读取 entry 数量
			final int entryCount = in.readInt();
			if (entryCount == 0) {
				row.sortedKeys = EMPTY_INT;
				row.entryMeta = EMPTY_INT;
				row.orderBytes = EMPTY_BYTES;
				return row;
			}
			// 跳过 skip 表（全量读不需要）
			final int skipCount = (entryCount + SKIP_INTERVAL - 1) / SKIP_INTERVAL;
			for (int s = 0; s < skipCount; s++) {
				in.readInt();
				in.readInt();
				in.readLong();
			}
			// 读取 sortedKeys
			row.sortedKeys = new int[entryCount];
			for (int i = 0; i < entryCount; i++) {
				row.sortedKeys[i] = in.readInt();
			}
			// 读取 entryMeta
			row.entryMeta = new int[entryCount];
			for (int i = 0; i < entryCount; i++) {
				row.entryMeta[i] = in.readInt();
			}
			// 读取 arena 数据
			final int arenaLen = in.readInt();
			if (arenaLen <= 0) {
				row.orderBytes = EMPTY_BYTES;
			} else {
				row.orderBytes = new byte[arenaLen];
				in.readBytes(row.orderBytes, 0, arenaLen);
			}
			return row;
		}

		/**
		 * selective 单 bucket 跳跃读（测试或独立调用）；
		 * 生产环境由 {@link TierIndex#readSelective} 预取到 {@link #sparseOrders}。
		 *
		 * @param in     输入流（指针应位于 LenRow 起点）
		 * @param bucket 目标 bucket 哈希值
		 * @return order 数组
		 * @throws IOException IO 异常
		 */
		static int[] readOrdersForBucket(IndexInput in, int bucket) throws IOException {
			throw new UnsupportedOperationException("use selective tier directory read in v6");
		}
	}

	/**
	 * 将 order 列表以 vint 增量压缩格式写入 DataOutput。
	 * 格式：vint(count) + vint(order[i] - order[i-1]) × count
	 *
	 * @param out    输出流
	 * @param orders 升序排列的 order 数组
	 * @throws IOException IO 异常
	 */
	static void writeOrderList(DataOutput out, int[] orders) throws IOException {
		out.writeVInt(orders.length);
		int prev = 0;
		for (int order : orders) {
			// 写入增量值
			out.writeVInt(order - prev);
			prev = order;
		}
	}

	/**
	 * 从字节数组的指定偏移处读取 vint 增量压缩的 order 列表。
	 * 通过 {@link #nextVIntPos} 传递每次 vint 读取后的位置。
	 *
	 * @param arena  orderBytes 字节数组
	 * @param offset 起始偏移
	 * @return 解码后的 order 数组
	 */
	static int[] readOrderList(byte[] arena, int offset) {
		try {
			int pos = offset;
			// 读取 order 数量
			final int count = readVIntAt(arena, pos);
			pos = nextVIntPos;
			if (count <= 0) {
				return EMPTY_ORDERS;
			}
			final int[] orders = new int[count];
			int prev = 0;
			// 逐个读取增量值并累加还原
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

	/** 线程不安全：记录上一次 readVIntAt 读取结束后的位置 */
	private static int nextVIntPos;

	/**
	 * 从字节数组指定偏移处读取一个 vint 值。
	 * 读取完成后通过 {@link #nextVIntPos} 返回下一个可读位置。
	 *
	 * @param bytes  字节数组
	 * @param offset 起始偏移
	 * @return 解码后的 int 值
	 * @throws IOException 越界或 vint 过长时抛出
	 */
	private static int readVIntAt(byte[] bytes, int offset) throws IOException {
		int shift = 0;
		int value = 0;
		int pos = offset;
		while (true) {
			if (pos >= bytes.length) {
				throw new IOException("vint past end");
			}
			final byte b = bytes[pos++];
			// 取低 7 位累加到 value
			value |= (b & 0x7F) << shift;
			// 最高位为 0 表示最后一个字节
			if ((b & 0x80) == 0) {
				nextVIntPos = pos;
				return value;
			}
			shift += 7;
			// vint 最多 5 字节（35 bit），超过则报错
			if (shift > 35) {
				throw new IOException("vint too long");
			}
		}
	}

	/**
	 * 构建期使用的可变 int 列表，支持自动扩容。
	 * finalize 时转为排序去重数组。
	 */
	static final class IntList {
		/** 内部存储数组 */
		private int[] data = new int[4];
		/** 当前元素数量 */
		private int size;

		/**
		 * 追加一个元素，容量不足时自动扩容
		 *
		 * @param v 要追加的值
		 */
		void add(int v) {
			if (size == data.length) {
				data = ArrayUtil.grow(data);
			}
			data[size++] = v;
		}

		/**
		 * 将内部数据转为排序且去重的数组副本
		 *
		 * @return 排序去重后的 int 数组
		 */
		int[] toSortedArray() {
			// 拷贝有效部分
			final int[] copy = Arrays.copyOf(data, size);
			// 排序
			Arrays.sort(copy);
			// 原地去重
			int w = 1;
			for (int i = 1; i < copy.length; i++) {
				if (copy[i] != copy[i - 1]) {
					copy[w++] = copy[i];
				}
			}
			return Arrays.copyOf(copy, w);
		}
	}

	/** 空 int 数组常量 */
	private static final int[] EMPTY_INT = new int[0];
	/** 空 byte 数组常量 */
	private static final byte[] EMPTY_BYTES = new byte[0];
}