package cn.lxdb.plugins.muqingyu.fptoken.api;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.index.TermsEnum$SeekStatus;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.offheap.OffheapPoolName;
import org.slf4j.Logger;

import cn.lucene.lxdb.params.LxdbLogerEncrypt;
import cn.lxdb.plugins.muqingyu.fptoken.config.FpTokenBlockLevelPolicy;
import cn.lxdb.plugins.muqingyu.fptoken.config.Lucene80FPSearchConfig;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.block.FpGroupHotNgramBitIndex;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpBlockInfo;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpLog;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpSearchStat;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpTokenTermLayout;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.Utils;

/**
 * 查询侧：按 ngram {@link BytesRef} 切片在组内 hot/common 精确 bucketIndex 倒排中定位 order 列表，再
 * {@link Terms#iterator_fp()} seek 倒排并合并 doc，多切片 AND、同切片多组 OR。
 */
public class FpSearch {
	/** 日志记录器，使用加密/脱敏日志工具获取 */
	public static final Logger LOG = LxdbLogerEncrypt.getLogger("mqy.fptoken");

	/** 搜索统计信息对象，记录命中数、耗时等指标 */
	public FpSearchStat stat;
	/** 追踪 ID，用于关联同一查询的日志 */
	public final String traceId;

	/**
	 * 构造函数（无 traceId）
	 *
	 * @param stat 搜索统计对象
	 */
	public FpSearch(FpSearchStat stat) {
		this(stat, "");
	}

	/**
	 * 构造函数
	 *
	 * @param stat    搜索统计对象
	 * @param traceId 追踪 ID；为空且开启调试日志时自动生成 UUID
	 */
	public FpSearch(FpSearchStat stat, String traceId) {
		this.stat = stat;
		if (traceId != null && !traceId.isEmpty()) {
			this.traceId = traceId;
		} else if (Lucene80FPSearchConfig.LOG_FP_SEARCH || Lucene80FPSearchConfig.PRINT_DEBUG) {
			// 调试模式下自动生成 traceId 以便日志关联
			this.traceId = java.util.UUID.randomUUID().toString();
		} else {
			this.traceId = "";
		}
	}


	/**
	 * 主搜索入口：对多个 slice 执行 AND 语义搜索。
	 * <p>
	 * 流程：
	 * <ol>
	 *   <li>将 slices 转为 selective bucketKeys</li>
	 *   <li>遍历 fpblock_list 中每个 group，按需加载位图索引</li>
	 *   <li>对每个 slice 在组内查找 hot/common order 并 seek 倒排合并 doc</li>
	 *   <li>补扫稀疏列（NOGROUP）的 term</li>
	 *   <li>对所有 slice 的结果做 AND 合并返回</li>
	 * </ol>
	 *
	 * @param fpblock_list 段内 group_id → 位图区元数据映射
	 * @param terms        Lucene Terms 实例，用于访问倒排
	 * @param maxDoc       最大文档号
	 * @param columnName   查询列名
	 * @param slices       查询串滑窗切片（长度须在 NGRAM_MIN..NGRAM_MAX）
	 * @return 同时命中全部 slice 的 doc 集合（AND）；无切片或任一切片无命中则返回空集
	 * @throws IOException IO 异常
	 */
	public FixedBitSet search( TreeMap<Integer, FpBlockInfo> fpblock_list, Terms terms, int maxDoc, BytesRef columnName,
			BytesRef[] slices) throws IOException {

		// 累加文档计数统计
		stat.doccount+=maxDoc;
		// 初始化结果位图（堆外内存）
		final FixedBitSet rtn = new FixedBitSet(OffheapPoolName.fptokenbitset, maxDoc);
		if (slices == null || slices.length == 0) {
			return rtn;
		}

		// 每个 slice 对应一个收集位图，最终做 AND 合并
		final FixedBitSet[] collect = new FixedBitSet[slices.length];
		int maxGroupId = -1;
		int columnMatchedGroups = 0;
		int columnSkippedGroups = 0;
		String indexColumnSample = null;
		// 将所有 slice 转换为去重后的 packed bucketKey 数组，用于 selective 读盘
		final long[] bucketKeys = FpGroupHotNgramBitIndex.selectiveKeysForSlices(slices);

		// 遍历所有 group 块
		for (Entry<Integer, FpBlockInfo> e : fpblock_list.entrySet()) {
			final FpBlockInfo blkinfo = e.getValue();
			// 记录首个匹配的列名样本，用于列不匹配时的诊断日志
			if (indexColumnSample == null && blkinfo.fieldInfo != null) {
				indexColumnSample = Utils.BytesReftoString(blkinfo.fieldInfo);
			}
			// 列名不匹配则跳过该 group
			if (!fieldInfoMatchesColumn(blkinfo, columnName)) {
				columnSkippedGroups++;
				continue;
			}
			columnMatchedGroups++;
			// 按层级和 slice 数累加块计数
			stat.blkCount[blkinfo.targetLevel]+=slices.length;
			final int groupId = e.getKey().intValue();
			if (groupId > maxGroupId) {
				maxGroupId = groupId;
			}
			
			// 通过 skip index 按需加载命中的 hot 和 common 的 term_order_num
			final FpGroupHotNgramBitIndex bitsetIndex = loadBitIndex(terms, groupId, bucketKeys);
			if (bitsetIndex == null) {
				// 位图索引为空，记录调试日志并跳过
				if (Lucene80FPSearchConfig.LOG_FP_SEARCH || Lucene80FPSearchConfig.PRINT_DEBUG) {
					final StringBuilder sb = FpLog.kv();
					FpLog.append(sb, "event", "skipGroup");
					FpLog.append(sb, "reason", "fpBitsNull");
					FpLog.append(sb, "groupId", groupId);
					FpLog.append(sb, "level", "L" + blkinfo.targetLevel);
					FpLog.debugTrace(LOG, traceId, sb);
				}
				continue;
			}

			try {
				// 对每个 slice 在当前 group 内搜索，结果累积到 collect[i]
				for (int i = 0; i < slices.length; i++) {
					searchSliceInGroup(bitsetIndex, columnName, slices[i], blkinfo, groupId, terms, maxDoc, collect, i);
				}
			} finally {
				bitsetIndex.releasePooledMaps();
			}
		}

		// 所有 group 均列不匹配时输出诊断日志
		if (Lucene80FPSearchConfig.LOG_FP_SEARCH && columnMatchedGroups == 0 && fpblock_list.size() > 0) {
			final StringBuilder sb = FpLog.kv();
			FpLog.append(sb, "event", "columnMismatch");
			FpLog.append(sb, "queryColumn", Utils.BytesReftoString(columnName));
			FpLog.append(sb, "indexColumnSample", indexColumnSample);
			FpLog.append(sb, "fpGroups", fpblock_list.size());
			FpLog.append(sb, "skippedGroups", columnSkippedGroups);
			FpLog.searchTrace(LOG, traceId, sb);
		}

		// 补扫稀疏列（无位图索引的小组），暴力处理 NOGROUP term
		searchSparseNoBitIndexTerms(terms, columnName, maxDoc, slices, collect, maxGroupId);

		// 对所有 slice 的收集位图做 AND 合并
		boolean merged = false;
		for (int i = 0; i < slices.length; i++) {
			if (collect[i] == null) {
				// 任一 slice 无命中，直接返回空集
				rtn.clear(0, maxDoc);
				return rtn;
			}
			if (!merged) {
				// 第一个 slice 用 OR 初始化结果集
				rtn.or(collect[i]);
				merged = true;
			} else {
				// 后续 slice 用 AND 交集
				rtn.and(collect[i]);
			}
		}
		// 搜索结束日志
		if (Lucene80FPSearchConfig.LOG_FP_SEARCH || Lucene80FPSearchConfig.PRINT_DEBUG) {
			final StringBuilder sb = FpLog.kv();
			FpLog.append(sb, "event", "searchEnd");
			FpLog.append(sb, "hitDocs", rtn.cardinality());
			FpLog.append(sb, "columnMatchedGroups", columnMatchedGroups);
			FpLog.append(sb, "stat", stat);
			FpLog.debugTrace(LOG, traceId, sb);
		}
		return rtn;
	}

	/**
	 * 加载组位图索引：{@code fpBits} 在返回前已按 {@code bucketKeys} 把 hot/common 各 bucket 的 orderList 读入内存；
	 * 后续 {@link FpGroupHotNgramBitIndex#lookupHotOrders} / {@link FpGroupHotNgramBitIndex#lookupCommonOrders} 纯查表。
	 * 不做全量 {@code fpBits(..., null, null)} 回退。
	 *
	 * @param terms      Lucene Terms 实例
	 * @param groupId    组 ID
	 * @param bucketKeys selective 读取的 packed bucketKey 数组
	 * @return 组位图索引实例，null 表示该组无索引
	 * @throws IOException IO 异常
	 */
	private FpGroupHotNgramBitIndex loadBitIndex(Terms terms, int groupId, long[] bucketKeys) throws IOException {
		return terms.fpBits(Lucene80FPSearchConfig.DEFAULT_INDEX_ID, groupId, bucketKeys, bucketKeys);
	}

	/**
	 * 在单个 group 内搜索单个 slice：先查 hot tier，未命中再查 common tier。
	 * 结果累积到 collect[sliceIndex] 位图中。
	 *
	 * @param bitIndex     组位图索引
	 * @param columnName   查询列名
	 * @param anchorSlice  当前搜索的 slice
	 * @param blkinfo      块信息
	 * @param groupid      组 ID
	 * @param terms        Lucene Terms 实例
	 * @param maxDoc       最大文档号
	 * @param collect      各 slice 的收集位图数组
	 * @param sliceIndex   当前 slice 在数组中的下标
	 * @throws IOException IO 异常
	 */
	private void searchSliceInGroup(FpGroupHotNgramBitIndex bitIndex, BytesRef columnName, BytesRef anchorSlice,
			FpBlockInfo blkinfo, int groupid, Terms terms, int maxDoc, FixedBitSet[] collect, int sliceIndex)
			throws IOException {
		// 确保当前 slice 的收集位图已初始化
		final FixedBitSet acc = ensureCollect(collect, sliceIndex, maxDoc);
		// 从位图索引中查找 hot 和 common 的 order 列表
		final int[] hotOrders = bitIndex.lookupHotOrders(anchorSlice);
		final int[] commonOrders = bitIndex.lookupCommonOrders(anchorSlice);
		
		// 优先搜索 hot tier
		final boolean hotHit = searchOrdersHot(bitIndex, columnName, anchorSlice, blkinfo, groupid, terms, maxDoc, acc,hotOrders);
		if (Lucene80FPSearchConfig.PRINT_DEBUG) {
			final StringBuilder sb = FpLog.kv();
			FpLog.append(sb, "event", "searchOrdersHot");
			FpLog.append(sb, "hit", hotHit);
			FpLog.append(sb, "slice", Utils.BytesReftoString(anchorSlice));
			FpLog.append(sb, "groupId", groupid);
			FpLog.debugTrace(LOG, traceId, sb);
		}
		// hot 未命中时回退到 common tier
		if (!hotHit) {
			final boolean commonHit = searchOrdersCommon(bitIndex, columnName, anchorSlice, blkinfo, groupid, terms,maxDoc, acc, commonOrders);
			if (Lucene80FPSearchConfig.PRINT_DEBUG) {
				final StringBuilder sb = FpLog.kv();
				FpLog.append(sb, "event", "searchOrdersCommon");
				FpLog.append(sb, "hit", commonHit);
				FpLog.append(sb, "slice", Utils.BytesReftoString(anchorSlice));
				FpLog.append(sb, "groupId", groupid);
				FpLog.debugTrace(LOG, traceId, sb);
			}
		}
	}

	/**
	 * 懒初始化收集位图：若 collect[sliceIndex] 为 null 则创建新的堆外 FixedBitSet。
	 *
	 * @param collect    收集位图数组
	 * @param sliceIndex slice 下标
	 * @param maxDoc     最大文档号
	 * @return 对应位置的 FixedBitSet
	 */
	private static FixedBitSet ensureCollect(FixedBitSet[] collect, int sliceIndex, int maxDoc) {
		if (collect[sliceIndex] == null) {
			collect[sliceIndex] = new FixedBitSet(OffheapPoolName.fptokenbitset, maxDoc);
		}
		return collect[sliceIndex];
	}

	/**
	 * 搜索 hot tier 的 order 列表，将命中文档 OR 到 collect 位图。
	 *
	 * @return true 表示至少有一个 order 命中
	 */
	private boolean searchOrdersHot(FpGroupHotNgramBitIndex bitIndex, BytesRef columnName, BytesRef anchorSlice,
			FpBlockInfo blkinfo, int groupid, Terms terms, int maxDoc, FixedBitSet collect, int[] orders)
			throws IOException {
		return seekOrderList(orders, true, columnName, anchorSlice, blkinfo, groupid, terms, maxDoc, collect);
	}

	/**
	 * 搜索 common tier 的 order 列表，将命中文档 OR 到 collect 位图。
	 *
	 * @return true 表示至少有一个 order 命中
	 */
	private boolean searchOrdersCommon(FpGroupHotNgramBitIndex bitIndex, BytesRef columnName, BytesRef anchorSlice,
			FpBlockInfo blkinfo, int groupid, Terms terms, int maxDoc, FixedBitSet collect, int[] orders)
			throws IOException {
		return seekOrderList(orders, false, columnName, anchorSlice, blkinfo, groupid, terms, maxDoc, collect);
	}

	/**
	 * 遍历 order 列表，逐个 seek 倒排 term 并将 postings OR 到 collect 位图。
	 * <p>
	 * hot 模式下首个命中会记录 maxHotPayloadLen，后续 order 若 payload 超过该长度则提前终止（BREAK_PAYLOAD_LEN），
	 * 因为更长的 payload 不可能包含当前 slice 的精确匹配。
	 *
	 * @param orders   term order 数组
	 * @param hotMark  true=hot tier, false=common tier
	 * @return true 表示至少有一个 order 命中
	 */
	private boolean seekOrderList(int[] orders, boolean hotMark, BytesRef columnName, BytesRef anchorSlice,
			FpBlockInfo blkinfo, int groupid, Terms terms, int maxDoc, FixedBitSet collect) throws IOException {
		if (orders == null || orders.length == 0) {
			return false;
		}
		// 复用 PostingsEnum 避免重复分配
		final AtomicReference<PostingsEnum> reusePosting = new AtomicReference<PostingsEnum>(null);
		final TermsEnum termsEnum = terms.iterator();
		// 复用 BytesRef 缓冲区
		final BytesRef reuse = new BytesRef(new byte[512]);
		boolean anyHit = false;
		// hot 模式下记录首个命中的最大 payload 长度，用于后续截断
		final AtomicInteger maxHotPayloadLen = new AtomicInteger(0);
		boolean payloadLenCapSet = false;
		boolean first = false;
		for (int termIndex : orders) {
			// 累加位图命中统计
			if (hotMark) {
				stat.bitHitHot[blkinfo.targetLevel]++;
			} else {
				stat.bitHitCommon[blkinfo.targetLevel]++;
			}
			// 首个 order 累加块级命中统计
			if (!first) {
				if (hotMark) {
					stat.blkHitHot[blkinfo.targetLevel]++;
				} else {
					stat.blkHitCommon[blkinfo.targetLevel]++;
				}
				first = true;
			}
			// hot 模式且尚未设置 payload 长度上限时，要求精确匹配以获取 maxHotPayloadLen
			if (hotMark && !payloadLenCapSet) {
				final int status = seekTermAndOrDocs(reusePosting, maxHotPayloadLen, true, termsEnum, reuse,
						Lucene80FPSearchConfig.DEFAULT_INDEX_ID, groupid, (byte) blkinfo.targetLevel, hotMark,
						termIndex, columnName, anchorSlice, maxDoc, collect);
				if (status == SEEK_OK) {
					anyHit = true;
					payloadLenCapSet = true;
				}
				continue;
			}
			// 常规 seek：hot 模式传入 maxHotPayloadLen 用于截断判断
			final int status = seekTermAndOrDocs(reusePosting, hotMark ? maxHotPayloadLen : null, false, termsEnum,
					reuse, Lucene80FPSearchConfig.DEFAULT_INDEX_ID, groupid, (byte) blkinfo.targetLevel, hotMark,
					termIndex, columnName, anchorSlice, maxDoc, collect);
			if (status == SEEK_OK) {
				anyHit = true;
			}
			// payload 超长截断，停止遍历后续 order
			if (status == SEEK_BREAK_PAYLOAD_LEN) {
				break;
			}
		}
		return anyHit;
	}

	/**
	 * 检查块信息的字段名是否与查询列名匹配。
	 *
	 * @param blkinfo    块信息
	 * @param columnName 查询列名
	 * @return true 表示匹配
	 */
	private static boolean fieldInfoMatchesColumn(FpBlockInfo blkinfo, BytesRef columnName) {
		if (blkinfo == null || blkinfo.fieldInfo == null) {
			return false;
		}
		return blkinfo.fieldInfo.equals(columnName);
	}

	/**
	 * 稀疏列补扫（写段时 common 词数 ≤ NO_INDEX_THRESHOLD、无 ngram 位图）：
	 * 仅扫 level=NOGROUP 的 term；有 {@link FpBlockInfo} 的 group 已在主循环走位图路径，
	 * 此处从 {@code maxGroupId+1} seek 起补扫，避免从 0 暴力扫全字典。
	 *
	 * @param terms      Lucene Terms 实例
	 * @param columnName 查询列名
	 * @param maxDoc     最大文档号
	 * @param slices     查询切片数组
	 * @param collect    各 slice 的收集位图数组
	 * @param maxGroupId 已索引的最大 group ID
	 * @throws IOException IO 异常
	 */
	private void searchSparseNoBitIndexTerms(Terms terms, BytesRef columnName, int maxDoc, BytesRef[] slices,
			FixedBitSet[] collect, int maxGroupId) throws IOException {
		final short indexId = Lucene80FPSearchConfig.DEFAULT_INDEX_ID;
		final AtomicReference<PostingsEnum> reusePosting = new AtomicReference<PostingsEnum>(null);
		final TermsEnum termsEnum = terms.iterator();
		final BytesRef reuse = new BytesRef(new byte[512]);

		// 构造 NOGROUP 级别的搜索前缀，从 maxGroupId+1 开始 seek
		FpTokenTermLayout.make_fp_search_prefix(reuse, columnName, indexId, maxGroupId + 1,
				(byte) FpTokenBlockLevelPolicy.BLOCK_LEVEL_NOGROUP, false, 0);
		if (termsEnum.seekCeil(reuse) == TermsEnum$SeekStatus.END) {
			stat.termMiss0++;
			if (Lucene80FPSearchConfig.PRINT_DEBUG) {
				final StringBuilder sb = FpLog.kv();
				FpLog.append(sb, "event", "sparseScanMiss");
				FpLog.append(sb, "reason", "seekCeilEnd");
				FpLog.append(sb, "column", Utils.BytesReftoString(columnName));
				FpLog.debugTrace(LOG, traceId, sb);
			}
			return;
		}

		if (Lucene80FPSearchConfig.PRINT_DEBUG) {
			final StringBuilder sb = FpLog.kv();
			FpLog.append(sb, "event", "sparseScanBegin");
			FpLog.append(sb, "column", Utils.BytesReftoString(columnName));
			FpLog.append(sb, "maxGroupId", maxGroupId);
			FpLog.debugTrace(LOG, traceId, sb);
		}

		// 顺序扫描 NOGROUP term
		for (BytesRef found = termsEnum.term(); found != null; found = termsEnum.next()) {
			// 列名不匹配说明已超出当前列范围，停止扫描
			if (!FpTokenTermLayout.columnNameEquals(found, columnName)) {
				
				break;
			}
			
			// 仅处理 NOGROUP 级别的 term
			final int level = FpTokenTermLayout.readLevel(found);
			if (level != FpTokenBlockLevelPolicy.BLOCK_LEVEL_NOGROUP) {
				continue;
			}
			
			

			// 提取 payload（去除列名和头部字节）
			final BytesRef payload = FpTokenTermLayout.removeColumnAndHeaderBytes(found);
			// 对每个 slice 检查 payload 是否包含该 slice
			for (int i = 0; i < slices.length; i++) {
				if (!payloadContainsSlice(payload, slices[i])) {
					stat.termMiss0++;
					continue;
				}
				stat.termHit0++;
				final FixedBitSet acc = ensureCollect(collect, i, maxDoc);
				// 将 postings OR 到对应 slice 的收集位图
				orPostingsInto(reusePosting, termsEnum, maxDoc, acc,false);
			}
		}
	}

	/**
	 * 检查 payload 是否包含 slice 连续子串（indexOf 语义）。
	 *
	 * @param payload 载荷字节
	 * @param slice   查询切片
	 * @return true 表示 payload 中包含 slice
	 */
	private static boolean payloadContainsSlice(BytesRef payload, BytesRef slice) {
		return payloadMatchesSlice(false, payload, slice);
	}

	/** seek 成功 */
	private static final int SEEK_OK = 0;
	/** seek 因 payload 超长而截断 */
	private static final int SEEK_BREAK_PAYLOAD_LEN = 1;
	/** seek 未命中 */
	private static final int SEEK_MISS = 2;

	/**
	 * 将 seek 状态码转为可读字符串（调试用）
	 *
	 * @param status 状态码
	 * @return 状态描述字符串
	 */
	private static String seekStatusLabel(int status) {
		if (status == SEEK_OK) {
			return "OK";
		}
		if (status == SEEK_BREAK_PAYLOAD_LEN) {
			return "BREAK_PAYLOAD_LEN";
		}
		if (status == SEEK_MISS) {
			return "MISS";
		}
		return String.valueOf(status);
	}

	/**
	 * 根据 termIndex 构造 term key 并 seek 倒排，校验头部/payload 后将 postings OR 到 collect 位图。
	 * <p>
	 * hot 模式下可选地检查 payload 长度上限：若当前 payload 超过 maxHotPayloadLen 则返回 BREAK_PAYLOAD_LEN，
	 * 调用方可据此提前终止后续 order 的遍历。
	 *
	 * @param reusePosting            复用的 PostingsEnum 引用
	 * @param maxHotPayloadLenOrNull  hot 模式下的最大 payload 长度限制，null 表示不限制
	 * @param requireExactPayloadMatch true 时要求 payload 与 slice 精确相等（用于获取 maxHotPayloadLen）
	 * @param termsEnum               TermsEnum 迭代器
	 * @param reuse                   复用的 BytesRef 缓冲区
	 * @param indexId                 索引 ID
	 * @param groupid                 组 ID
	 * @param groupLevel              组层级
	 * @param hotMark                 true=hot, false=common
	 * @param termIndex               term 在组内的序号
	 * @param columnName              查询列名
	 * @param slice                   查询切片
	 * @param maxDoc                  最大文档号
	 * @param collect                 收集位图
	 * @return SEEK_OK / SEEK_BREAK_PAYLOAD_LEN / SEEK_MISS
	 * @throws IOException IO 异常
	 */
	private  int seekTermAndOrDocs(AtomicReference<PostingsEnum> reusePosting,
			AtomicInteger maxHotPayloadLenOrNull, boolean requireExactPayloadMatch, TermsEnum termsEnum, BytesRef reuse,
			short indexId, int groupid, byte groupLevel, boolean hotMark, int termIndex, BytesRef columnName,
			BytesRef slice, int maxDoc, FixedBitSet collect) throws IOException {
		// 构造 term 搜索前缀
		FpTokenTermLayout.make_fp_search_prefix(reuse, columnName, indexId, groupid, groupLevel, hotMark, termIndex);

		
		// seek 到目标 term
		if (termsEnum.seekCeil(reuse) == TermsEnum$SeekStatus.END) {
			// 字典末尾，未找到
			if (hotMark) {
				stat.termMissHot1[groupLevel]++;
			} else {
				stat.termMissCommon1[groupLevel]++;
			}
			if (Lucene80FPSearchConfig.PRINT_DEBUG) {
				final StringBuilder sb = FpLog.kv();
				FpLog.append(sb, "event", "seekResult");
				FpLog.append(sb, "status", seekStatusLabel(SEEK_MISS));
				FpLog.append(sb, "stage", "seekCeilEnd");
				FpLog.append(sb, "groupId", groupid);
				FpLog.append(sb, "termIndex", termIndex);
				FpLog.append(sb, "hot", hotMark);
				FpLog.debugTrace(LOG, traceId, sb);
			}
			return SEEK_MISS;
		}
		final BytesRef found = termsEnum.term();
		// 检查是否为删除标记 term
		boolean isDelTerm = FpTokenTermLayout.readIsDelTerm(found);
	
		// 校验 term 头部字段是否完全匹配
		if (!termHeaderMatches(found, columnName, groupid, groupLevel, hotMark, termIndex)) {
			if (hotMark) {
				stat.termMissHot2[groupLevel]++;
			} else {
				stat.termMissCommon2[groupLevel]++;
			}
			if (Lucene80FPSearchConfig.PRINT_DEBUG) {
				final StringBuilder sb = FpLog.kv();
				FpLog.append(sb, "event", "seekResult");
				FpLog.append(sb, "status", seekStatusLabel(SEEK_MISS));
				FpLog.append(sb, "stage", "headerMismatch");
				FpLog.append(sb, "groupId", groupid);
				FpLog.append(sb, "termIndex", termIndex);
				FpLog.debugTrace(LOG, traceId, sb);
			}
			return SEEK_MISS;
		}
		// 提取 payload 并校验是否匹配 slice
		final BytesRef payload = FpTokenTermLayout.removeColumnAndHeaderBytes(found);
		if (!payloadMatchesSlice(requireExactPayloadMatch, payload, slice)) {
			if (hotMark) {
				stat.termMissHot3[groupLevel]++;
			} else {
				stat.termMissCommon3[groupLevel]++;
			}
			if (Lucene80FPSearchConfig.PRINT_DEBUG) {
				final StringBuilder sb = FpLog.kv();
				FpLog.append(sb, "event", "seekResult");
				FpLog.append(sb, "status", seekStatusLabel(SEEK_MISS));
				FpLog.append(sb, "stage", "payloadMismatch");
				FpLog.append(sb, "groupId", groupid);
				FpLog.append(sb, "termIndex", termIndex);
				FpLog.append(sb, "payload", Utils.BytesReftoString(payload));
				FpLog.append(sb, "slice", Utils.BytesReftoString(slice));
				FpLog.debugTrace(LOG, traceId, sb);
			}
			return SEEK_MISS;
		}

		// hot 模式下检查 payload 长度上限
		if (hotMark && maxHotPayloadLenOrNull != null) {
			if (requireExactPayloadMatch) {
				// 首次精确匹配时记录最大 payload 长度
				maxHotPayloadLenOrNull.set(FpTokenTermLayout.maxHotPayloadLenFromHeader(found, payload.length));
			}
			// 当前 payload 超过上限，返回截断信号
			if (maxHotPayloadLenOrNull.get() < payload.length) {
				if (Lucene80FPSearchConfig.PRINT_DEBUG) {
					final StringBuilder sb = FpLog.kv();
					FpLog.append(sb, "event", "seekResult");
					FpLog.append(sb, "status", seekStatusLabel(SEEK_BREAK_PAYLOAD_LEN));
					FpLog.append(sb, "maxHotPayloadLen", maxHotPayloadLenOrNull.get());
					FpLog.append(sb, "payloadLen", payload.length);
					FpLog.debugTrace(LOG, traceId, sb);
				}
				return SEEK_BREAK_PAYLOAD_LEN;
			}
		}

		// 非删除 term：将 postings OR 到收集位图
		if (!isDelTerm) {
			orPostingsInto(reusePosting, termsEnum, maxDoc, collect,hotMark);
		}
		
		// 累加 term 级命中统计
		if(hotMark)
		{
			stat.termHitHot[groupLevel]++;
		}else {
			stat.termHitCommon[groupLevel]++;
		}
		return SEEK_OK;
	}

	/**
	 * 校验找到的 term 头部是否与期望的各字段完全一致。
	 *
	 * @param found      找到的 term BytesRef
	 * @param columnName 期望的列名
	 * @param groupid    期望的组 ID
	 * @param groupLevel 期望的层级
	 * @param hotMark    期望的 hot/common 标记
	 * @param termIndex  期望的 term 序号
	 * @return true 表示所有头部字段均匹配
	 */
	private static boolean termHeaderMatches(BytesRef found, BytesRef columnName, int groupid, byte groupLevel,
			boolean hotMark, int termIndex) {
		if (found.length <= 0) {
			return false;
		}
		return FpTokenTermLayout.columnNameEquals(found, columnName)
				&& FpTokenTermLayout.read_group_id(found) == groupid
				&& FpTokenTermLayout.readLevel(found) == (groupLevel & 0xFF)
				&& FpTokenTermLayout.isHotTerm(found) == hotMark
				&& FpTokenTermLayout.readTermIndex(found) == termIndex;
	}

	/**
	 * 检查 payload 是否匹配 slice。
	 * <ul>
	 *   <li>{@code requireExactPayloadMatch=true} 或长度相等时：精确比较</li>
	 *   <li>否则：检查 payload 是否包含 slice 作为连续子串</li>
	 * </ul>
	 *
	 * @param requireExactPayloadMatch 是否要求精确匹配
	 * @param payload                  载荷字节
	 * @param slice                    查询切片
	 * @return true 表示匹配
	 */
	private static boolean payloadMatchesSlice(boolean requireExactPayloadMatch, BytesRef payload, BytesRef slice) {
		final int plen = payload.length;
		final int slen = slice.length;
		if (plen < slen) {
			return false;
		}
		if (requireExactPayloadMatch || plen == slen) {
			return payload.equals(slice);
		}

		// 滑动窗口子串匹配
		final byte[] pb = payload.bytes;
		final int po = payload.offset;
		final byte[] sb = slice.bytes;
		final int so = slice.offset;

		for (int i = 0; i <= plen - slen; i++) {
			boolean match = true;
			for (int j = 0; j < slen; j++) {
				if (pb[po + i + j] != sb[so + j]) {
					match = false;
					break;
				}
			}
			if (match) {
				return true;
			}
		}
		return false;
	}

	/**
	 * 读取当前 term 的 postings 并 OR 到收集位图中。
	 * 复用 PostingsEnum 以减少 GC 压力。
	 *
	 * @param reuse     复用的 PostingsEnum 引用
	 * @param termsEnum TermsEnum 迭代器（已定位到目标 term）
	 * @param maxDoc    最大文档号
	 * @param collect   收集位图
	 * @param hotMark   true=hot tier 命中, false=common tier 命中
	 * @throws IOException IO 异常
	 */
	private  void orPostingsInto(AtomicReference<PostingsEnum> reuse, TermsEnum termsEnum, int maxDoc,
			FixedBitSet collect,boolean hotMark) throws IOException {
		// 获取 postings 枚举器，复用已有实例
		final PostingsEnum pe = termsEnum.postings(reuse.get(), PostingsEnum.NONE);
		if (pe == null) {
			return;
		}

		reuse.set(pe);
		int doc;
		// 遍历所有 docID 并设置到位图中
		while ((doc = pe.nextDoc()) != PostingsEnum.NO_MORE_DOCS) {
			if (doc >= 0 && doc < maxDoc) {
				collect.set(doc);
				// 累加 posting 级命中统计
				if(hotMark)
				{
					stat.hothit++;
				}else {
					stat.commonhit++;
				}
			}
		}
	}

}