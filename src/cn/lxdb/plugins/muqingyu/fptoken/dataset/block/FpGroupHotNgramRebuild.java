package cn.lxdb.plugins.muqingyu.fptoken.dataset.block;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.lucene.util.BytesRef;
import org.slf4j.Logger;

import cn.lucene.lxdb.params.LxdbLogerEncrypt;
import cn.lucene.proguard.keep.lxdb.common.CLMillisecondClock;
import cn.lxdb.plugins.muqingyu.fptoken.api.FpTokenBlockOrchestrator;
import cn.lxdb.plugins.muqingyu.fptoken.config.FpTokenBlockLevelPolicy;
import cn.lxdb.plugins.muqingyu.fptoken.config.Lucene80FPSearchConfig;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FPDocList;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpStatNgram;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpTermKey;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpTermKeyHash;
import cn.lxdb.plugins.muqingyu.fptoken.ngram.Counter;
import cn.lxdb.plugins.muqingyu.fptoken.pool.FpHashMapPoolHub;
import cn.lxdb.plugins.muqingyu.fptoken.pool.FpHashMapPoolIds;
import cn.lxdb.plugins.muqingyu.fptoken.pool.FpHashMapPoolLease;

/**
 * 从 {@link FpGroupDataRebuild#commonTermMapInternal()} 按 byte n-gram 挖掘热词，合并 doc，并计算
 * {@link FpGroupDataRebuild#hotTermDownTierBudget}（锚点向下可遍历深度）。
 *
 * <h2>实现原理</h2>
 * <p>
 * 热词表是扁平 {@link TreeMap}，不是一棵树；层级关系由<strong>字节前缀 + 长度档</strong>表达。
 * 以锚点热词 {@code ab}（字节长 2）为例：
 * </p>
 * <ul>
 *   <li><b>长度档</b>：{@code i=2} 为锚点自身；{@code i=3,4,5…} 依次对应向下扩展的 {@code ab*}、{@code ab**}、{@code ab***}…
 *       （示意：固定总字节长度的一层热词/子串，不把更长档算进更短档的个数里单独去重，但见下「累加预算」）。</li>
 *   <li><b>{@code hotTermDownTierBudget}（maxDown）</b>：从锚点字节长起，按档累加已登记子串个数 {@code cumulativeTierCount}；
 *       若累加仍 ≤ {@link Lucene80FPSearchConfig#HOT_TIER_TERM_COUNT_THRESHOLD}，则允许再向下一档遍历并 {@code downTierBudget++}；
 *       超过阈值则停止——表示查询侧再向下拼子档的代价过大。该值写入热词 term 布局，供检索按深度截断。</li>
 *   <li><b>写段 merge（空间换时间）</b>：同一 common 载荷内，对命中的热词 ngram 从长到短合并 doc。
 *       较长热词 merge 后，对其真前缀调用 {@link #markParentPrefixesSkippedInCommonTerm}：
 *       若 {@code extensionBytesFromParent &lt; parentDownTierBudget}，则在本 common 词内标记父前缀已处理，
 *       避免短 ngram 再次 merge 同 doc；若更深则<strong>不</strong>标记，父热词 posting 仍可保留该 doc（深档查询不拼回时不能丢 posting）。</li>
 *   <li><b>不做写段后 doclist 剔除</b>：不在 merge 之后从父热词 posting 物理删除子档 doc；控制粒度是「单 common 内是否重复 merge」+ 查询按 level 拼回。</li>
 * </ul>
 *
 * <h2>流程</h2>
 * <ol>
 *   <li>{@link #countNgramOccurrencesInCommon} — 统计 common 滑窗 ngram 出现次数；</li>
 *   <li>{@link #buildHotTermsAndAnchorTierIndex} — 频率达阈值者进入热词表，并建立锚点分档索引；</li>
 *   <li>{@link #computeHotDownTierBudgets} — 写入 {@code hotTermDownTierBudget}；</li>
 *   <li>{@link #mergeCommonDocsIntoHotTerms} — common doc 合并到热词，并按 maxDown 标记父前缀 skip。</li>
 * </ol>
 */
public final class FpGroupHotNgramRebuild {
    /** 日志记录器，使用加密/脱敏日志工具获取 */
    public static final Logger LOG = LxdbLogerEncrypt.getLogger("mqy.fptoken");

	private static final Boolean UNIQUE_NGRAM_MARKER = Boolean.TRUE;

	/**
	 * merge 阶段辅助结构：一次 {@link HashMap#get} 同时拿到 doclist 与序号，
	 * 避免在 merge 循环中对同一个 key 做多次查找。
	 */
	private static final class HotMergeSlot {
		/** 该热词对应的文档列表 */
		final FPDocList docList;
		/** 该热词在 hotMergeTable 中的全局序号，用于 mergedThisCommon 位图索引 */
		final int ordinal;

		HotMergeSlot(FPDocList docList, int ordinal) {
			this.docList = docList;
			this.ordinal = ordinal;
		}
	}

	public static final class CommonTermSortEntry {
		final FpTermKey key;
		final int sortKey;
		final int originalOrdinal;
		final FPDocList sourceDocList;
		CommonTermSortEntry(FpTermKey key,FPDocList sourceDocList, int sortKey, int originalOrdinal) {
			this.key = key;
			this.sourceDocList=sourceDocList;
			this.sortKey = sortKey;
			this.originalOrdinal = originalOrdinal;
		}
	}

	/**
	 * 热词重建主流程入口，结果写入 {@code group} 的热词表与 {@code hotTermDownTierBudget}。
	 *
	 * <p>执行步骤：
	 * <ol>
	 *   <li>统计 common term 中各 ngram 的出现次数</li>
	 *   <li>筛选高频 ngram 作为热词，建立锚点分档索引</li>
	 *   <li>计算每个热词锚点的向下遍历预算（downTierBudget）</li>
	 *   <li>将 common doc 合并到热词 posting，利用预算标记避免重复 merge</li>
	 * </ol>
	 *
	 * @param group      组数据重建对象，包含 common term map 及热词输出容器
	 * @param parentItem 上层编排器（预留参数）
	 * @return ngram 统计信息
	 * @throws IOException IO 异常
	 */
	public static FpStatNgram execute(int targetLevel,FpGroupDataRebuild group, FpTokenBlockOrchestrator parentItem
			) throws IOException {
		
		// 初始化统计对象
		FpStatNgram stat = new FpStatNgram();
		// 获取热词输出容器（term→doclist 映射和 downTierBudget 映射），并清空旧数据
		final HashMap<FpTermKey, FPDocList> hotTermToDocs = group.hotTermMapInternal();
		final HashMap<FpTermKey, Integer> hotTermDownTierBudget = group.hotTermDownTierBudgetInternal();
		hotTermToDocs.clear();
		hotTermDownTierBudget.clear();

		// 获取 common term map 和最大文档号
		final HashMap<FpTermKey, FPDocList> commonTermToDocs = group.commonTermMapInternal();
		final int maxDoc = group.maxDocInternal();
		

		// 预估 HashMap 容量，避免频繁扩容
		// ngram 出现次数计数器
//		final HashMap<FpTermKey, Counter> ngramOccurrenceCount = new HashMap<>(hash_map_init_size);
		
		final FpHashMapPoolLease<FpTermKey, Counter> ngramOccurrenceCountLease = FpHashMapPoolHub.borrow(
				FpHashMapPoolIds.ngramOccurrenceCount, FpTermKey.class, Counter.class,
				FpTokenBlockLevelPolicy.HASH_MAP_DEFAULT_SIZE);
		final HashMap<FpTermKey, Counter> ngramOccurrenceCount = ngramOccurrenceCountLease.map();
		final FpHashMapPoolLease<FpTermKey, AnchorTierIndex> anchorTierIndexByHotTermLease = FpHashMapPoolHub.borrow(
				FpHashMapPoolIds.anchorTierIndexByHotTerm, FpTermKey.class, AnchorTierIndex.class,
				FpTokenBlockLevelPolicy.HASH_MAP_DEFAULT_SIZE);
		final HashMap<FpTermKey, AnchorTierIndex> anchorTierIndexByHotTerm = anchorTierIndexByHotTermLease.map();
//		final FpHashMapPoolLease<FpTermKey, FPDocList> hotTermsPendingDocMergeLease = FpHashMapPoolHub.borrow(
//				FpHashMapPoolIds.hotTermsPendingDocMerge, FpTermKey.class, FPDocList.class,
//				FpTokenBlockLevelPolicy.HASH_MAP_DEFAULT_SIZE);
		final HashMap<FpTermKey, FPDocList> hotTermsPendingDocMerge = hotTermToDocs;
		final FpHashMapPoolLease<FpTermKey, HotMergeSlot> hotMergeTableLease = FpHashMapPoolHub.borrow(
				FpHashMapPoolIds.hotMergeTable, FpTermKey.class, HotMergeSlot.class,
				FpTokenBlockLevelPolicy.HASH_MAP_DEFAULT_SIZE);
		final HashMap<FpTermKey, HotMergeSlot> hotMergeTable = hotMergeTableLease.map();



		try {
		// === 阶段1：统计 common 中各 ngram 出现次数 ===
		long t0 = CLMillisecondClock.CLOCK.now();
		//----TODO:主要瓶颈一
		countNgramOccurrencesInCommon(stat, commonTermToDocs, ngramOccurrenceCount);
		stat.ms_count = CLMillisecondClock.CLOCK.now() - t0;
		stat.distinct_ngram = ngramOccurrenceCount.size();

		// === 阶段2：构建热词表和锚点分档索引 ===
		t0 = CLMillisecondClock.CLOCK.now();
		buildHotTermsAndAnchorTierIndex(hotTermsPendingDocMerge,stat,ngramOccurrenceCount, FpTokenBlockLevelPolicy.get_common_to_hot_threshold(targetLevel), maxDoc, anchorTierIndexByHotTerm);
		
		stat.ms_build = CLMillisecondClock.CLOCK.now() - t0;
		stat.hot_pending = hotTermsPendingDocMerge.size();
		
		

		// === 阶段3：计算每个热词锚点的向下遍历预算 ===
		t0 = CLMillisecondClock.CLOCK.now();
		computeHotDownTierBudgets(stat,hotTermDownTierBudget, anchorTierIndexByHotTerm,FpTokenBlockLevelPolicy.get_hot_layer_threshold(targetLevel));
		stat.ms_budget = CLMillisecondClock.CLOCK.now() - t0;
		stat.budget_entries = hotTermDownTierBudget.size();

		// === 阶段4准备：构建 merge 加速结构 ===
		// merge 阶段大量 get：一次性拷贝为 HashMap，规则不变，仅加速查找
//		final HashMap<FpTermKey, Integer> hotTermDownTierBudgetFast = new HashMap<>(hotTermDownTierBudget);
		
		// 将所有热词 doclist 切换为稀疏模式以优化批量 merge 性能
		ensureAllHotDocListsSparse(hotTermsPendingDocMerge);
		// 构建 hotMergeTable：key → (docList, ordinal) 的快速查找表
		buildHotMergeTable(hotMergeTable,hotTermsPendingDocMerge);
		
		// 按锚点长度分区 budget，便于 merge 时按长度快速定位
//		final HashMap<FpTermKey, Integer>[] budgetByLen = partitionBudgetByAnchorLength1(hotTermDownTierBudget);
		// 标记哪些长度存在 budget 条目，用于快速跳过无预算的长度档
//		final boolean[] anchorLenHasBudget = anchorLengthsPresentInBudget(hotTermDownTierBudget);

		// === 阶段4：将 common doc 合并到热词 posting ===
		t0 = CLMillisecondClock.CLOCK.now();
		//----TODO:主要瓶颈二
		final ArrayList<CommonTermSortEntry> commonFlushOrder = mergeCommonDocsIntoHotTerms(stat, commonTermToDocs, hotMergeTable,hotTermDownTierBudget);
		group.setCommonTermFlushOrder(commonFlushOrder);
		stat.ms_merge = CLMillisecondClock.CLOCK.now() - t0;
		stat.hot_doclist_sparse = countSparseHotDocLists(hotTermsPendingDocMerge);
		// 释放锚点分档索引内存
		anchorTierIndexByHotTerm.clear();

		// 将待 merge 的热词写入最终输出容器
//		hotTermToDocs.putAll(hotTermsPendingDocMerge);
		stat.hot_final = hotTermToDocs.size();
		
		
		} finally {
			FpHashMapPoolHub.release(ngramOccurrenceCountLease);
			FpHashMapPoolHub.release(anchorTierIndexByHotTermLease);
//			FpHashMapPoolHub.release(hotTermsPendingDocMergeLease);
			FpHashMapPoolHub.release(hotMergeTableLease);
		}

		return stat;
	}

	/**
	 * 为每个热词锚点计算 {@code hotTermDownTierBudget}：从锚点字节长向下，累加各档已登记子串个数；
	 * 累加超过 {@code hotFreqThreshold} 前每纳入一档则 {@code downTierBudget++}，作为查询/merge 可向下扩展的字节层数预算。
	 *
	 * @param stat                      统计对象
	 * @param hotTermDownTierBudget     输出：热词→预算映射
	 * @param anchorTierIndexByHotTerm  锚点分档索引（按字节长度组织的子串集合）
	 * @param hotFreqThreshold          累加子串数量的阈值上限
	 */
	private static void computeHotDownTierBudgets(FpStatNgram stat, HashMap<FpTermKey, Integer> hotTermDownTierBudget,
			HashMap<FpTermKey, AnchorTierIndex> anchorTierIndexByHotTerm, final long hotFreqThreshold) {
		for (Map.Entry<FpTermKey, AnchorTierIndex> entry : anchorTierIndexByHotTerm.entrySet()) {
			final FpTermKey anchorTerm = entry.getKey();
			final AnchorTierIndex termsByByteLength = entry.getValue();
			// 累计子串计数
			int cumulativeTierTermCount = 0;
			// 初始预算为 1（至少包含锚点自身所在档）
			int downTierBudget = 1;
			final int anchorByteLen = anchorTerm.bytesRef().length;
			// 从锚点长度的下一档开始，逐档累加子串数量
			for (int byteLen = (anchorByteLen+1); byteLen <= Lucene80FPSearchConfig.NGRAM_MAX; byteLen++) {
				cumulativeTierTermCount += termsByByteLength.get(byteLen).size();
				// 超过阈值则停止向下扩展
				if (cumulativeTierTermCount > hotFreqThreshold) {
					break;
				}
				downTierBudget++;
			}
			
			// 记录统计：按锚点长度和预算值分桶计数
			stat.term_level_cnt[anchorTerm.bytesRef().length][downTierBudget]++;
			// 写入预算映射
			hotTermDownTierBudget.put(anchorTerm, downTierBudget);
		}
	}

	/**
	 * 在每个 common 词项载荷内滑窗切 ngram，累加出现次数。
	 * 同一 common term 内的相同 ngram 只计一次（去重），跨 common term 则累加。
	 *
	 * @param stat                  统计对象
	 * @param commonTermToDocs      common term → doclist 映射
	 * @param ngramOccurrenceCount  输出：ngram → 出现次数映射
	 */
	private static void countNgramOccurrencesInCommon(FpStatNgram stat, HashMap<FpTermKey, FPDocList> commonTermToDocs,
			HashMap<FpTermKey, Counter> ngramOccurrenceCount) {
		final int uniqueCap = Lucene80FPSearchConfig.BITSET_WINDOW_SIZE * Lucene80FPSearchConfig.NGRAM_MAX
				* Lucene80FPSearchConfig.NGRAM_MAX;
		final FpHashMapPoolLease<FpTermKey, Boolean> uniqueNgramsLease = FpHashMapPoolHub.borrow(
				FpHashMapPoolIds.commonTermUniqueNgrams, FpTermKey.class, Boolean.class, uniqueCap);
		final HashMap<FpTermKey, Boolean> uniqueNgramsThisCommonTerm = uniqueNgramsLease.map();
		try {
			final BytesRef sliceScratch = new BytesRef();
			for (Map.Entry<FpTermKey, FPDocList> entry : commonTermToDocs.entrySet()) {
				final BytesRef commonPayload = entry.getKey().bytesRef();
				final int payloadLen = commonPayload.length;
				if (payloadLen <= 0) {
					continue;
				}
				stat.term_cnt++;
				uniqueNgramsThisCommonTerm.clear();
				final byte[] bytes = commonPayload.bytes;
				final int base = commonPayload.offset;
				sliceScratch.bytes = bytes;

				for (int start = 0; start < payloadLen; start++) {
					int h = 0;
					final int maxNgramLen = Math.min(Lucene80FPSearchConfig.NGRAM_MAX, payloadLen - start);
					for (int ngramLen = Lucene80FPSearchConfig.NGRAM_MIN; ngramLen <= maxNgramLen; ngramLen++) {
						h = FpTermKeyHash.rollAppend(h, bytes[base + start + ngramLen - 1]);
						sliceScratch.offset = base + start;
						sliceScratch.length = ngramLen;
						stat.token_cnt++;
						final FpTermKey view_key = FpTermKey.viewOf(sliceScratch, h);
						if (uniqueNgramsThisCommonTerm.containsKey(view_key)) {
							continue;
						}
						Counter val = ngramOccurrenceCount.get(view_key);
						if (val == null) {
							BytesRef sliceScratch_copy = new BytesRef();
							sliceScratch_copy.bytes = sliceScratch.bytes;
							sliceScratch_copy.offset = sliceScratch.offset;
							sliceScratch_copy.length = sliceScratch.length;
							FpTermKey key_soft_copy = FpTermKey.viewOf(sliceScratch_copy, view_key.hashCode());
							val = new Counter(1, key_soft_copy);
							ngramOccurrenceCount.put(key_soft_copy, val);
						} else {
							val.cnt++;
						}
						uniqueNgramsThisCommonTerm.put(val.key, UNIQUE_NGRAM_MARKER);
					}
				}
			}
		} finally {
			FpHashMapPoolHub.release(uniqueNgramsLease);
		}
	}

	/**
	 * 频率达阈值的热词放入结果表（空 doclist），并在 {@code anchorTierIndexByHotTerm} 中按字节长度档登记锚点内的子串
	 * （单档 {@link TreeSet} 规模有上限，避免统计爆炸）。
	 *
	 * @param stat                       统计对象
	 * @param ngramOccurrenceCount       ngram 出现次数映射
	 * @param hotFreqThreshold           热词频率阈值
	 * @param maxDoc                     最大文档号（用于初始化 FPDocList）
	 * @param anchorTierIndexByHotTerm   输出：热词锚点→分档索引映射
	 * @return 待 merge 的热词→空 doclist 映射
	 */
	private static HashMap<FpTermKey, FPDocList> buildHotTermsAndAnchorTierIndex(final HashMap<FpTermKey, FPDocList> hotTermsPendingDocMerge,FpStatNgram stat,
			HashMap<FpTermKey, Counter> ngramOccurrenceCount, long hotFreqThreshold, int maxDoc,
			HashMap<FpTermKey, AnchorTierIndex> anchorTierIndexByHotTerm) {
		
		// 单档 TreeSet 的最大容量上限，防止某个锚点的子串过多导致内存爆炸
		final int tierSetSizeCap = (int) (hotFreqThreshold * 2);

		for (Map.Entry<FpTermKey, Counter> entry : ngramOccurrenceCount.entrySet()) {
			FpTermKey key=entry.getKey();
			// 长度 >1 且频率未达阈值的 ngram 跳过（长度为 1 的单字始终保留）
			if (key.bytesRef().length>1&& entry.getValue().cnt< hotFreqThreshold) {
				stat.freqThreshold_skip++;
				continue;
			}
			stat.freqThreshold_keep++;
			final FpTermKey hotTerm = entry.getKey();
			// 创建空的 doclist，后续 merge 阶段填充
			hotTermsPendingDocMerge.put(hotTerm, new FPDocList(maxDoc));

			// 获取或创建该热词的锚点分档索引
			AnchorTierIndex termsByByteLength = anchorTierIndexByHotTerm.get(hotTerm);
			if (termsByByteLength == null) {
				termsByByteLength = new AnchorTierIndex();
				anchorTierIndexByHotTerm.put(hotTerm, termsByByteLength);
			}

			// 将该热词的所有子 ngram 按长度登记到对应档位
			final BytesRef hotPayload = hotTerm.bytesRef();
			final int base = hotPayload.offset;
			final int payloadLen = hotPayload.length;
			if (payloadLen <= 0) {
				continue;
			}
			final byte[] hotBytes = hotPayload.bytes;
			for (int ngramLen = Lucene80FPSearchConfig.NGRAM_MAX; ngramLen >= Lucene80FPSearchConfig.NGRAM_MIN; ngramLen--) {
				final int maxStart = payloadLen - ngramLen;
				if (maxStart < 0) {
					continue;
				}
				int h = FpTermKeyHash.hashOf(hotBytes, base, ngramLen);
				for (int start = 0; start <= maxStart; start++) {
					if (start > 0) {
						h = FpTermKeyHash.rollStartForward(h, hotBytes, base + start - 1, ngramLen);
					}
					final BytesRef slice = new BytesRef(hotBytes, base + start, ngramLen);
					final FpTermKey tierTerm = FpTermKey.viewOf(slice, h);
					final TreeSet<FpTermKey> tierTermSet = termsByByteLength.get(slice.length);
					// 仅在未超上限时添加，避免单档过大
					if (tierTermSet.size() < tierSetSizeCap) {
						tierTermSet.add(tierTerm);
					}
				}
			}
		}
		return hotTermsPendingDocMerge;
	}

	/**
	 * 将所有热词的 doclist 切换为稀疏模式，以优化后续批量 merge 的性能。
	 *
	 * @param hotTermsPendingDocMerge 待 merge 的热词→doclist 映射
	 */
	private static void ensureAllHotDocListsSparse(HashMap<FpTermKey, FPDocList> hotTermsPendingDocMerge) {
		for (FPDocList docList : hotTermsPendingDocMerge.values()) {
			docList.ensureSparseForBulkMerge();
		}
	}

//	/**
//	 * 按锚点字节长度将 budget 映射分区到数组中，便于 merge 阶段按长度 O(1) 定位。
//	 *
//	 * @param hotTermDownTierBudget 热词→预算映射
//	 * @return 按长度分区的 budget 数组，下标为字节长度
//	 */
//	@SuppressWarnings("unchecked")
//	private static HashMap<FpTermKey, Integer>[] partitionBudgetByAnchorLength1(
//			HashMap<FpTermKey, Integer> hotTermDownTierBudget) {
//		final HashMap<FpTermKey, Integer>[] byLen = new HashMap[Lucene80FPSearchConfig.NGRAM_MAX + 1];
//		for (Map.Entry<FpTermKey, Integer> entry : hotTermDownTierBudget.entrySet()) {
//			final int len = entry.getKey().bytesRef().length;
//			HashMap<FpTermKey, Integer> bucket = byLen[len];
//			if (bucket == null) {
//				bucket = new HashMap<>();
//				byLen[len] = bucket;
//			}
//			bucket.put(entry.getKey(), entry.getValue());
//		}
//		return byLen;
//	}
//
//	/**
//	 * 生成一个布尔数组，标记哪些字节长度在 budget 中存在条目。
//	 * 用于 merge 阶段快速跳过无预算的长度档，避免无效的 HashMap 查找。
//	 *
//	 * @param hotTermDownTierBudget 热词→预算映射
//	 * @return 长度存在性标记数组
//	 */
//	private static boolean[] anchorLengthsPresentInBudget(HashMap<FpTermKey, Integer> hotTermDownTierBudget) {
//		final boolean[] present = new boolean[Lucene80FPSearchConfig.NGRAM_MAX + 1];
//		for (FpTermKey anchor : hotTermDownTierBudget.keySet()) {
//			present[anchor.bytesRef().length] = true;
//		}
//		return present;
//	}

	/**
	 * 统计热词 doclist 中使用稀疏存储的数量。
	 *
	 * @param hotTermsPendingDocMerge 热词→doclist 映射
	 * @return 稀疏 doclist 的数量
	 */
	private static int countSparseHotDocLists(HashMap<FpTermKey, FPDocList> hotTermsPendingDocMerge) {
		int sparse = 0;
		for (FPDocList docList : hotTermsPendingDocMerge.values()) {
			if (docList.docsSparse != null) {
				sparse++;
			}
		}
		return sparse;
	}

	/**
	 * 构建 hotMergeTable：将热词→doclist 映射转换为热词→(docList, ordinal) 映射。
	 * ordinal 用于 mergedThisCommon 位图的索引，避免每次 merge 都做 HashMap 查找。
	 *
	 * @param hotTermsPendingDocMerge 热词→doclist 映射
	 * @return 热词→HotMergeSlot 映射
	 */
	private static HashMap<FpTermKey, HotMergeSlot> buildHotMergeTable(final HashMap<FpTermKey, HotMergeSlot> hotMergeTable,
			HashMap<FpTermKey, FPDocList> hotTermsPendingDocMerge) {
		int ord = 0;
		for (Map.Entry<FpTermKey, FPDocList> entry : hotTermsPendingDocMerge.entrySet()) {
			hotMergeTable.put(entry.getKey(), new HotMergeSlot(entry.getValue(), ord++));
		}
		return hotMergeTable;
	}

	/**
	 * 遍历 common term，把 doc 合并到热词 ngram（长 ngram 优先）；
	 * 在 maxDown 预算内的父前缀在本 common 词内标记为已 merge，避免重复写入。
	 *
	 * <p>核心逻辑：
	 * <ul>
	 *   <li>对每个 common term，从最长到最短遍历其所有 ngram 窗口</li>
	 *   <li>命中热词时，将 common doclist 合并到热词 doclist</li>
	 *   <li>合并后调用 {@link #markParentPrefixesSkippedInCommonTerm} 标记预算内的父前缀</li>
	 *   <li>使用 mergedThisCommon 位图确保同一 common term 内每个热词只 merge 一次</li>
	 *   <li>统计每个 common 载荷内命中 hot 的 ngram 窗口总数，按命中数升序决定写序（0 命中最后）</li>
	 * </ul>
	 *
	 * @param stat              统计对象
	 * @param commonTermToDocs  common term → doclist 映射
	 * @param hotMergeTable     热词→(docList, ordinal) 快速查找表
	 * @param budgetByLen       按长度分区的 budget 数组
	 * @param anchorLenHasBudget 长度存在性标记数组
	 * @throws IOException IO 异常
	 */
	private static ArrayList<CommonTermSortEntry> mergeCommonDocsIntoHotTerms(final FpStatNgram stat,
			HashMap<FpTermKey, FPDocList> commonTermToDocs, final HashMap<FpTermKey, HotMergeSlot> hotMergeTable,
			final HashMap<FpTermKey, Integer> hotTermDownTierBudget) throws IOException {
		// 位图：标记当前 common term 内哪些热词已经 merge 过
		final boolean[] mergedThisCommon = new boolean[hotMergeTable.size()];
		// 复用的 BytesRef 滑动窗口
		final BytesRef sliceScratch = new BytesRef();
		final ArrayList<CommonTermSortEntry> sortEntries = new ArrayList<>(commonTermToDocs.size());
		int originalOrdinal = 0;

		for (Map.Entry<FpTermKey, FPDocList> entry : commonTermToDocs.entrySet()) {
			final BytesRef commonPayload = entry.getKey().bytesRef();
			final FPDocList sourceDocList = entry.getValue();
			final int payloadLen = commonPayload.length;
			if (payloadLen <= 0) {
				continue;
			}
			// 确保源 doclist 为稀疏格式以优化 merge 性能
			sourceDocList.ensureSparseIfMergeSource();

			// 重置位图（每个 common term 独立）
			Arrays.fill(mergedThisCommon, false);
			final byte[] bytes = commonPayload.bytes;
			final int base = commonPayload.offset;
			sliceScratch.bytes = bytes;
			int hotHitCount = 0;

			for (int ngramLen = Lucene80FPSearchConfig.NGRAM_MAX; ngramLen >= Lucene80FPSearchConfig.NGRAM_MIN; ngramLen--) {
				final int maxStart = payloadLen - ngramLen;
				if (maxStart < 0) {
					continue;
				}
				int h = FpTermKeyHash.hashOf(bytes, base, ngramLen);
				for (int start = 0; start <= maxStart; start++) {
					if (start > 0) {
						h = FpTermKeyHash.rollStartForward(h, bytes, base + start - 1, ngramLen);
					}
					sliceScratch.offset = base + start;
					sliceScratch.length = ngramLen;
					final HotMergeSlot slot = hotMergeTable.get(FpTermKey.viewOf(sliceScratch, h));
					if (slot == null) {
						continue;
					}
					hotHitCount++;
					// 已在当前 common term 内 merge 过，跳过
					if (mergedThisCommon[slot.ordinal]) {
						stat.hot_doc_cnt_skip++;
						continue;
					}
					// 标记为已 merge
					mergedThisCommon[slot.ordinal] = true;
					stat.hot_doc_cnt_keep++;

					// 将 common doclist 合并到热词 doclist
					slot.docList.addAllDocsFrom(sourceDocList);
					// 标记预算内的父前缀，避免短 ngram 重复 merge 同一 doc
					markParentPrefixesSkippedInCommonTerm(stat, sliceScratch, mergedThisCommon, hotMergeTable,
							hotTermDownTierBudget);
				}
			}

			final int sortKey = hotHitCount == 0 ? Integer.MAX_VALUE : hotHitCount;
			stat.commonHotHitTierCnt[FpStatNgram.hotHitCountToTier(hotHitCount)]++;
			sortEntries.add(new CommonTermSortEntry(entry.getKey(),entry.getValue(), sortKey, originalOrdinal++));
		}

		sortEntries.sort(Comparator.comparingInt((CommonTermSortEntry e) -> e.sortKey)
				.thenComparingInt(e -> e.originalOrdinal));
		final ArrayList<CommonTermSortEntry> flushOrder = new ArrayList<>(sortEntries.size());
		for (CommonTermSortEntry e : sortEntries) {
			flushOrder.add(e);
		}
		return flushOrder;
	}

	/**
	 * 较长热词 ngram 已 merge 后，对其真前缀判断是否落在父锚点的 maxDown 预算内：
	 * {@code extensionBytesFromParent &lt; parentDownTierBudget} 时在本 common 词内标记父前缀，避免对更短热词重复 merge。
	 * 父前缀非热词（无 level）则跳过。
	 *
	 * <p>设计意图：当子热词已被 merge，且子热词相对于父锚点的扩展字节数在父锚点的预算范围内时，
	 * 说明查询侧可以通过父锚点 + 向下拼接得到该子热词的结果，因此无需再将同一 doc 重复写入父热词 posting。
	 * 若扩展超出预算，则父热词仍需保留该 doc（深档查询不拼回时不能丢 posting）。
	 *
	 * @param stat               统计对象
	 * @param matchedHotSlice    已 merge 的子热词切片
	 * @param mergedThisCommon   当前 common term 的 merge 位图
	 * @param hotMergeTable      热词→(docList, ordinal) 查找表
	 * @param budgetByLen        按长度分区的 budget 数组
	 * @param anchorLenHasBudget 长度存在性标记数组
	 */
	public static void markParentPrefixesSkippedInCommonTerm(final FpStatNgram stat, final BytesRef matchedHotSlice,
			final boolean[] mergedThisCommon, final HashMap<FpTermKey, HotMergeSlot> hotMergeTable,
			final HashMap<FpTermKey, Integer> hotTermDownTierBudget) {
		final int childByteLen = matchedHotSlice.length;
		// 最短 ngram 没有更短的父前缀，直接返回
		if (childByteLen <= Lucene80FPSearchConfig.NGRAM_MIN) {
			return;
		}
		final int childOffset = matchedHotSlice.offset;
		final byte[] bytes = matchedHotSlice.bytes;
		final BytesRef parentScratch = new BytesRef(bytes, childOffset, 0);

		for (int parentLen = childByteLen - 1; parentLen >= Lucene80FPSearchConfig.NGRAM_MIN; parentLen--) {
		
			final int maxStart = childByteLen - parentLen;
			int h = FpTermKeyHash.hashOf(bytes, childOffset, parentLen);
			for (int start = 0; start <= maxStart; start++) {
				if (start > 0) {
					h = FpTermKeyHash.rollStartForward(h, bytes, childOffset + start - 1, parentLen);
				}
				parentScratch.offset = childOffset + start;
				parentScratch.length = parentLen;
				final FpTermKey parentHotKey = FpTermKey.viewOf(parentScratch, h);
				// 查找该父前缀是否为热词及其预算值
				final Integer parentDownTierBudget = hotTermDownTierBudget.get(parentHotKey);
				if (parentDownTierBudget == null) {
					continue;
				}
				// 计算子热词相对于父锚点的扩展字节数
				final int extensionBytesFromParent = childByteLen - parentLen;
				if (extensionBytesFromParent < parentDownTierBudget) {
					// 扩展在预算内：标记父前缀在当前 common term 内已处理
					final HotMergeSlot parentSlot = hotMergeTable.get(parentHotKey);
					if (parentSlot != null) {
						mergedThisCommon[parentSlot.ordinal] = true;
					}
					stat.ngram_level_ok++;
				} else {
					// 扩展超出预算：不标记，父热词仍需保留该 doc
					stat.ngram_level_skip++;
				}
			}
		}
	}

}