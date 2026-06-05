package cn.lxdb.plugins.muqingyu.fptoken.dataset.block;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.lucene.util.BytesRef;
import org.slf4j.Logger;

import cn.lucene.lxdb.params.LxdbLogerEncrypt;
import cn.lucene.proguard.keep.lxdb.common.CLMillisecondClock;
import cn.lxdb.plugins.muqingyu.fptoken.api.FpTokenBlockOrchestrator;
import cn.lxdb.plugins.muqingyu.fptoken.config.Lucene80FPSearchConfig;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FPDocList;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpStatNgram;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpTermKey;

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
    public static final Logger LOG = LxdbLogerEncrypt.getLogger("mqy.fptoken");

	/** merge 阶段一次 {@link HashMap#get} 同时拿到 doclist 与序号。 */
	private static final class HotMergeSlot {
		final FPDocList docList;
		final int ordinal;

		HotMergeSlot(FPDocList docList, int ordinal) {
			this.docList = docList;
			this.ordinal = ordinal;
		}
	}
	/**
	 * 热词重建主流程，结果写入 {@code group} 的热词表与 {@code hotTermDownTierBudget}。
	 */
	public static FpStatNgram execute(FpGroupDataRebuild group, FpTokenBlockOrchestrator parentItem
			) throws IOException {
		

		final long hotFreqThreshold=Lucene80FPSearchConfig.HOT_TIER_TERM_COUNT_THRESHOLD;
		FpStatNgram stat = new FpStatNgram();
		final TreeMap<FpTermKey, FPDocList> hotTermToDocs = group.hotTermMapInternal();
		final TreeMap<FpTermKey, Integer> hotTermDownTierBudget = group.hotTermDownTierBudgetInternal();
		hotTermToDocs.clear();
		hotTermDownTierBudget.clear();

		final TreeMap<FpTermKey, FPDocList> commonTermToDocs = group.commonTermMapInternal();
		final int maxDoc = group.maxDocInternal();

		final int mapCapacity = Math.max(commonTermToDocs.size() / 100, 32);
		final HashMap<FpTermKey, Integer> ngramOccurrenceCount = new HashMap<>(mapCapacity);


		long t0 = CLMillisecondClock.CLOCK.now();
		countNgramOccurrencesInCommon(stat, commonTermToDocs, ngramOccurrenceCount);
		stat.ms_count = CLMillisecondClock.CLOCK.now() - t0;
		stat.distinct_ngram = ngramOccurrenceCount.size();

		final HashMap<FpTermKey, AnchorTierIndex> anchorTierIndexByHotTerm = new HashMap<>(mapCapacity);
		t0 = CLMillisecondClock.CLOCK.now();
		final HashMap<FpTermKey, FPDocList> hotTermsPendingDocMerge = buildHotTermsAndAnchorTierIndex(stat,
				ngramOccurrenceCount, hotFreqThreshold, maxDoc, anchorTierIndexByHotTerm);
		stat.ms_build = CLMillisecondClock.CLOCK.now() - t0;
		stat.hot_pending = hotTermsPendingDocMerge.size();

		t0 = CLMillisecondClock.CLOCK.now();
		computeHotDownTierBudgets(stat,hotTermDownTierBudget, anchorTierIndexByHotTerm, hotFreqThreshold);
		stat.ms_budget = CLMillisecondClock.CLOCK.now() - t0;
		stat.budget_entries = hotTermDownTierBudget.size();

		// merge 阶段大量 get：一次性拷贝为 HashMap，规则不变，仅加速查找
		final HashMap<FpTermKey, Integer> hotTermDownTierBudgetFast = new HashMap<>(hotTermDownTierBudget);
		ensureAllHotDocListsSparse(hotTermsPendingDocMerge);
		final HashMap<FpTermKey, HotMergeSlot> hotMergeTable = buildHotMergeTable(hotTermsPendingDocMerge);
		@SuppressWarnings("unchecked")
		final HashMap<FpTermKey, Integer>[] budgetByLen = partitionBudgetByAnchorLength(hotTermDownTierBudgetFast);
		final boolean[] anchorLenHasBudget = anchorLengthsPresentInBudget(hotTermDownTierBudgetFast);
		t0 = CLMillisecondClock.CLOCK.now();
		mergeCommonDocsIntoHotTerms(stat, commonTermToDocs, hotMergeTable, budgetByLen, anchorLenHasBudget);
		stat.ms_merge = CLMillisecondClock.CLOCK.now() - t0;
		stat.hot_doclist_sparse = countSparseHotDocLists(hotTermsPendingDocMerge);
		anchorTierIndexByHotTerm.clear();

		hotTermToDocs.putAll(hotTermsPendingDocMerge);
		stat.hot_final = hotTermToDocs.size();

		return stat;
	}

	

	/**
	 * 为每个热词锚点计算 {@code hotTermDownTierBudget}：从锚点字节长向下，累加各档已登记子串个数；
	 * 累加超过 {@code hotFreqThreshold} 前每纳入一档则 {@code downTierBudget++}，作为查询/merge 可向下扩展的字节层数预算。
	 */
	private static void computeHotDownTierBudgets(FpStatNgram stat, TreeMap<FpTermKey, Integer> hotTermDownTierBudget,
			HashMap<FpTermKey, AnchorTierIndex> anchorTierIndexByHotTerm, final long hotFreqThreshold) {
		for (Map.Entry<FpTermKey, AnchorTierIndex> entry : anchorTierIndexByHotTerm.entrySet()) {
			final FpTermKey anchorTerm = entry.getKey();
			final AnchorTierIndex termsByByteLength = entry.getValue();
			int cumulativeTierTermCount = 0;
			int downTierBudget = 1;
			final int anchorByteLen = anchorTerm.bytesRef().length;
			for (int byteLen = (anchorByteLen+1); byteLen <= Lucene80FPSearchConfig.NGRAM_MAX; byteLen++) {
				cumulativeTierTermCount += termsByByteLength.get(byteLen).size();
				if (cumulativeTierTermCount > hotFreqThreshold) {
					break;
				}
				downTierBudget++;
			}
			
			stat.term_level_cnt[anchorTerm.bytesRef().length][downTierBudget]++;
			hotTermDownTierBudget.put(anchorTerm, downTierBudget);
		}
	}

	/** 在每个 common 词项载荷内滑窗切 ngram，累加出现次数。 */
	private static void countNgramOccurrencesInCommon(FpStatNgram stat, TreeMap<FpTermKey, FPDocList> commonTermToDocs,
			HashMap<FpTermKey, Integer> ngramOccurrenceCount) {
		final BytesRef sliceScratch = new BytesRef();

		for (Map.Entry<FpTermKey, FPDocList> entry : commonTermToDocs.entrySet()) {
			final BytesRef commonPayload = entry.getKey().bytesRef();
			final int payloadLen = commonPayload.length;
			if (payloadLen <= 0) {
				continue;
			}
			stat.term_cnt++;
			final HashSet<FpTermKey> uniqueNgramsThisCommonTerm = new HashSet<>(
					Math.max(16, Math.min(payloadLen * 4, 4096)));
			final byte[] bytes = commonPayload.bytes;
			final int base = commonPayload.offset;
			sliceScratch.bytes = bytes;
			for (int start = 0; start < payloadLen; start++) {
				for (int ngramLen = Lucene80FPSearchConfig.NGRAM_MIN; ngramLen <= Lucene80FPSearchConfig.NGRAM_MAX
						&& start + ngramLen <= payloadLen; ngramLen++) {
					sliceScratch.offset = base + start;
					sliceScratch.length = ngramLen;
					stat.token_cnt++;
					if (uniqueNgramsThisCommonTerm.contains(FpTermKey.viewOf(sliceScratch))) {
						continue;
					}
					final FpTermKey ngramKey = FpTermKey.copyOf(sliceScratch);
					uniqueNgramsThisCommonTerm.add(ngramKey);
					ngramOccurrenceCount.merge(ngramKey, 1, Integer::sum);
				}
			}
		}
	}

	/**
	 * 频率达阈值的热词放入结果表（空 doclist），并在 {@code anchorTierIndexByHotTerm} 中按字节长度档登记锚点内的子串
	 * （单档 {@link TreeSet} 规模有上限，避免统计爆炸）。
	 */
	private static HashMap<FpTermKey, FPDocList> buildHotTermsAndAnchorTierIndex(FpStatNgram stat,
			HashMap<FpTermKey, Integer> ngramOccurrenceCount, long hotFreqThreshold, int maxDoc,
			HashMap<FpTermKey, AnchorTierIndex> anchorTierIndexByHotTerm) {
		final HashMap<FpTermKey, FPDocList> hotTermsPendingDocMerge = new HashMap<>(
				Math.max(16, ngramOccurrenceCount.size() / 4));
		final int tierSetSizeCap = (int) (hotFreqThreshold * 2);

		for (Map.Entry<FpTermKey, Integer> entry : ngramOccurrenceCount.entrySet()) {
			FpTermKey key=entry.getKey();
			if (key.bytesRef().length>1&& entry.getValue()< hotFreqThreshold) {
				stat.freqThreshold_skip++;
				continue;
			}
			stat.freqThreshold_keep++;
			final FpTermKey hotTerm = entry.getKey();
			hotTermsPendingDocMerge.put(hotTerm, new FPDocList(maxDoc));

			AnchorTierIndex termsByByteLength = anchorTierIndexByHotTerm.get(hotTerm);
			if (termsByByteLength == null) {
				termsByByteLength = new AnchorTierIndex();
				anchorTierIndexByHotTerm.put(hotTerm, termsByByteLength);
			}

			final BytesRef hotPayload = hotTerm.bytesRef();
			final int base = hotPayload.offset;
			final int payloadLen = hotPayload.length;
			if (payloadLen <= 0) {
				continue;
			}
			for (int ngramLen = Lucene80FPSearchConfig.NGRAM_MAX; ngramLen >= Lucene80FPSearchConfig.NGRAM_MIN; ngramLen--) {
				for (int start = 0; start + ngramLen <= payloadLen; start++) {
					final BytesRef slice = new BytesRef(hotPayload.bytes, base + start, ngramLen);
					final FpTermKey tierTerm = FpTermKey.viewOf(slice);
					final TreeSet<FpTermKey> tierTermSet = termsByByteLength.get(slice.length);
					if (tierTermSet.size() < tierSetSizeCap) {
						tierTermSet.add(tierTerm);
					}
				}
			}
		}
		return hotTermsPendingDocMerge;
	}

	private static void ensureAllHotDocListsSparse(HashMap<FpTermKey, FPDocList> hotTermsPendingDocMerge) {
		for (FPDocList docList : hotTermsPendingDocMerge.values()) {
			docList.ensureSparseForBulkMerge();
		}
	}

	@SuppressWarnings("unchecked")
	private static HashMap<FpTermKey, Integer>[] partitionBudgetByAnchorLength(
			HashMap<FpTermKey, Integer> hotTermDownTierBudget) {
		final HashMap<FpTermKey, Integer>[] byLen = new HashMap[Lucene80FPSearchConfig.NGRAM_MAX + 1];
		for (Map.Entry<FpTermKey, Integer> entry : hotTermDownTierBudget.entrySet()) {
			final int len = entry.getKey().bytesRef().length;
			HashMap<FpTermKey, Integer> bucket = byLen[len];
			if (bucket == null) {
				bucket = new HashMap<>();
				byLen[len] = bucket;
			}
			bucket.put(entry.getKey(), entry.getValue());
		}
		return byLen;
	}

	private static boolean[] anchorLengthsPresentInBudget(HashMap<FpTermKey, Integer> hotTermDownTierBudget) {
		final boolean[] present = new boolean[Lucene80FPSearchConfig.NGRAM_MAX + 1];
		for (FpTermKey anchor : hotTermDownTierBudget.keySet()) {
			present[anchor.bytesRef().length] = true;
		}
		return present;
	}

	private static int countSparseHotDocLists(HashMap<FpTermKey, FPDocList> hotTermsPendingDocMerge) {
		int sparse = 0;
		for (FPDocList docList : hotTermsPendingDocMerge.values()) {
			if (docList.docsSparse != null) {
				sparse++;
			}
		}
		return sparse;
	}

	private static HashMap<FpTermKey, HotMergeSlot> buildHotMergeTable(
			HashMap<FpTermKey, FPDocList> hotTermsPendingDocMerge) {
		final HashMap<FpTermKey, HotMergeSlot> hotMergeTable = new HashMap<>(hotTermsPendingDocMerge.size() * 2);
		int ord = 0;
		for (Map.Entry<FpTermKey, FPDocList> entry : hotTermsPendingDocMerge.entrySet()) {
			hotMergeTable.put(entry.getKey(), new HotMergeSlot(entry.getValue(), ord++));
		}
		return hotMergeTable;
	}

	/**
	 * 遍历 common，把 doc 合并到热词 ngram（长 ngram 优先）；在 maxDown 预算内的父前缀在本 common 词内标记为已 merge，避免重复写入。
	 */
	private static void mergeCommonDocsIntoHotTerms(final FpStatNgram stat, TreeMap<FpTermKey, FPDocList> commonTermToDocs,
			final HashMap<FpTermKey, HotMergeSlot> hotMergeTable,
			final HashMap<FpTermKey, Integer>[] budgetByLen, final boolean[] anchorLenHasBudget) throws IOException {
		final boolean[] mergedThisCommon = new boolean[hotMergeTable.size()];
		final BytesRef sliceScratch = new BytesRef();

		for (Map.Entry<FpTermKey, FPDocList> entry : commonTermToDocs.entrySet()) {
			final BytesRef commonPayload = entry.getKey().bytesRef();
			final FPDocList sourceDocList = entry.getValue();
			final int payloadLen = commonPayload.length;
			if (payloadLen <= 0) {
				continue;
			}
			sourceDocList.ensureSparseIfMergeSource();

			Arrays.fill(mergedThisCommon, false);
			final byte[] bytes = commonPayload.bytes;
			final int base = commonPayload.offset;
			sliceScratch.bytes = bytes;

			for (int ngramLen = Lucene80FPSearchConfig.NGRAM_MAX; ngramLen >= Lucene80FPSearchConfig.NGRAM_MIN; ngramLen--) {
				for (int start = 0; start + ngramLen <= payloadLen; start++) {

					sliceScratch.offset = base + start;
					sliceScratch.length = ngramLen;
					final HotMergeSlot slot = hotMergeTable.get(FpTermKey.viewOf(sliceScratch));
					if (slot == null) {
						continue;
					}
					if (mergedThisCommon[slot.ordinal]) {
						stat.hot_doc_cnt_skip++;
						continue;
					}
					mergedThisCommon[slot.ordinal] = true;
					stat.hot_doc_cnt_keep++;

					slot.docList.addAllDocsFrom(sourceDocList);
					markParentPrefixesSkippedInCommonTerm(stat, sliceScratch, mergedThisCommon, hotMergeTable,
							budgetByLen, anchorLenHasBudget);
				}
			}
		}
	}

	/**
	 * 较长热词 ngram 已 merge 后，对其真前缀判断是否落在父锚点的 maxDown 预算内：
	 * {@code extensionBytesFromParent &lt; parentDownTierBudget} 时在本 common 词内标记父前缀，避免对更短热词重复 merge。
	 * 父前缀非热词（无 level）则跳过。
	 */
	public static void markParentPrefixesSkippedInCommonTerm(final FpStatNgram stat, final BytesRef matchedHotSlice,
			final boolean[] mergedThisCommon, final HashMap<FpTermKey, HotMergeSlot> hotMergeTable,
			final HashMap<FpTermKey, Integer>[] budgetByLen, final boolean[] anchorLenHasBudget) {
		final int childByteLen = matchedHotSlice.length;
		if (childByteLen <= Lucene80FPSearchConfig.NGRAM_MIN) {
			return;
		}
		final int childOffset = matchedHotSlice.offset;
		final BytesRef parentScratch = new BytesRef(matchedHotSlice.bytes, childOffset, 0);

		for (int parentLen = childByteLen - 1; parentLen >= Lucene80FPSearchConfig.NGRAM_MIN; parentLen--) {
			if (!anchorLenHasBudget[parentLen]) {
				continue;
			}
			final HashMap<FpTermKey, Integer> budgetAtLen = budgetByLen[parentLen];
			final int maxStart = childByteLen - parentLen;
			for (int start = 0; start <= maxStart; start++) {
				parentScratch.offset = childOffset + start;
				parentScratch.length = parentLen;
				final FpTermKey parentHotKey = FpTermKey.viewOf(parentScratch);
				final Integer parentDownTierBudget = budgetAtLen.get(parentHotKey);
				if (parentDownTierBudget == null) {
					continue;
				}
				final int extensionBytesFromParent = childByteLen - parentLen;
				if (extensionBytesFromParent < parentDownTierBudget) {
					final HotMergeSlot parentSlot = hotMergeTable.get(parentHotKey);
					if (parentSlot != null) {
						mergedThisCommon[parentSlot.ordinal] = true;
					}
					stat.ngram_level_ok++;
				} else {
					stat.ngram_level_skip++;
				}
			}
		}
	}

}
