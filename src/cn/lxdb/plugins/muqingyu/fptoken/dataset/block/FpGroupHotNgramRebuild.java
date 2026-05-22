package cn.lxdb.plugins.muqingyu.fptoken.dataset.block;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.lucene.util.BytesRef;

import cn.lxdb.plugins.muqingyu.fptoken.api.FpTokenBlockOrchestrator;
import cn.lxdb.plugins.muqingyu.fptoken.config.Lucene80FPSearchConfig;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FPDocList;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpStatNgram;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpTermKey;

/**
 * 从 {@link FpGroupDataRebuild#commonTermMapInternal()} 按 byte n-gram 挖掘热词，合并 doc，并计算
 * {@link FpGroupDataRebuild#hotTermToLevel}（锚点向下可遍历深度）。
 *
 * <h2>实现原理</h2>
 * <p>
 * 热词表是扁平 {@link TreeMap}，不是一棵树；层级关系由<strong>字节前缀 + 长度档</strong>表达。
 * 以锚点热词 {@code ab}（字节长 2）为例：
 * </p>
 * <ul>
 *   <li><b>长度档</b>：{@code i=2} 为锚点自身；{@code i=3,4,5…} 依次对应向下扩展的 {@code ab*}、{@code ab**}、{@code ab***}…
 *       （示意：固定总字节长度的一层热词/子串，不把更长档算进更短档的个数里单独去重，但见下「累加预算」）。</li>
 *   <li><b>{@code hotTermToLevel}（maxDown）</b>：从锚点字节长起，按档累加已登记子串个数 {@code cumulativeTierCount}；
 *       若累加仍 ≤ {@link Lucene80FPSearchConfig#HOT_TIER_TERM_COUNT_THRESHOLD}，则允许再向下一档遍历并 {@code maxDownLevel++}；
 *       超过阈值则停止——表示查询侧再向下拼子档的代价过大。该值写入热词 term 布局，供检索按深度截断。</li>
 *   <li><b>写段 merge（空间换时间）</b>：同一 common 载荷内，对命中的热词 ngram 从长到短合并 doc。
 *       较长热词 merge 后，对其真前缀调用 {@link #markParentPrefixesSkippedInCommonTerm}：
 *       若 {@code extensionBytesFromParent = childLen - parentLen <= parentMaxDown}，则在本 common 词内标记父前缀已处理，
 *       避免短 ngram 再次 merge 同 doc；若更深则<strong>不</strong>标记，父热词 posting 仍可保留该 doc（深档查询不拼回时不能丢 posting）。</li>
 *   <li><b>不做写段后 doclist 剔除</b>：不在 merge 之后从父热词 posting 物理删除子档 doc；控制粒度是「单 common 内是否重复 merge」+ 查询按 level 拼回。</li>
 * </ul>
 *
 * <h2>流程</h2>
 * <ol>
 *   <li>{@link #countNgramOccurrencesInCommon} — 统计 common 滑窗 ngram 出现次数；</li>
 *   <li>{@link #buildHotTermsAndAnchorTierIndex} — 频率达阈值者进入热词表，并建立锚点分档索引；</li>
 *   <li>{@link #computeMaxDownLevels} — 写入 {@code hotTermToLevel}；</li>
 *   <li>{@link #mergeCommonDocsIntoHotTerms} — common doc 合并到热词，并按 maxDown 标记父前缀 skip。</li>
 * </ol>
 */
public final class FpGroupHotNgramRebuild {

	

	/**
	 * 热词重建主流程，结果写入 {@code group} 的热词表与 {@code hotTermToLevel}。
	 */
	public static FpStatNgram execute(FpGroupDataRebuild group, FpTokenBlockOrchestrator parentItem,
			final long hotFreqThreshold) throws IOException {
		FpStatNgram stat = new FpStatNgram();
		final TreeMap<FpTermKey, FPDocList> hotTermToDocs = group.hotTermMapInternal();
		final TreeMap<FpTermKey, Integer> hotTermToLevel = group.hotTermToLevelInternal();
		hotTermToDocs.clear();
		hotTermToLevel.clear();

		final TreeMap<FpTermKey, FPDocList> commonTermToDocs = group.commonTermMapInternal();
		final int maxDoc = group.maxDocInternal();

		final int mapCapacity = Math.max(commonTermToDocs.size() / 100, 32);
		final HashMap<FpTermKey, Integer> ngramOccurrenceCount = new HashMap<>(mapCapacity);
		countNgramOccurrencesInCommon(stat, commonTermToDocs, ngramOccurrenceCount);

		final HashMap<FpTermKey, AnchorTierIndex> anchorTierIndexByHotTerm = new HashMap<>(mapCapacity);
		final TreeMap<FpTermKey, FPDocList> hotTermsPendingDocMerge = buildHotTermsAndAnchorTierIndex(stat,
				ngramOccurrenceCount, hotFreqThreshold, maxDoc, anchorTierIndexByHotTerm);

		computeMaxDownLevels(stat,hotTermToLevel, anchorTierIndexByHotTerm, hotFreqThreshold);

		mergeCommonDocsIntoHotTerms(stat, commonTermToDocs, hotTermsPendingDocMerge, hotTermToLevel);
		anchorTierIndexByHotTerm.clear();

		hotTermToDocs.putAll(hotTermsPendingDocMerge);
		return stat;
	}

	/**
	 * 为每个热词锚点计算 {@code hotTermToLevel}：从锚点字节长向下，累加各档已登记子串个数；
	 * 累加超过 {@code hotFreqThreshold} 前每纳入一档则 {@code maxDownLevel++}，作为查询/merge 可向下扩展的字节层数预算。
	 */
	private static void computeMaxDownLevels(FpStatNgram stat, TreeMap<FpTermKey, Integer> hotTermToLevel,
			HashMap<FpTermKey, AnchorTierIndex> anchorTierIndexByHotTerm, final long hotFreqThreshold) {
		for (Map.Entry<FpTermKey, AnchorTierIndex> entry : anchorTierIndexByHotTerm.entrySet()) {
			final FpTermKey anchorTerm = entry.getKey();
			final AnchorTierIndex termsByByteLength = entry.getValue();
			int cumulativeTierTermCount = 0;
			int maxDownLevel = 0;
			final int anchorByteLen = anchorTerm.bytesRef().length;
			for (int byteLen = anchorByteLen; byteLen <= Lucene80FPSearchConfig.NGRAM_MAX; byteLen++) {
				cumulativeTierTermCount += termsByByteLength.get(byteLen).size();
				if (cumulativeTierTermCount > hotFreqThreshold) {
					break;
				}
				maxDownLevel++;
			}
			
			stat.term_level_cnt[anchorTerm.bytesRef().length][maxDownLevel]++;
			hotTermToLevel.put(anchorTerm, maxDownLevel);
		}
	}

	/** 在每个 common 词项载荷内滑窗切 ngram，累加出现次数。 */
	private static void countNgramOccurrencesInCommon(FpStatNgram stat, TreeMap<FpTermKey, FPDocList> commonTermToDocs,
			HashMap<FpTermKey, Integer> ngramOccurrenceCount) {
		final Set<FpTermKey> uniqueNgramsThisCommonTerm = new HashSet<>();

		for (Map.Entry<FpTermKey, FPDocList> entry : commonTermToDocs.entrySet()) {
			final BytesRef commonPayload = entry.getKey().bytesRef();
			final int payloadLen = commonPayload.length;
			if (payloadLen <= 0) {
				continue;
			}
			stat.term_cnt++;
			uniqueNgramsThisCommonTerm.clear();
			final int base = commonPayload.offset;
			for (int start = 0; start < payloadLen; start++) {
				for (int ngramLen = Lucene80FPSearchConfig.NGRAM_MIN; ngramLen <= Lucene80FPSearchConfig.NGRAM_MAX
						&& start + ngramLen <= payloadLen; ngramLen++) {
					final BytesRef slice = new BytesRef(commonPayload.bytes, base + start, ngramLen);
					stat.token_cnt++;
					final FpTermKey ngramKey = FpTermKey.viewOf(slice);

					if (!uniqueNgramsThisCommonTerm.add(ngramKey)) {
						continue;
					}
					ngramOccurrenceCount.merge(ngramKey, 1, Integer::sum);
				}
			}
		}
	}

	/**
	 * 频率达阈值的热词放入结果表（空 doclist），并在 {@code anchorTierIndexByHotTerm} 中按字节长度档登记锚点内的子串
	 * （单档 {@link TreeSet} 规模有上限，避免统计爆炸）。
	 */
	private static TreeMap<FpTermKey, FPDocList> buildHotTermsAndAnchorTierIndex(FpStatNgram stat,
			HashMap<FpTermKey, Integer> ngramOccurrenceCount, long hotFreqThreshold, int maxDoc,
			HashMap<FpTermKey, AnchorTierIndex> anchorTierIndexByHotTerm) {
		final TreeMap<FpTermKey, FPDocList> hotTermsPendingDocMerge = new TreeMap<>(FpTermKey.ORDER_BY_LENGTH_THEN_BYTES);
		final int tierSetSizeCap = (int) (hotFreqThreshold * 2);

		for (Map.Entry<FpTermKey, Integer> entry : ngramOccurrenceCount.entrySet()) {
			if (entry.getValue() < hotFreqThreshold) {
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

	/**
	 * 遍历 common，把 doc 合并到热词 ngram（长 ngram 优先）；在 maxDown 预算内的父前缀在本 common 词内标记为已 merge，避免重复写入。
	 */
	private static void mergeCommonDocsIntoHotTerms(final FpStatNgram stat, TreeMap<FpTermKey, FPDocList> commonTermToDocs,
			TreeMap<FpTermKey, FPDocList> hotTermsPendingDocMerge, final TreeMap<FpTermKey, Integer> hotTermToLevel)
			throws IOException {
		final Set<FpTermKey> mergedHotKeysThisCommonTerm = new HashSet<>();

		for (Map.Entry<FpTermKey, FPDocList> entry : commonTermToDocs.entrySet()) {
			final BytesRef commonPayload = entry.getKey().bytesRef();
			final FPDocList sourceDocList = entry.getValue();
			final int payloadLen = commonPayload.length;
			if (payloadLen <= 0) {
				continue;
			}

			mergedHotKeysThisCommonTerm.clear();
			final int base = commonPayload.offset;

			for (int ngramLen = Lucene80FPSearchConfig.NGRAM_MAX; ngramLen >= Lucene80FPSearchConfig.NGRAM_MIN; ngramLen--) {
				for (int start = 0; start + ngramLen <= payloadLen; start++) {

					final BytesRef slice = new BytesRef(commonPayload.bytes, base + start, ngramLen);
					final FpTermKey hotNgramKey = FpTermKey.viewOf(slice);

					final FPDocList hotDocList = hotTermsPendingDocMerge.get(hotNgramKey);
					if (hotDocList != null) {
						if (!mergedHotKeysThisCommonTerm.add(hotNgramKey)) {
							stat.hot_doc_cnt_skip++;
							continue;
						}
						stat.hot_doc_cnt_keep++;

						hotDocList.addAllDocsFrom(sourceDocList);
						markParentPrefixesSkippedInCommonTerm(stat, slice, mergedHotKeysThisCommonTerm, hotTermToLevel);
					}
				}
			}
		}
	}

	/**
	 * 较长热词 ngram 已 merge 后，对其真前缀判断是否落在父锚点的 maxDown 预算内：
	 * {@code extensionBytesFromParent <= parentMaxDown} 时在本 common 词内标记父前缀，避免对更短热词重复 merge。
	 * 父前缀非热词（无 level）则跳过。
	 */
	public static void markParentPrefixesSkippedInCommonTerm(final FpStatNgram stat, final BytesRef matchedHotSlice,
			final Set<FpTermKey> mergedHotKeysThisCommonTerm, final TreeMap<FpTermKey, Integer> hotTermToLevel) {
		final int childByteLen = matchedHotSlice.length;
		final int childOffset = matchedHotSlice.offset;

		for (int parentLen = matchedHotSlice.length - 1; parentLen >= Lucene80FPSearchConfig.NGRAM_MIN; parentLen--) {
			for (int start = 0; start + parentLen <= childByteLen; start++) {
				final BytesRef parentPrefixSlice = new BytesRef(matchedHotSlice.bytes, childOffset + start, parentLen);
				final FpTermKey parentHotKey = FpTermKey.viewOf(parentPrefixSlice);
				final Integer parentMaxDown = hotTermToLevel.get(parentHotKey);
				if (parentMaxDown == null) {
					continue;
				}
				final int extensionBytesFromParent = childByteLen - parentPrefixSlice.length;
				if (extensionBytesFromParent <= parentMaxDown) {
					mergedHotKeysThisCommonTerm.add(parentHotKey);
					stat.ngram_level_ok++;
				} else {
					stat.ngram_level_skip++;
				}
			}
		}
	}

}
