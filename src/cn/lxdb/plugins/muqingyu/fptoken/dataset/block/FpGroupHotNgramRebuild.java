package cn.lxdb.plugins.muqingyu.fptoken.dataset.block;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.lucene.util.BytesRef;

import cn.lxdb.plugins.muqingyu.fptoken.api.FpTokenBlockOrchestrator;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FPDocList;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpHotTermHierarchyConstants;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpStatNgram;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpTermKey;

/**
 * 从 {@link FpGroupDataRebuild#commonTermMapInternal()} 按 byte n-gram(1~8) 挖掘热词，合并 doc 后做<strong>前缀层级剔除</strong>。
 * <p>
 * <b>层级剔除在说什么</b>（以父热词 {@code ab} 为例，字节长度 len=2）：
 * <ul>
 *   <li>最终热词表 {@code finalHot} 里每个键是一个独立热词（如 {@code ab}、{@code abc}、{@code abcd}…），不是一棵树；</li>
 *   <li>「档」= 以 {@code ab} 为字节前缀、且<strong>总字节长度固定</strong> 的那一层热词个数：
 *     <ul>
 *       <li>len=3 档（+1 字节，示意 {@code ab*}）：只数 {@code abc,abd,…}，<strong>不含</strong> len=4、5；</li>
 *       <li>len=4 档（+2 字节，示意 {@code ab**}）：只数 {@code abcd,abef,…}；</li>
 *       <li>len=5 档（+3 字节，示意 {@code ab***}）：只数该档；<strong>不会</strong>把 len=3、4、5 加在一起算 32。</li>
 *     </ul>
 *   </li>
 *   <li>{@link FpGroupDataRebuild#hotTermToLevel}：以该热词为锚点的 {@code maxDown}（0=仅自身，1=ab+ab*，2=ab+ab*+ab**），由 {@link #resolveMaxDownLevelsFromAnchor} 按各档 &lt;32 连续层数算出；</li>
 *   <li>写段剔除：子 C 的 {@code relLevel} &lt;= 父锚点 {@code maxDown} 时，从父 doclist 剔除 C 的 doc（查询再拼回）。</li>
 * </ul>
 * 各档热词个数见 {@link #countHotTermsInTier}（扫描 {@code finalHot}，按 exactLength 分档，非累加）。
 */
public final class FpGroupHotNgramRebuild {

	public static final int NGRAM_MIN = 1;
	public static final int NGRAM_MAX = 8;

	private FpGroupHotNgramRebuild() {
	}

	/**
	 * 热词重建主流程。
	 * <ol>
	 *   <li>{@link #countNewHotNgramsFromCommon}：统计 common 里 ngram 出现次数；</li>
	 *   <li>{@link #buildFinalHotTerms}：频率 ≥ 阈值（32）的 ngram 进入 finalHot（空 doclist）；</li>
	 *   <li>{@link #mergeCommonDocsIntoFinalHot}：把 common 的 doc 合并到命中的热词 ngram 上；</li>
	 *   <li>{@link #fillHotTermLevels}：写入 {@link FpGroupDataRebuild#hotTermToLevel}（锚点向下最多几层，0 起）；</li>
	 *   <li>{@link #applyHierarchyDocStripping}：按档计数决定父热词是否剔除子热词 doc。</li>
	 * </ol>
	 */
	public static FpStatNgram execute(FpGroupDataRebuild group, FpTokenBlockOrchestrator parentItem, final long freqThreshold)
			throws IOException {
		FpStatNgram stat = new FpStatNgram();
		final TreeMap<FpTermKey, FPDocList> hot = group.hotTermMapInternal();
		final TreeMap<FpTermKey, Integer> hotLevel = group.hotTermToLevelInternal();
		hot.clear();
		hotLevel.clear();

		final TreeMap<FpTermKey, FPDocList> common = group.commonTermMapInternal();
		final int maxDoc = group.maxDocInternal();

		final HashMap<FpTermKey, Integer> newhotFreq = new HashMap<>(Math.max(common.size() / 100, 32));

		countNewHotNgramsFromCommon(stat, common, newhotFreq);

		HashMap<FpTermKey, ArrayList<TreeSet<FpTermKey>>> hotfreqSub = new HashMap<>(Math.max(common.size() / 100, 32));

		final TreeMap<FpTermKey, FPDocList> finalHot = buildFinalHotTerms(stat, newhotFreq, freqThreshold, maxDoc,hotfreqSub);

		fillHotTermLevels(hotLevel, hotfreqSub,freqThreshold);

		mergeCommonDocsIntoFinalHot(stat, common, finalHot,hotLevel);
		hotfreqSub.clear();
		hotfreqSub=null;

		hot.putAll(finalHot);

		return stat;
	}

	/**
	 * 为每个热词（作为锚点前缀）写入 {@link FpGroupDataRebuild#hotTermToLevel}：
	 * 从 0 起，表示查询最多向下拼回几层子档（见 {@link #resolveMaxDownLevelsFromAnchor}）。
	 */
	private static void fillHotTermLevels(TreeMap<FpTermKey, Integer> hotLevel, HashMap<FpTermKey, ArrayList<TreeSet<FpTermKey>>> hotfreqSub,final long freqThreshold) {
		for (Map.Entry<FpTermKey, ArrayList<TreeSet<FpTermKey>>> e : hotfreqSub.entrySet()) {
			FpTermKey key=e.getKey();
			ArrayList<TreeSet<FpTermKey>> val=e.getValue();
			int amt=0;
			int level=0;
			for(int i=key.bytesRef().length;i<=NGRAM_MAX;i++)
			{
				amt+=val.get(i).size();
				if(amt>freqThreshold)
				{
					break;
				}
				
				level++;
			}
			
			hotLevel.put(key, level);
		
		}
	}

	

	





	/** 在每个 common 词项载荷内滑窗切 ngram，累加出现次数到 newhotFreq。 */
	private static void countNewHotNgramsFromCommon(FpStatNgram stat, TreeMap<FpTermKey, FPDocList> common,
			HashMap<FpTermKey, Integer> newhotFreq) {
		final Set<FpTermKey> uniqueNgramsThisTerm = new HashSet<>();

		for (Map.Entry<FpTermKey, FPDocList> e : common.entrySet()) {
			final BytesRef term = e.getKey().bytesRef();
			final int payloadLen = term.length;
			if (payloadLen <= 0) {
				continue;
			}
			stat.term_cnt++;
			uniqueNgramsThisTerm.clear();
			final int base = term.offset;
			for (int start = 0; start < payloadLen; start++) {
				for (int n = NGRAM_MIN; n <= NGRAM_MAX && start + n <= payloadLen; n++) {
					final BytesRef slice = new BytesRef(term.bytes, base + start, n);
					stat.token_cnt++;
					final FpTermKey key = FpTermKey.viewOf(slice);

					if (!uniqueNgramsThisTerm.add(key)) {
						continue;
					}
					newhotFreq.merge(key, 1, Integer::sum);
				}
			}
		}
	}

	/** 频率达阈值的热词放入 finalHot（此时 doclist 仍为空，待 merge）。 */
	private static TreeMap<FpTermKey, FPDocList> buildFinalHotTerms(FpStatNgram stat,
			HashMap<FpTermKey, Integer> newhotFreq, long freqThreshold, int maxDoc,HashMap<FpTermKey, ArrayList<TreeSet<FpTermKey>>> hotfreqSub) {
		final TreeMap<FpTermKey, FPDocList> out = new TreeMap<>();
		long freqThreshold_level=freqThreshold*2;

		for (Map.Entry<FpTermKey, Integer> e : newhotFreq.entrySet()) {
			if (e.getValue() < freqThreshold) {
				stat.freqThreshold_skip++;
				continue;
			}
			stat.freqThreshold_keep++;
			FpTermKey key= e.getKey();
			out.put(key, new FPDocList(maxDoc));
			ArrayList<TreeSet<FpTermKey>> list=hotfreqSub.get(key);
			if(list==null)
			{
				list=new ArrayList<TreeSet<FpTermKey>>(NGRAM_MAX);
				for(int i=0;i<=NGRAM_MAX;i++)
				{
					list.add(new TreeSet<FpTermKey>());
				}
				
				hotfreqSub.put(key, list);
			}
			
			

			final BytesRef term = key.bytesRef();

			final int base = term.offset;
			final int payloadLen = term.length;
			if (payloadLen <= 0) {
				continue;
			}
			for (int len = NGRAM_MAX; len >= NGRAM_MIN; len--) {
				for (int start = 0; start + len <= payloadLen; start++) {
					final BytesRef slice = new BytesRef(term.bytes, base + start, len);
					final FpTermKey mergeKey = FpTermKey.viewOf(slice);
					TreeSet<FpTermKey> set=list.get(slice.length);
					if(set.size()<freqThreshold_level)
					{
						set.add(mergeKey);
					}
					
				}
			}
		
			
			
		}
		return out;
	}

	/**
	 * 遍历 common，把 doc 合并到 finalHot 里已存在的热词 ngram 上（长 ngram 优先，短 ngram 用 mark_skip 避免重复合并）。
	 */
	private static void mergeCommonDocsIntoFinalHot(final FpStatNgram stat, TreeMap<FpTermKey, FPDocList> common,
			TreeMap<FpTermKey, FPDocList> finalHot,final TreeMap<FpTermKey, Integer> hotLevel) throws IOException {
		final Set<FpTermKey> mergedNgramKeysThisTerm = new HashSet<>();

		for (Map.Entry<FpTermKey, FPDocList> e : common.entrySet()) {
			final BytesRef term = e.getKey().bytesRef();
			final FPDocList srcDocs = e.getValue();
			final int payloadLen = term.length;
			if (payloadLen <= 0) {
				continue;
			}

			mergedNgramKeysThisTerm.clear();
			final int base = term.offset;

			for (int len = NGRAM_MAX; len >= NGRAM_MIN; len--) {
				for (int start = 0; start + len <= payloadLen; start++) {

					final BytesRef slice = new BytesRef(term.bytes, base + start, len);
					final FpTermKey mergeKey = FpTermKey.viewOf(slice);

					final FPDocList doclist = finalHot.get(mergeKey);
					if (doclist != null) {
						if (!mergedNgramKeysThisTerm.add(mergeKey)) {
							stat.hot_doc_cnt_skip++;
							continue;
						}
						stat.hot_doc_cnt_keep++;

						doclist.addAllDocsFrom(srcDocs);
						mark_skip_subField(stat,slice, mergedNgramKeysThisTerm,hotLevel);
					}
				}
			}
		}
	}

	/** 较长 ngram 已合并后，标记其所有真子串 ngram，避免同一段 common 内对子串再 merge 一次。 */
	public static void mark_skip_subField(final FpStatNgram stat,final BytesRef sliceChild, final Set<FpTermKey> mergedNgramKeysThisTerm,final TreeMap<FpTermKey, Integer> hotLevel) {
		final int childlen = sliceChild.length;
		final int childoff = sliceChild.offset;

		for (int len = (sliceChild.length - 1); len >= NGRAM_MIN; len--) {
			for (int start = 0; start + len <= childlen; start++) {
				final BytesRef sliceParent = new BytesRef(sliceChild.bytes, childoff + start, len);
				final FpTermKey parentKey = FpTermKey.viewOf(sliceParent);
				int parentlevel=hotLevel.get(parentKey);
				int depth=childlen-sliceParent.length;
				if(depth<=parentlevel)
				{
					mergedNgramKeysThisTerm.add(parentKey);
					stat.ngram_level_ok++;
				}else {
					stat.ngram_level_skip++;
				}
			}
		}
	}

}
