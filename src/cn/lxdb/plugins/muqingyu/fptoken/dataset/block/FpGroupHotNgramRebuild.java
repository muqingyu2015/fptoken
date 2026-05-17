package cn.lxdb.plugins.muqingyu.fptoken.dataset.block;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.lucene.util.BytesRef;

import cn.lxdb.plugins.muqingyu.fptoken.api.FpTokenBlockOrchestrator;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FPDocList;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpTermKey;

/**
 * 从 {@link FpGroupDataRebuild#commonTermMapInternal()} 的载荷按 byte ngram(1~8) 挖掘热词；Map 键均为 {@link FpTermKey}。
 * <p>两处 ngram 扫描均在<strong>单个 common 词项内</strong>对相同字节序列去重。</p>
 */
public final class FpGroupHotNgramRebuild {

	private static final int NGRAM_MIN = 1;
	private static final int NGRAM_MAX = 8;

	private FpGroupHotNgramRebuild() {
	}

	/**
	 * 根据频率计算热词
	 */
	public static void execute(FpGroupDataRebuild group, FpTokenBlockOrchestrator parentItem, final long freqThreshold) throws IOException {
		final TreeMap<FpTermKey, FPDocList> hot = group.hotTermMapInternal();
		final TreeMap<FpTermKey, FPDocList> common = group.commonTermMapInternal();
		final int maxDoc = group.maxDocInternal();

		final HashMap<FpTermKey, Integer> newhotFreq = new HashMap<>(Math.max(common.size()/100, 32));
		//这个计算量很大
		countNewHotNgramsFromCommon(common, hot, newhotFreq);

		//构建最终的的热词表
		final TreeMap<FpTermKey, FPDocList> finalHot = buildFinalHotTerms(hot, newhotFreq, freqThreshold, maxDoc);

		//构建热词的doclist
		mergeCommonDocsIntoFinalHot(common, finalHot);

		//替换热词表
		hot.clear();
		hot.putAll(finalHot);
	}

	/**
	 * 按照ngram切割统计每个长度的频率
	 */
	private static void countNewHotNgramsFromCommon(TreeMap<FpTermKey, FPDocList> common, TreeMap<FpTermKey, FPDocList> hot,
			HashMap<FpTermKey, Integer> newhotFreq) {
		final Set<FpTermKey> uniqueNgramsThisTerm = new HashSet<>();

		for (Map.Entry<FpTermKey, FPDocList> e : common.entrySet()) {
			final BytesRef term = e.getKey().bytesRef();
			final int payloadLen = term.length;
			if (payloadLen <= 0) {
				continue;
			}

			uniqueNgramsThisTerm.clear();
			final int base = term.offset;
			for (int start = 0; start < payloadLen; start++) {
				for (int n = NGRAM_MIN; n <= NGRAM_MAX && start + n <= payloadLen; n++) {
					final BytesRef slice = new BytesRef(term.bytes, base + start, n);
					final FpTermKey key=FpTermKey.viewOf(slice);
					if (hot.containsKey(key)) {
						continue;
					}
					if (!uniqueNgramsThisTerm.add(key)) {
						continue;
					}
					newhotFreq.merge(key, 1, Integer::sum);
				}
			}
		}
	}

	private static TreeMap<FpTermKey, FPDocList> buildFinalHotTerms(TreeMap<FpTermKey, FPDocList> hot,
			HashMap<FpTermKey, Integer> newhotFreq, long freqThreshold, int maxDoc) {
		final TreeMap<FpTermKey, FPDocList> out = new TreeMap<>();
		//将旧热词放进去
		for (Map.Entry<FpTermKey, FPDocList> e : hot.entrySet()) {
			out.put(e.getKey(), new FPDocList(maxDoc));
		}
		//新热词也放进去
		for (Map.Entry<FpTermKey, Integer> e : newhotFreq.entrySet()) {
			if (e.getValue() < freqThreshold) {
				continue;
			}
			final FpTermKey k = e.getKey();
			if (!out.containsKey(k)) {
				out.put(k, new FPDocList(maxDoc));
			}
		}
		return out;
	}

	private static void mergeCommonDocsIntoFinalHot(TreeMap<FpTermKey, FPDocList> common, TreeMap<FpTermKey, FPDocList> finalHot)
			throws IOException {
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
					final FpTermKey mergeKey=FpTermKey.viewOf(slice);

					final FPDocList tgt = finalHot.get(mergeKey);
					if (tgt != null) {
//						final FpTermKey mergeKey = FpTermKey.copyOf(slice);
						if (!mergedNgramKeysThisTerm.add(mergeKey)) {
							continue;
						}
						tgt.addAllDocsFrom(srcDocs);
						mark_skip_subField(slice, mergedNgramKeysThisTerm);
					}
				}
			}
		}
	}
	
	
	
	public static void mark_skip_subField(final BytesRef term,final Set<FpTermKey> mergedNgramKeysThisTerm)
	{
		final int payloadLen = term.length;

		final int base = term.offset;

		for (int len = (term.length-1); len >= NGRAM_MIN; len--) {
			for (int start = 0; start + len <= payloadLen; start++) {
				
				final BytesRef slice = new BytesRef(term.bytes, base + start, len);
				final FpTermKey mergeKey=FpTermKey.viewOf(slice);

				if (!mergedNgramKeysThisTerm.add(mergeKey)) {
					continue;
				}
			}
		}
	
	}


}
