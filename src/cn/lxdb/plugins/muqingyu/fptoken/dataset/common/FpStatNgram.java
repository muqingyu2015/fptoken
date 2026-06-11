package cn.lxdb.plugins.muqingyu.fptoken.dataset.common;

import java.util.Arrays;

import cn.lxdb.plugins.muqingyu.fptoken.config.Lucene80FPSearchConfig;

public class FpStatNgram {

	public long term_cnt=0;
	public long token_cnt=0;
	/** {@code countNgramOccurrencesInCommon} 后 {@code ngramOccurrenceCount.size()} */
	public int distinct_ngram=0;
	/** {@code buildHotTerms} 后待 merge 热词数 */
	public int hot_pending=0;
	/** {@code putAll} 后进 {@code hotTermToDocs} 的热词数 */
	public int hot_final=0;
	/** {@code hotTermDownTierBudget} 条数 */
	public int budget_entries=0;
	public long ms_count=0;
	public long ms_build=0;
	public long ms_budget=0;
	public long ms_merge=0;

	public long freqThreshold_keep=0;
	public long freqThreshold_skip=0;
	
	public long hot_doc_cnt_keep=0;
	public long hot_doc_cnt_skip=0;
	/** merge 结束后以 {@link org.apache.lucene.util.SparseFixedBitSet} 存放 doc 的热词条数 */
	public int hot_doclist_sparse=0;

	public long ngram_level_ok=0;
	public long ngram_level_skip=0;
	public long[][] term_level_cnt=mkLevel();
	/**
	 * common 按 hot 命中次数每 {@link Lucene80FPSearchConfig#COMMON_HOT_HIT_TIER_BAND_SIZE} 一组分档计数。
	 * 下标 {@code 0} 为 0 命中；{@code 1..maxTier} 为 1..max 窗口命中。
	 */
	public long[] commonHotHitTierCnt = mkCommonHotHitTierCnt();

	/** 0 次命中 hot（写序最后） */
	public static final int HOT_HIT_TIER_ZERO = 0;

	public static int hotHitTierCount() {
		return hotHitMaxTierIndex() + 1;
	}

	private static int hotHitMaxTierIndex() {
		final int max = Lucene80FPSearchConfig.commonHotHitMaxWindows();
		final int band = Lucene80FPSearchConfig.COMMON_HOT_HIT_TIER_BAND_SIZE;
		return (max + band - 1) / band;
	}

	@Override
	public String toString() {
		return "terms=" + term_cnt
				+ " tokens=" + token_cnt
				+ " distinctNgram=" + distinct_ngram
				+ " hotPending=" + hot_pending
				+ " hotFinal=" + hot_final
				+ " budgetEntries=" + budget_entries
				+ " freqKeep<=" + freqThreshold_keep
				+ " freqSkip>=" + freqThreshold_skip
				+ " hotDocsKeep=" + hot_doc_cnt_keep
				+ " hotDocsSkip=" + hot_doc_cnt_skip
				+ " sparseDoclist=" + hot_doclist_sparse + "/" + hot_pending
				+ " ngramLevelOk=" + ngram_level_ok
				+ " ngramLevelSkip=" + ngram_level_skip
				+ " phasesMs=" + ms_count + "+" + ms_build + "+" + ms_budget + "+" + ms_merge
				+ " termByLenLevel=" + getLevelString()
				+ " commonHotHitMax=" + Lucene80FPSearchConfig.commonHotHitMaxWindows()
				+ " commonHotHitTier=" + getCommonHotHitTierString();
	}

	/** 将 common 载荷内 hot 命中次数映射到日志分档下标（每 50 一组，超过 max 归入最高档）。 */
	public static int hotHitCountToTier(int hotHitCount) {
		if (hotHitCount <= 0) {
			return HOT_HIT_TIER_ZERO;
		}
		final int band = Lucene80FPSearchConfig.COMMON_HOT_HIT_TIER_BAND_SIZE;
		int tier = (hotHitCount - 1) / band + 1;
		if (tier > hotHitMaxTierIndex()) {
			tier = hotHitMaxTierIndex();
		}
		return tier;
	}

	/** 分档 {@code tier} 的命中次数闭区间 [lo, hi]（{@code tier=0} 为 0）。 */
	public static int hotHitTierLo(int tier) {
		if (tier <= HOT_HIT_TIER_ZERO) {
			return 0;
		}
		final int band = Lucene80FPSearchConfig.COMMON_HOT_HIT_TIER_BAND_SIZE;
		return (tier - 1) * band + 1;
	}

	public static int hotHitTierHi(int tier) {
		if (tier <= HOT_HIT_TIER_ZERO) {
			return 0;
		}
		final int band = Lucene80FPSearchConfig.COMMON_HOT_HIT_TIER_BAND_SIZE;
		return Math.min(tier * band, Lucene80FPSearchConfig.commonHotHitMaxWindows());
	}

	public static String hotHitTierLabel(int tier) {
		if (tier <= HOT_HIT_TIER_ZERO) {
			return "hit0";
		}
		return "hit" + hotHitTierLo(tier) + '_' + hotHitTierHi(tier);
	}

	public String getCommonHotHitTierString() {
		final StringBuilder buff = new StringBuilder();
		for (int tier = 0; tier < hotHitTierCount(); tier++) {
			if (commonHotHitTierCnt[tier] > 0) {
				if (buff.length() > 0) {
					buff.append(',');
				}
				buff.append(hotHitTierLabel(tier)).append('=').append(commonHotHitTierCnt[tier]);
			}
		}
		return buff.toString();
	}

	public String getLevelString()
	{
		StringBuffer buff=new StringBuffer();
		for(int i=0;i<term_level_cnt.length;i++)
		{
			for(int j=0;j<term_level_cnt[i].length;j++)
			{
				if(term_level_cnt[i][j]>0)
				{
					buff.append("len").append(i).append("@L").append(j).append('=').append(term_level_cnt[i][j]).append(',');
				}
			}
		}
		if (buff.length() > 0) {
			buff.setLength(buff.length() - 1);
		}
		return buff.toString();
	}
	
	public static long[][] mkLevel()
	{
		long[][] term_level_cnt=new long[Lucene80FPSearchConfig.NGRAM_MAX*2][Lucene80FPSearchConfig.NGRAM_MAX*2];
		for(int i=0;i<term_level_cnt.length;i++)
		{
			Arrays.fill(term_level_cnt[i], 0);
		}
		
		return term_level_cnt;
		
	}

	public static long[] mkCommonHotHitTierCnt() {
		return new long[hotHitTierCount()];
	}

}
