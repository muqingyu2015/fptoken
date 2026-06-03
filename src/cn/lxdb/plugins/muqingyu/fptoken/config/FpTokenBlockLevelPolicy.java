package cn.lxdb.plugins.muqingyu.fptoken.config;

import cn.lxdb.plugins.muqingyu.fptoken.api.FpTokenBlockOrchestrator;

/**
 * FP token 块级别与「闭块」判定策略：目标级别、阈值与是否应写出均可集中在此修改。
 * <p>
 * 级别语义（与词项第 7 字节一致）：0 表示未参与合并语义（编排外）；1 千级；2 万级；3 十万级以上规模。
 */
public final class FpTokenBlockLevelPolicy {
	
	

	public final static int BLOCK_LEVEL_HIGH=3;
	public final static int BLOCK_LEVEL_MID=2;
	public final static int BLOCK_LEVEL_LOW=1;
	public final static int BLOCK_LEVEL_NOGROUP=0;
	
	public final static int BLOCK_LEVEL_HIGH_CNT=102400;
	public final static int BLOCK_LEVEL_MID_CNT=10240;
	public final static int BLOCK_LEVEL_LOW_CNT=1024; 
	public static final int NO_INDEX_THRESHOLD = 128;
	public static final int REBUID_OVER_RATE = 3;

	private FpTokenBlockLevelPolicy() {
	}

	/**
	 * 根据段规模与词项规模估计，决定本轮「可合并小词项」要凑齐的目标级别（1~3）。
	 * <p>
	 * 词项上级别大于等于该目标时，由 {@link FpTokenBlockOrchestrator} 走透传/高候选路径，不参与低级别合并链。
	 *
	 * @param maxDoc    段内最大 doc id + 1 语义下的文档数上界
	 * @param term_size 词项数估计（如 {@code Terms#guess_size()}）
	 * @return 目标块级别 1、2 或 3
	 */
	public static int resolveTargetBlockLevel(int maxDoc, long term_size) {
		// 取文档与词项规模的较大者，使大段或大词表都倾向更高闭块阈值
		long check_size = Math.max(maxDoc, term_size);
		if (check_size >= BLOCK_LEVEL_HIGH_CNT) {
			return BLOCK_LEVEL_HIGH;
		}
		if (check_size >= BLOCK_LEVEL_MID_CNT) {
			return BLOCK_LEVEL_MID;
		}
		return BLOCK_LEVEL_LOW;
	}

	/**
	 * 某级别在统计意义上期望覆盖的「去重文档数」下限，用于退化检查与闭块判定的一侧条件。
	 *
	 * @param level 块级别 1~3
	 * @return 文档数阈值；非法 level 返回 0
	 */
	private static int minDistinctDocsForLevel(int level) {
		if(BLOCK_LEVEL_HIGH==level)
		{
			return BLOCK_LEVEL_HIGH_CNT;
		}
		if(BLOCK_LEVEL_MID==level)
		{
			return BLOCK_LEVEL_MID_CNT;
		}
		return BLOCK_LEVEL_LOW_CNT;

	}

	/**
	 * 某级别在统计意义上期望覆盖的「不同词项数」下限；当前实现与文档阈值相同，可独立调整。
	 *
	 * @param level 块级别 1~3
	 * @return 词项数阈值
	 */
	private static int minDistinctTermsForLevel(int level) {
		return minDistinctDocsForLevel(level);
	}

	/**
	 * 是否满足闭块条件：去重文档数或不同词项数<strong>其一</strong>达到当前判定级别对应阈值即可。
	 *
	 * @param targetLevel       用于查表的级别（通常为编排目标或组内最大级别）
	 * @param distinctDocCount  组内去重后的文档数
	 * @param distinctTermCount 组内不同词项数
	 * @return true 表示应将该组写出（或高候选路径下视为体量达标）
	 */
	public static boolean shouldCompleteBlock(double rate,int targetLevel, int distinctDocCount, int distinctTermCount) {
		final int docTh = minDistinctDocsForLevel(targetLevel);
		final int termTh = minDistinctTermsForLevel(targetLevel);
		return distinctDocCount >= (docTh*rate) || distinctTermCount >= (termTh*rate);
	}

	
}
