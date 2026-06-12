package cn.lxdb.plugins.muqingyu.fptoken.pool;

/**
 * 预定义的 HashMap 对象池槽位编号。
 * <p>
 * 静态块中为常用池注册类型；之后可直接 {@code borrow(poolId)} / {@code release(poolId, map)}。
 * 未在此定义的 id，首次使用传类型即可自动注册。
 */
public final class FpHashMapPoolIds {
	
	public static final int NGRAM_OCCURRENCE = 1;

	/** {@code HashMap<FpTermKey, Counter>} — ngram 出现次数统计 */
	public static final int ngramOccurrenceCount = 2;
	public static final int anchorTierIndexByHotTerm = 3;
	public static final int hotTermDownTierBudget = 4;

	public static final int hotTermToDocs = 5;
	public static final int commonTermToDocs = 6;
	public static final int hotTermsPendingDocMerge = 7;

	public static final int hotMergeTable = 8;

	/** {@code HashMap<FpTermKey, Boolean>} — common payload 内 ngram slice 去重（bitindex 构建） */
	public static final int commonPayloadUniqueSlices = 9;

	/** {@code HashMap<FpTermKey, Boolean>} — countNgram 阶段 common term 内 ngram 去重 */
	public static final int commonTermUniqueNgrams = 10;
	/** 自定义池从 100 起，避免与内置 id 冲突 */
	public static final int CUSTOM_BASE = 100;



	private FpHashMapPoolIds() {
	}
}
