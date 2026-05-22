package cn.lxdb.plugins.muqingyu.fptoken.dataset.common;

/**
 * 热词前缀层级：写段剔除子 term doc 与查询拼回的阈值。
 * <p>
 * 「档」= 同一父前缀（如 {@code ab}）下、热词<strong>字节长度相同</strong>的一层（len=3 是 ab* 档，len=4 是 ab** 档），
 * 各档个数<strong>分开数</strong>，不做 ab*+ab**+ab*** 累加。
 * 某档热词个数 &lt; {@link #HOT_TIER_TERM_COUNT_THRESHOLD} 时查询会拼回该档 doc，写段可从父热词剔除该档子 term；
 * 更深（&gt;{@link #MAX_RELATIVE_BYTE_LEVEL_STRIP_FROM_PARENT} 层）或该档 ≥32 时不拼回、不剔除。
 */
public final class FpHotTermHierarchyConstants {

	/** 热词频率挖掘阈值（{@link cn.lxdb.plugins.muqingyu.fptoken.dataset.block.FpGroupHotNgramRebuild}）及「按档计数」上限。 */
	public static final int HOT_TIER_TERM_COUNT_THRESHOLD = 32;


}
