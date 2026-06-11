package cn.lxdb.plugins.muqingyu.fptoken.dataset.block;

import java.util.ArrayList;
import java.util.TreeSet;

import cn.lxdb.plugins.muqingyu.fptoken.config.Lucene80FPSearchConfig;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpTermKey;

/** 锚点分档索引：{@code termsByByteLength.get(byteLen)} 为该字节长度档内的去重热词/子串（有上限）。 */
public final class AnchorTierIndex extends ArrayList<TreeSet<FpTermKey>> {
	private static final long serialVersionUID = 1L;

	public AnchorTierIndex() {
		super(Lucene80FPSearchConfig.NGRAM_MAX + 1);
		for (int byteLen = 0; byteLen <= Lucene80FPSearchConfig.NGRAM_MAX; byteLen++) {
			add(new TreeSet<>());
		}
	}
}