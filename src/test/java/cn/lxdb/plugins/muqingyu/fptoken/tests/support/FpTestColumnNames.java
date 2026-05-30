package cn.lxdb.plugins.muqingyu.fptoken.tests.support;

import org.apache.lucene.util.BytesRef;

import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpTermKey;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpTokenTermLayout;

/** 单测默认 JSON 列名（UTF-8）。 */
public final class FpTestColumnNames {

	public static final BytesRef DEFAULT = new BytesRef("field");

	private FpTestColumnNames() {
	}

	public static BytesRef forSeed(int seed) {
		return new BytesRef("f" + seed);
	}



}
