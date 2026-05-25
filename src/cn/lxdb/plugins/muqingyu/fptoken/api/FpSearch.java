package cn.lxdb.plugins.muqingyu.fptoken.api;

import org.apache.lucene.index.Terms;
import org.apache.lucene.util.FixedBitSet;

public class FpSearch {
	public FixedBitSet search(Terms terms,String keyword)
	{

		w bitset=terms.fpBits(0, 0, null, null)
		return null;
	}
}
