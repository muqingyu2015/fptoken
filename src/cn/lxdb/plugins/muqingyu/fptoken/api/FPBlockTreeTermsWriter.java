package cn.lxdb.plugins.muqingyu.fptoken.api;

import org.apache.lucene.codecs.blocktree.BlockTreeTermsWriter;
import org.apache.lucene.store.IndexOutput;

//为了兼容旧API 已经不推荐使用了
public class FPBlockTreeTermsWriter extends FpTermsWriter{

	public FPBlockTreeTermsWriter(BlockTreeTermsWriter w, IndexOutput bitOut) {
		super(w, bitOut);
	}

}
