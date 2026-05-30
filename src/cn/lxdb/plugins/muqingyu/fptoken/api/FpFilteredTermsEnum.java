package cn.lxdb.plugins.muqingyu.fptoken.api;

import java.io.IOException;

import org.apache.lucene.index.ImpactsEnum;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.TermState;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.index.TermsEnum$SeekStatus;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;

import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpTokenTermLayout;

/**
 * 在 {@link TermsEnum#next()} 返回的词项前部注入 2 字节 sortable 短整型前缀，使不同逻辑索引流合并时
 * 可通过字节序区分来源分片。
 * <p>
 * 其余 {@link TermsEnum} 行为直接委托内层迭代器。
 */
public class FpFilteredTermsEnum extends TermsEnum {

	private static final long serialVersionUID = 8953898047186137965L;

	/** 被包装的底层词项枚举。 */
	private final TermsEnum it;


	/** 与 {@link #index} 对应的 sortable 2 字节前缀，写入每个 {@link #next()} 返回的 {@link BytesRef}。 */
	private final int index_id ;

	/**
	 * @param it    底层 {@link TermsEnum}
	 * @param index 写入词项前两字节的短整型标识（sortable 编码）
	 */
	public FpFilteredTermsEnum(TermsEnum it, int index) {
		this.it = it;
		this.index_id=index;
	}

	@Override
	public BytesRef next() throws IOException {
		BytesRef rtn = this.it.next();
		if (rtn == null) {
			return null;
		}
		
		FpTokenTermLayout.modify_index_id(rtn, this.index_id);
		return rtn;
	}

	@Override
	public AttributeSource attributes() {
		return this.it.attributes();
	}

	@Override
	public boolean seekExact(BytesRef text) throws IOException {
		return this.it.seekExact(text);
	}

	@Override
	public TermsEnum$SeekStatus seekCeil(BytesRef text) throws IOException {
		return this.it.seekCeil(text);
	}

	@Override
	public void seekExact(long ord) throws IOException {
		this.it.seekExact(ord);
	}

	@Override
	public void seekExact(BytesRef term, TermState state) throws IOException {
		this.it.seekExact(term, state);
	}

	@Override
	public BytesRef term() throws IOException {
		return this.it.term();
	}

	@Override
	public long ord() throws IOException {
		return this.it.ord();
	}

	@Override
	public int docFreq() throws IOException {
		return this.it.docFreq();
	}

	@Override
	public long totalTermFreq() throws IOException {
		return this.it.totalTermFreq();
	}

	@Override
	public PostingsEnum postings(PostingsEnum reuse, int flags) throws IOException {
		return this.it.postings(reuse, flags);
	}

	@Override
	public ImpactsEnum impacts(int flags) throws IOException {
		return this.it.impacts(flags);
	}

	@Override
	public TermState termState() throws IOException {
		return this.it.termState();
	}
}
