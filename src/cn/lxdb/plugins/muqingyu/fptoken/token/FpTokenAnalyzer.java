package cn.lxdb.plugins.muqingyu.fptoken.token;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Analyzer$TokenStreamComponents;



/**
 * FP（指纹）二进制域的 Lucene {@link Analyzer}：产生 {@link FpToken} 分词器，将 Reader 文本转为
 * 滑窗字节词项流。
 */
public class FpTokenAnalyzer extends Analyzer  {

	/** true 表示查询侧构造（与索引侧区分保留，当前分词逻辑相同）。 */
	boolean isQuery;

	/** Reader 文本到字节的编码方式（UTF-8 或十六进制串）。 */
	private final FpTokenBytesMode bytesMode;

	/**
	 * 默认 UTF-8 字节模式。
	 *
	 * @param isQuery 是否为查询分析器
	 */
	public FpTokenAnalyzer(boolean isQuery) {
		this(isQuery, FpTokenBytesMode.UTF8);
	}

	/**
	 * @param isQuery   是否为查询分析器
	 * @param bytesMode 字节模式，null 时退化为 {@link FpTokenBytesMode#UTF8}
	 */
	public FpTokenAnalyzer(boolean isQuery, FpTokenBytesMode bytesMode) {
		this.isQuery = isQuery;
		this.bytesMode = bytesMode == null ? FpTokenBytesMode.UTF8 : bytesMode;
	}

	/**
	 * 通过整型码指定字节模式（与外部 RPC/配置约定一致）。
	 *
	 * @param isQuery        是否为查询分析器
	 * @param bytesModeCode {@link FpTokenBytesMode#getCode()}：1=UTF-8，2=十六进制字符串
	 */
	public FpTokenAnalyzer(boolean isQuery, int bytesModeCode) {
		this(isQuery, FpTokenBytesMode.fromCode(bytesModeCode));
	}

	@Override
	public String toString() {
		return "FpTokenAnalyzer [isQuery=" + isQuery + ", bytesMode=" + bytesMode + "]";
	}

	/**
	 * 构造单一 {@link FpToken} 组成的词项流。
	 *
	 * @param fieldName Lucene 字段名（当前分词器未区分字段）
	 * @return {@link TokenStreamComponents}
	 */
	@Override
	protected Analyzer$TokenStreamComponents createComponents(String fieldName) {
		FpToken toker = new FpToken(this.isQuery, this.bytesMode);
		return new Analyzer$TokenStreamComponents(toker);
	}
}
