package cn.lxdb.plugins.muqingyu.fptoken.dataset.common;

import org.apache.lucene.util.BytesRef;

/**
 * {@link FpTermKey} 专用滚动哈希：Java {@link String#hashCode()} 同款 {@code h = 31*h + byte}。
 * <p>
 * 保证分段/递推与全量扫描一致，例如 {@code hash(abcd) == rollAppend(hash(abc), d)}、
 * {@code hash(abcd) == rollRemoveLast(hash(abcde), e)}、
 * {@code hash(abcd) == rollAppend(hash(ab), cd)} == rollAppend(hash(a), bcd)}。
 */
public final class FpTermKeyHash {

	public static final int MULT = 31;

	/** {@code 31^(len-1)}，{@code len} 为 1..6（{@link cn.lxdb.plugins.muqingyu.fptoken.config.Lucene80FPSearchConfig#NGRAM_MAX}）。 */
	private static final int[] POW31_LEN_MINUS_1 = { 1, 31, 961, 29791, 923521, 28629151 };

	/** {@code 31} 在 mod 2^32 下的乘法逆元。 */
	private static final int INV31 = 0xBDEF7BDF;

	private FpTermKeyHash() {
	}

	public static int hashOf(BytesRef ref) {
		return hashOf(ref.bytes, ref.offset, ref.length);
	}

	public static int hashOf(byte[] bytes, int offset, int length) {
		int h = 0;
		for (int i = 0; i < length; i++) {
			h = rollAppend(h, bytes[offset + i]);
		}
		return h;
	}

	/** 在已有哈希末尾追加一字节。 */
	public static int rollAppend(int hash, byte appended) {
		return MULT * hash + (appended & 0xff);
	}

	/** 在已有哈希末尾追加一段字节。 */
	public static int rollAppend(int hash, byte[] bytes, int offset, int length) {
		for (int i = 0; i < length; i++) {
			hash = rollAppend(hash, bytes[offset + i]);
		}
		return hash;
	}

	/** 去掉切片最后一个字节。 */
	public static int rollRemoveLast(int hash, byte removedLast) {
		return (hash - (removedLast & 0xff)) * INV31;
	}

	/** 从末尾依次去掉 {@code removeCount} 个字节（{@code bytes[endOffset-removeCount .. endOffset-1]}）。 */
	public static int rollRemoveSuffix(int hash, byte[] bytes, int endOffset, int removeCount) {
		for (int i = removeCount - 1; i >= 0; i--) {
			hash = rollRemoveLast(hash, bytes[endOffset - removeCount + i]);
		}
		return hash;
	}

	/**
	 * 固定长度滑窗起点 +1：{@code [off+1 .. off+len]} 由 {@code [off .. off+len-1]} 递推。
	 */
	public static int rollStartForward(int hash, byte[] bytes, int off, int len) {
		final int removedFirst = bytes[off] & 0xff;
		final int addedLast = bytes[off + len] & 0xff;
		return MULT * (hash - removedFirst * pow31(len - 1)) + addedLast;
	}

	static int pow31(int exponent) {
		return POW31_LEN_MINUS_1[exponent];
	}
}
