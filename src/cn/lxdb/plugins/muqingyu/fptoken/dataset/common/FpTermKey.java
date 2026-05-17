package cn.lxdb.plugins.muqingyu.fptoken.dataset.common;

import org.apache.lucene.util.BytesRef;

/**
 * 作为 {@code java.util.TreeMap} / {@code java.util.HashMap} 键使用的词字节：内部持有一个
 * {@link BytesRef}，在构造时<strong>一次性</strong>计算并缓存 {@link #hashCode()}，避免在 Map 操作中反复扫描字节。
 * <p>
 * {@link #copyOf(BytesRef)}：拷贝字节，适合长期存放在 Map 中。<br>
 * {@link #viewOf(BytesRef)}：仅持有切片引用，哈希在构造时按当时字节计算；仅适用于在<strong>底层缓冲区不变</strong>
 * 的短生命周期内做 {@code get/containsKey} 等查找，不要作为已存入 Map 的键长期保存。
 */
public final class FpTermKey implements Comparable<FpTermKey> {

	private final BytesRef ref;
	private final int hash;

	private FpTermKey(BytesRef ref, int hash) {
		this.ref = ref;
		this.hash = hash;
	}

	/**
	 * 深拷贝字节为独立 {@link BytesRef}，并缓存哈希；用于 {@link TreeMap#put} 等持久键。
	 */
	public static FpTermKey copyOf(BytesRef src) {
		final BytesRef owned = BytesRef.deepCopyOf(src);
		return new FpTermKey(owned, owned.hashCode());
	}

	/**
	 * 不拷贝底层数组，仅包装当前切片；哈希在构造时按切片内容计算一次。
	 * <p>调用方须保证在用作查找键期间，{@code slice} 指向的缓冲区不被改写。</p>
	 */
	public static FpTermKey viewOf(BytesRef slice) {
		return new FpTermKey(slice, slice.hashCode());
	}
	
	

	/** 与 Lucene {@link BytesRef} 相同的字节视图（可能是拷贝也可能是切片）。 */
	public BytesRef bytesRef() {
		return ref;
	}

	/**
	 * 与 {@link #copyOf(BytesRef)} 相同语义，从已有键再拷贝一份（合并写入新 Map 时使用）。
	 */
	public static FpTermKey copyOf(FpTermKey other) {
		return copyOf(other.ref);
	}

	@Override
	public int hashCode() {
		return hash;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (!(obj instanceof FpTermKey)) {
			return false;
		}
		final FpTermKey o = (FpTermKey) obj;
		return this.ref.equals(o.ref);
	}

	@Override
	public int compareTo(FpTermKey o) {
		return this.ref.compareTo(o.ref);
	}

	@Override
	public String toString() {
		return "FpTermKey{" + ref + ",hash=" + hash + "}";
	}

	
}
