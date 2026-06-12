package cn.lxdb.plugins.muqingyu.fptoken.pool;

import java.util.HashMap;

/**
 * {@link FpHashMapPoolHub#borrow} 返回的租约：持有池内 {@link HashMap} 与 {@code leaseId}，
 * {@link FpHashMapPoolHub#release(FpHashMapPoolLease)} 凭租约归还，不依赖 map 引用作键。
 */
public final class FpHashMapPoolLease<K, V> {

	private final int poolId;
	private final long leaseId;
	private final HashMap<K, V> map;
	private final long bornAtMs;
	private boolean released;

	FpHashMapPoolLease(int poolId, long leaseId, HashMap<K, V> map, long bornAtMs) {
		this.poolId = poolId;
		this.leaseId = leaseId;
		this.map = map;
		this.bornAtMs = bornAtMs;
	}

	/** 借出的 HashMap；可任意 {@code put}/{@code clear}，不影响租约 lookup。 */
	public HashMap<K, V> map() {
		return map;
	}

	public int poolId() {
		return poolId;
	}

	public long leaseId() {
		return leaseId;
	}

	long bornAtMs() {
		return bornAtMs;
	}

	void ensureReleasable() {
		if (released) {
			throw new IllegalStateException(
					"lease already released: poolId=" + poolId + " leaseId=" + leaseId);
		}
	}

	void markReleased() {
		released = true;
	}
}
