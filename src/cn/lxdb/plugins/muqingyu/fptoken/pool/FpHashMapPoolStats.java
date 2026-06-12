package cn.lxdb.plugins.muqingyu.fptoken.pool;

/**
 * 对象池槽位运行统计（只读快照）。
 */
public final class FpHashMapPoolStats {

	private final int poolId;
	private final int idleSize;
	private final int leasedSize;
	private final long borrowCount;
	private final long releaseCount;
	private final long createdCount;
	private final long expiredReuseCount;
	private final long expiredIdleEvictCount;

	FpHashMapPoolStats(int poolId, int idleSize, int leasedSize, long borrowCount, long releaseCount,
			long createdCount, long expiredReuseCount, long expiredIdleEvictCount) {
		this.poolId = poolId;
		this.idleSize = idleSize;
		this.leasedSize = leasedSize;
		this.borrowCount = borrowCount;
		this.releaseCount = releaseCount;
		this.createdCount = createdCount;
		this.expiredReuseCount = expiredReuseCount;
		this.expiredIdleEvictCount = expiredIdleEvictCount;
	}

	public int poolId() {
		return poolId;
	}

	public int idleSize() {
		return idleSize;
	}

	public int leasedSize() {
		return leasedSize;
	}

	public long borrowCount() {
		return borrowCount;
	}

	public long releaseCount() {
		return releaseCount;
	}

	public long createdCount() {
		return createdCount;
	}

	public long expiredReuseCount() {
		return expiredReuseCount;
	}

	public long expiredIdleEvictCount() {
		return expiredIdleEvictCount;
	}

	@Override
	public String toString() {
		return "FpHashMapPoolStats{poolId=" + poolId + ", idle=" + idleSize + ", leased=" + leasedSize + ", borrow="
				+ borrowCount + ", release=" + releaseCount + ", created=" + createdCount + ", expiredReuse="
				+ expiredReuseCount + ", expiredIdleEvict=" + expiredIdleEvictCount + '}';
	}
}
