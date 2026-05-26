package cn.lxdb.plugins.muqingyu.fptoken.tests.support;

/**
 * 单组 rebuild 测试索引：写段产物 + 查询 payload + 期望命中的 doc 列表。
 */
public final class FpRebuildSeedIndex {

	public final FpWriteReadTestIndex.Built built;
	public final byte[] payload;
	public final int[] expectedDocs;

	public FpRebuildSeedIndex(FpWriteReadTestIndex.Built built, byte[] payload, int[] expectedDocs) {
		this.built = built;
		this.payload = payload;
		this.expectedDocs = expectedDocs;
	}
}
