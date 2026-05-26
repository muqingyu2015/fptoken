package cn.lxdb.plugins.muqingyu.fptoken.tests.unit;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import cn.lxdb.plugins.muqingyu.fptoken.dataset.block.FpGroupHotNgramBitIndex;

/** 写段标记与查询选桶共用 {@link FpGroupHotNgramBitIndex#bucketIndex}。 */
class FpGroupHotNgramBitIndexBucketIndexParameterizedTest {

	@ParameterizedTest(name = "byte={0}")
	@MethodSource("cn.lxdb.plugins.muqingyu.fptoken.tests.support.FpParameterizedTestSources#allUnsignedBytes")
	void singleByteBucket_isUnsignedValue(int unsignedByte) {
		final byte[] buf = { (byte) unsignedByte };
		assertEquals(unsignedByte, FpGroupHotNgramBitIndex.bucketIndex(buf, 0, 1));
	}

	@ParameterizedTest(name = "lead={0}")
	@MethodSource("cn.lxdb.plugins.muqingyu.fptoken.tests.support.FpParameterizedTestSources#allUnsignedBytes")
	void twoByteBucket_isDeterministic(int lead) {
		final byte[] buf = { (byte) lead, 7 };
		final int a = FpGroupHotNgramBitIndex.bucketIndex(buf, 0, 2);
		final int b = FpGroupHotNgramBitIndex.bucketIndex(buf, 0, 2);
		assertEquals(a, b);
		assertEquals(a & 0xFF, a);
	}
}
