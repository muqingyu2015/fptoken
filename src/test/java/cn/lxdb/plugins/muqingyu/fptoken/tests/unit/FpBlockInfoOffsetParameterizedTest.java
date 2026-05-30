package cn.lxdb.plugins.muqingyu.fptoken.tests.unit;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpBlockInfo;
import cn.lxdb.plugins.muqingyu.fptoken.tests.support.FpParameterizedTestSources;

/** 位图区偏移公式：写 {@link FpBlockInfo#flushto} 布局与读 {@link FpBlockInfo#hotBankOffset} 一致。 */
class FpBlockInfoOffsetParameterizedTest {

	@ParameterizedTest(name = "li={0} bucket={1}")
	@MethodSource("cn.lxdb.plugins.muqingyu.fptoken.tests.support.FpParameterizedTestSources#allNgramLengthIndexAndBucket")
	void hotAndCommonOffsets_matchInterleavedLayout(int li, int bucket) {
		final FpBlockInfo info = new FpBlockInfo();
		info.fpBanksHot = 5000L;
		info.bytesPerHotSerialized = 40;
		info.bytesPerCommonSerialized = 48;
		info.fpBanksCommon = info.fpBanksHot + info.bytesPerHotSerialized;

		final long hot = info.hotBankOffset(li, bucket);
		final long common = info.commonBankOffset(li, bucket);
		final long pair = (long) li * 256L + (long) bucket;
		final long expectedHot = info.fpBanksHot + pair * (40L + 48L);
		assertEquals(expectedHot, hot);
		assertEquals(expectedHot + 40L, common);
	}
}
