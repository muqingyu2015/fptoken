package cn.lxdb.plugins.muqingyu.fptoken.tests.unit;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

import cn.lxdb.plugins.muqingyu.fptoken.config.Lucene80FPSearchConfig;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpBlockInfo;

class FpBlockInfoOffsetTest {

	@Test
	void bankOffsets_followInterleavedLayout() {
		FpBlockInfo info = new FpBlockInfo();
		info.fpBanksHot = 1000L;
		info.bytesPerHotSerialized = 40;
		info.bytesPerCommonSerialized = 48;
		info.fpBanksCommon = info.fpBanksHot + info.bytesPerHotSerialized;
		final long pair = Lucene80FPSearchConfig.bankPairIndex(2, 3);
		assertEquals(256L + 512L + 3L, pair);
		assertEquals(1000L + pair * (40L + 48L), info.hotBankOffset(2, 3));
		assertEquals(info.hotBankOffset(2, 3) + 40L, info.commonBankOffset(2, 3));
		assertEquals(1040L, info.fpBanksCommon);
	}
}
