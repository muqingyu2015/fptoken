package cn.lxdb.plugins.muqingyu.fptoken.tests.unit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpBlockInfo;

class FpBlockInfoOffsetTest {

	@Test
	void formatVersion_isV3() {
		assertEquals(3, FpBlockInfo.FORMAT_VERSION);
	}

	@Test
	void hotBankOffset_deprecatedInV3() {
		final FpBlockInfo info = new FpBlockInfo();
		try {
			info.hotBankOffset(0, 0);
		} catch (UnsupportedOperationException expected) {
			assertTrue(expected.getMessage().contains("deprecated"));
		}
	}
}
