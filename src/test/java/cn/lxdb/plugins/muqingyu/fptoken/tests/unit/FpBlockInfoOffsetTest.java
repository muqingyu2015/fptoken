package cn.lxdb.plugins.muqingyu.fptoken.tests.unit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpBlockInfo;

@Tag("lxdb-runtime")
class FpBlockInfoOffsetTest {

	@Test
	void formatVersion_isV7() {
		assertEquals(7, FpBlockInfo.FORMAT_VERSION);
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

	@Test
	void commonBankOffset_deprecatedInV3() {
		final FpBlockInfo info = new FpBlockInfo();
		try {
			info.commonBankOffset(1, 2);
		} catch (UnsupportedOperationException expected) {
			assertTrue(expected.getMessage().contains("deprecated"));
		}
	}
}
