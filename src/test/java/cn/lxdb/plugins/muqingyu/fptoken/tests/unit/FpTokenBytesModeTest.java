package cn.lxdb.plugins.muqingyu.fptoken.tests.unit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

import cn.lxdb.plugins.muqingyu.fptoken.token.FpTokenBytesMode;

class FpTokenBytesModeTest {

	@Test
	void getCode_and_fromCode_roundTrip() {
		assertEquals(1, FpTokenBytesMode.UTF8.getCode());
		assertEquals(2, FpTokenBytesMode.HEX_STRING.getCode());
		assertEquals(FpTokenBytesMode.UTF8, FpTokenBytesMode.fromCode(1));
		assertEquals(FpTokenBytesMode.HEX_STRING, FpTokenBytesMode.fromCode(2));
	}

	@Test
	void fromCode_unknown_throws() {
		assertThrows(IllegalArgumentException.class, () -> FpTokenBytesMode.fromCode(99));
	}
}
