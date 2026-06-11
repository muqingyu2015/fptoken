package cn.lxdb.plugins.muqingyu.fptoken.tests.unit;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.lucene.util.BytesRef;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.Utils;

@Tag("lxdb-runtime")
class UtilsTest {

	@Test
	void bytesReftoString_nullAndEmpty() {
		assertEquals("null", Utils.BytesReftoString(null));
		assertEquals("", Utils.BytesReftoString(new BytesRef()));
	}

	@Test
	void bytesReftoString_utf8() {
		assertEquals("hello", Utils.BytesReftoString(new BytesRef("hello".getBytes())));
	}

	@Test
	void bytesReftoString_invalidUtf8_fallsBackToHex() {
		byte[] invalid = { (byte) 0xFF, (byte) 0xFE };
		BytesRef ref = new BytesRef(invalid);
		assertEquals(ref.toHexString(), Utils.BytesReftoString(ref));
	}
}
