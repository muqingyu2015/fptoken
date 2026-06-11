package cn.lxdb.plugins.muqingyu.fptoken.tests.unit;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.lang.reflect.Method;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import cn.lxdb.plugins.muqingyu.fptoken.dataset.block.FpBitIndexTempDirectory;

@Tag("lxdb-runtime")
class FpBitIndexTempDirectorySanitizeTest {

	@Test
	void sanitize_replacesInvalidChars() throws Exception {
		Method m = FpBitIndexTempDirectory.class.getDeclaredMethod("sanitize", String.class);
		m.setAccessible(true);
		assertEquals("field", m.invoke(null, (Object) null));
		assertEquals("field", m.invoke(null, ""));
		assertEquals("content_bfp", m.invoke(null, "content_bfp"));
		assertEquals("a_b_c", m.invoke(null, "a/b.c"));
		assertEquals("hello_world", m.invoke(null, "hello world!"));
	}
}
