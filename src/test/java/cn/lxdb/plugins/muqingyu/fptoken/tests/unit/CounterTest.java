package cn.lxdb.plugins.muqingyu.fptoken.tests.unit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

import org.apache.lucene.util.BytesRef;
import org.junit.jupiter.api.Test;

import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpTermKey;
import cn.lxdb.plugins.muqingyu.fptoken.ngram.Counter;

class CounterTest {

	@Test
	void fieldsAccessible() {
		FpTermKey key = FpTermKey.copyOf(new BytesRef("ab"));
		Counter c = new Counter(3, key);
		assertEquals(3, c.cnt);
		assertSame(key, c.key);
		c.cnt = 5;
		assertEquals(5, c.cnt);
	}
}
