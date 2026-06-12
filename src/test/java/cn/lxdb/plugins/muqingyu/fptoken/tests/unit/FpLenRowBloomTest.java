package cn.lxdb.plugins.muqingyu.fptoken.tests.unit;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.lucene.store.RAMOutputStream;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import cn.lxdb.plugins.muqingyu.fptoken.dataset.block.FpLenRowBloom;

@Tag("lxdb-runtime")
class FpLenRowBloomTest {

	@Test
	void build_rejectsAbsentKey() {
		final int[] keys = { 10, 20, 30, 40, 50 };
		final FpLenRowBloom bloom = FpLenRowBloom.build(keys);
		for (int key : keys) {
			assertTrue(bloom.mightContain(key), "member " + key);
		}
		assertFalse(bloom.mightContain(35), "absent key should be filtered");
		assertFalse(bloom.mightContain(999), "absent key should be filtered");
	}

	@Test
	void writeRead_roundTrip() throws Exception {
		final int[] keys = new int[500];
		for (int i = 0; i < keys.length; i++) {
			keys[i] = i * 997 + 13;
		}
		final FpLenRowBloom built = FpLenRowBloom.build(keys);
		final RAMOutputStream out = new RAMOutputStream();
		built.writeTo(out);
		final byte[] bytes = new byte[Math.toIntExact(out.getFilePointer())];
		out.writeTo(bytes, 0);
		final FpLenRowBloom loaded = FpLenRowBloom.readFrom(new org.apache.lucene.store.ByteArrayDataInput(bytes));
		for (int key : keys) {
			assertTrue(loaded.mightContain(key));
		}
		assertFalse(loaded.mightContain(123456789));
	}

	@Test
	void emptyKeys_passthrough() {
		final FpLenRowBloom bloom = FpLenRowBloom.build(new int[0]);
		assertTrue(bloom.mightContain(42));
	}
}
