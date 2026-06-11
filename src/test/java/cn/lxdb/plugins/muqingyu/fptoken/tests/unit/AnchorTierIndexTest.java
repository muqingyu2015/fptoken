package cn.lxdb.plugins.muqingyu.fptoken.tests.unit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import cn.lxdb.plugins.muqingyu.fptoken.config.Lucene80FPSearchConfig;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.block.AnchorTierIndex;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpTermKey;

import org.apache.lucene.util.BytesRef;

class AnchorTierIndexTest {

	@Test
	void ctor_createsEmptyTierSets() {
		AnchorTierIndex idx = new AnchorTierIndex();
		assertEquals(Lucene80FPSearchConfig.NGRAM_MAX + 1, idx.size());
		for (int i = 0; i <= Lucene80FPSearchConfig.NGRAM_MAX; i++) {
			assertNotNull(idx.get(i));
			assertTrue(idx.get(i).isEmpty());
		}
	}

	@Test
	void tierSet_ordersByFpTermKey() {
		AnchorTierIndex idx = new AnchorTierIndex();
		idx.get(2).add(FpTermKey.copyOf(new BytesRef(new byte[] { 2 })));
		idx.get(2).add(FpTermKey.copyOf(new BytesRef(new byte[] { 1 })));
		assertEquals(2, idx.get(2).size());
		assertEquals(1, idx.get(2).first().bytesRef().bytes[0]);
	}
}
