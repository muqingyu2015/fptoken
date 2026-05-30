package cn.lxdb.plugins.muqingyu.fptoken.tests.unit;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RAMDirectory;
import org.junit.jupiter.api.Test;

import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpBlockInfo;

/** {@link FpBlockInfo} meta 写读与偏移公式。 */
class FpBlockInfoSerializationTest {

	@Test
	void writeto_readfrom_roundTrip() throws Exception {
		final FpBlockInfo expected = new FpBlockInfo();
		expected.fpBanksHot = 1000L;
		expected.fpBanksCommon = 1040L;
		expected.bytesPerHotSerialized = 40;
		expected.bytesPerCommonSerialized = 48;
		expected.hotNumBits = 5;
		expected.commonNumBits = 6;
		expected.hotCount = 2;
		expected.commonCount = 3;
		expected.targetLevel = 2;

		final Directory dir = new RAMDirectory();
		try (IndexOutput out = dir.createOutput("meta", IOContext.DEFAULT)) {
			expected.writeto(out);
		}
		try (IndexInput in = dir.openInput("meta", IOContext.DEFAULT)) {
			final FpBlockInfo actual = new FpBlockInfo();
			actual.readfrom(in);
			assertEquals(expected.fpBanksHot, actual.fpBanksHot);
			assertEquals(expected.fpBanksCommon, actual.fpBanksCommon);
			assertEquals(expected.bytesPerHotSerialized, actual.bytesPerHotSerialized);
			assertEquals(expected.bytesPerCommonSerialized, actual.bytesPerCommonSerialized);
			assertEquals(expected.hotNumBits, actual.hotNumBits);
			assertEquals(expected.commonNumBits, actual.commonNumBits);
			assertEquals(expected.hotCount, actual.hotCount);
			assertEquals(expected.commonCount, actual.commonCount);
			assertEquals(expected.targetLevel, actual.targetLevel);
		}
	}
}
