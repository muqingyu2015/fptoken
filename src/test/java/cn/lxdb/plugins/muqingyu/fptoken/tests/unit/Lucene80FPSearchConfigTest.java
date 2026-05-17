package cn.lxdb.plugins.muqingyu.fptoken.tests.unit;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import cn.lxdb.plugins.muqingyu.fptoken.config.Lucene80FPSearchConfig;

class Lucene80FPSearchConfigTest {

	@Test
	void isFpField_recognizesSuffixes() {
		assertTrue(Lucene80FPSearchConfig.isFpField("payload_bfp"));
		assertTrue(Lucene80FPSearchConfig.isFpField("text_sfp"));
		assertFalse(Lucene80FPSearchConfig.isFpField("payload"));
		assertFalse(Lucene80FPSearchConfig.isFpField(null));
	}
}
