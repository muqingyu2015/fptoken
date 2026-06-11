package cn.lxdb.plugins.muqingyu.fptoken.tests.unit;

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.lucene.util.BytesRef;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpBlockInfo;

@Tag("lxdb-runtime")
class FpBlockInfoTest {

	@Test
	void toString_containsCoreFields() {
		FpBlockInfo info = new FpBlockInfo(10, 20, 100, 200, 8, 16, 3, 5, 2, new BytesRef("col"), 99);
		String s = info.toString();
		assertTrue(s.contains("targetLevel=L2"));
		assertTrue(s.contains("hotTerms=3"));
		assertTrue(s.contains("commonTerms=5"));
		assertTrue(s.contains("docs=99"));
		assertTrue(s.contains("fileOffHot=10"));
	}

	@Test
	void defaultCtor_initializesFieldInfo() {
		FpBlockInfo info = new FpBlockInfo();
		assertTrue(info.toString().contains("field=d"));
	}
}
