package cn.lxdb.plugins.muqingyu.fptoken.tests.unit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpCommonAccumDiag;

@Tag("lxdb-runtime")
class FpCommonAccumDiagTest {

	private FpCommonAccumDiag diag;

	@BeforeEach
	void setUp() {
		diag = new FpCommonAccumDiag();
	}

	@Test
	void recordIngest_tracksGroupZeroAndNonZero() {
		diag.recordIngest(0);
		diag.recordIngest(0);
		diag.recordIngest(3);
		diag.recordIngest(3);
		diag.recordIngest(5);
		assertEquals(2, diag.ingestGroupZero());
		assertEquals(3, diag.ingestGroupNonZero());
	}

	@Test
	void recordMergeBatch_accumulates() {
		diag.recordMergeBatch(1, 10, 20);
		diag.recordMergeBatch(1, 5, 1);
		StringBuilder sb = new StringBuilder();
		diag.appendFields(sb);
		assertTrue(sb.toString().contains("ingestGroupZero=0"));
		assertTrue(sb.toString().contains("mergeFromGroup=1{hot=15,common=21}"));
	}

	@Test
	void shouldLogNow_respectsThresholdAndStep() {
		assertFalse(diag.shouldLogNow(100, 200, 50));
		assertTrue(diag.shouldLogNow(250, 200, 50));
		diag.markLogged(250);
		assertFalse(diag.shouldLogNow(280, 200, 50));
		assertTrue(diag.shouldLogNow(300, 200, 50));
	}

	@Test
	void reset_clearsState() {
		diag.recordIngest(1);
		diag.recordMergeBatch(2, 1, 1);
		diag.markLogged(500);
		diag.reset();
		assertEquals(0, diag.ingestGroupZero());
		assertEquals(0, diag.ingestGroupNonZero());
		assertTrue(diag.shouldLogNow(500, 200, 50));
	}

	@Test
	void appendFields_includesIngestBySourceGroup() {
		diag.recordIngest(3);
		diag.recordIngest(3);
		diag.recordIngest(7);
		StringBuilder sb = new StringBuilder();
		diag.appendFields(sb);
		assertTrue(sb.toString().contains("ingestBySourceGroup=3:2,7:1"));
	}
}
