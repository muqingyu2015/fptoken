package cn.lxdb.plugins.muqingyu.fptoken.tests.unit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.lucene.util.BytesRef;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.helpers.NOPLogger;

import cn.lxdb.plugins.muqingyu.fptoken.config.Lucene80FPSearchConfig;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpLog;

@Tag("lxdb-runtime")
class FpLogTest {

	private static final Logger NOP = NOPLogger.NOP_LOGGER;

	@Test
	void kv_append_line() {
		StringBuilder sb = FpLog.kv();
		FpLog.append(sb, "a", 1);
		FpLog.append(sb, "b", "x");
		assertEquals("[fp_write] a=1 b=x", FpLog.line(FpLog.TAG_WRITE, sb));
		assertEquals("[fp_token] k=v", FpLog.line(FpLog.TAG_TOKEN, "k", "v"));
	}

	@Test
	void trace_withAndWithoutId() {
		StringBuilder sb = FpLog.kv();
		FpLog.append(sb, "q", 1);
		assertTrue(FpLog.trace("t1", FpLog.TAG_SEARCH, sb).startsWith("trace=t1 "));
		assertEquals(FpLog.line(FpLog.TAG_SEARCH, sb), FpLog.trace(null, FpLog.TAG_SEARCH, sb));
		assertEquals(FpLog.line(FpLog.TAG_SEARCH, sb), FpLog.trace("", FpLog.TAG_SEARCH, sb));
	}

	@Test
	void bytesToHex_and_appendSliceSummary() {
		assertEquals("0a0b", FpLog.bytesToHex(new byte[] { 10, 11 }, 0, 2));
		assertEquals("", FpLog.bytesToHex(null, 0, 1));
		StringBuilder sb = FpLog.kv();
		FpLog.appendSliceSummary(sb, new BytesRef[] { new BytesRef("ab") });
		assertTrue(sb.toString().contains("sliceCount=1"));
		assertTrue(sb.toString().contains("sliceLens=2"));
		FpLog.appendSliceSummary(sb, null);
	}

	@Test
	void appendBucketKeys_formatsLenAndBucket() {
		StringBuilder sb = FpLog.kv();
		long key = cn.lxdb.plugins.muqingyu.fptoken.dataset.block.FpGroupHotNgramBitIndex.packBucketKey(2, 0x1F);
		FpLog.appendBucketKeys(sb, new long[] { key });
		assertTrue(sb.toString().contains("bucketKeys=2:1f"));
		FpLog.appendBucketKeys(sb, null);
	}

	@Test
	void infoLineSampled_slowAlwaysLogs() {
		Lucene80FPSearchConfig.FLUSH_LOG_SLOW_MS = 10;
		Lucene80FPSearchConfig.FLUSH_LOG_SAMPLE_RATE = 1000;
		StringBuilder sb = FpLog.kv();
		FpLog.append(sb, "e", "slow");
		FpLog.infoLineSampled(NOP, FpLog.TAG_REBUILD, sb, 50);
		FpLog.infoLine(NOP, FpLog.TAG_ORIGINAL, sb);
		FpLog.debugLine(NOP, FpLog.TAG_BITINDEX, sb);
		FpLog.searchTrace(NOP, "tr", sb);
		FpLog.debugTrace(NOP, "tr", sb);
		assertFalse(sb.toString().isEmpty());
	}
}
