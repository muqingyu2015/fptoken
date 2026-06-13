package cn.lxdb.plugins.muqingyu.fptoken.tests.unit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import cn.lxdb.plugins.muqingyu.fptoken.dataset.block.FpGroupHotNgramBitIndex;

@Tag("lxdb-runtime")
class FpGroupHotNgramBitIndexStaticTest {

	@Test
	void packAndUnpackBucketKey() {
		long key = FpGroupHotNgramBitIndex.packBucketKey(3, 42);
		assertEquals(3, FpGroupHotNgramBitIndex.unpackLenIdx(key));
		assertEquals(42, FpGroupHotNgramBitIndex.unpackBucketIndex(key));
	}

	@Test
	void bucketIndex_shortNgram_usesLowBytes() {
		org.apache.lucene.util.BytesRef slice = new org.apache.lucene.util.BytesRef(new byte[] { 1, 2 });
		int b = FpGroupHotNgramBitIndex.bucketIndex(slice);
		assertEquals((2 << 24) | ((1 << 8) | 2), b);
	}

	@Test
	void bucketIndex_encodesLengthInHighByte() {
		org.apache.lucene.util.BytesRef slice = new org.apache.lucene.util.BytesRef(new byte[] { 'a', 'b', 'c' });
		int b = FpGroupHotNgramBitIndex.bucketIndex(slice);
		assertEquals(3, b >>> 24);
		assertEquals(((int) 'a' << 16) | ((int) 'b' << 8) | (int) 'c', b & 0xFFFFFF);
	}

	@Test
	void bucketIndex_byteArray_matchesBytesRef() {
		byte[] buf = { 10, 20, 30 };
		org.apache.lucene.util.BytesRef ref = new org.apache.lucene.util.BytesRef(buf, 1, 2);
		assertEquals(FpGroupHotNgramBitIndex.bucketIndex(ref),
				FpGroupHotNgramBitIndex.bucketIndex(buf, 1, 2));
	}

	@Test
	void selectiveKeysForSlice_invalidLength_returnsEmpty() {
		assertEquals(0, FpGroupHotNgramBitIndex.selectiveKeysForSlice(new org.apache.lucene.util.BytesRef()).length);
		assertEquals(0,
				FpGroupHotNgramBitIndex.selectiveKeysForSlice(new org.apache.lucene.util.BytesRef(new byte[7])).length);
	}

	@Test
	void selectiveKeysForSlice_validSlice_returnsOneKey() {
		org.apache.lucene.util.BytesRef slice = new org.apache.lucene.util.BytesRef(new byte[] { 1, 2, 3 });
		long[] keys = FpGroupHotNgramBitIndex.selectiveKeysForSlice(slice);
		assertEquals(1, keys.length);
		assertEquals(2, FpGroupHotNgramBitIndex.unpackLenIdx(keys[0]));
	}

	@Test
	void selectiveKeysForSlices_deduplicates() {
		org.apache.lucene.util.BytesRef a = new org.apache.lucene.util.BytesRef(new byte[] { 1, 2 });
		org.apache.lucene.util.BytesRef b = new org.apache.lucene.util.BytesRef(new byte[] { 1, 2 });
		long[] keys = FpGroupHotNgramBitIndex.selectiveKeysForSlices(new org.apache.lucene.util.BytesRef[] { a, b });
		assertEquals(1, keys.length);
	}
}
