package cn.lxdb.plugins.muqingyu.fptoken.tests.unit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import org.junit.jupiter.api.Test;

import cn.lxdb.plugins.muqingyu.fptoken.dataset.block.FpGroupHotNgramBitIndex;

/** {@link FpGroupHotNgramBitIndex#bucketIndex} 三路哈希 */
class FpGroupHotNgramBitIndexBucketIndex2Test {

	@Test
	void bucketIndex_returnsThreeIndependentBuckets() {
		final byte[] buf = { 'a', 'b', 'c' };
		final int[] buckets = FpGroupHotNgramBitIndex.bucketIndex(buf, 0, 3);
		assertEquals(FpGroupHotNgramBitIndex.BUCKET_HASH_COUNT, buckets.length);
		assertEquals(FpGroupHotNgramBitIndex.bucketIndex1(buf, 0, 3), buckets[0]);
		assertEquals(FpGroupHotNgramBitIndex.bucketIndex2(buf, 0, 3), buckets[1]);
		assertEquals(FpGroupHotNgramBitIndex.bucketIndex3(buf, 0, 3), buckets[2]);
		for (int b : buckets) {
			assertEquals(b & 0xFF, b);
		}
	}

	@Test
	void singleByte_threeHashes_mostlyDistinctFromBucketIndex1() {
		int differ2 = 0;
		int differ3 = 0;
		for (int b = 0; b < 256; b++) {
			final byte[] buf = { (byte) b };
			final int h1 = FpGroupHotNgramBitIndex.bucketIndex1(buf, 0, 1);
			final int h2 = FpGroupHotNgramBitIndex.bucketIndex2(buf, 0, 1);
			final int h3 = FpGroupHotNgramBitIndex.bucketIndex3(buf, 0, 1);
			assertEquals(b, h1);
			if (h1 != h2) {
				differ2++;
			}
			if (h1 != h3) {
				differ3++;
			}
		}
		assertNotEquals(0, differ2);
		assertNotEquals(0, differ3);
	}

	@Test
	void multiByte_eachHashDeterministicInRange() {
		final byte[] buf = { 1, 2, 3, 4, 5 };
		for (int fn = 1; fn <= 3; fn++) {
			final int a = hash(buf, fn);
			final int b = hash(buf, fn);
			assertEquals(a, b);
			assertEquals(a & 0xFF, a);
		}
	}

	private static int hash(byte[] buf, int fn) {
		return switch (fn) {
			case 1 -> FpGroupHotNgramBitIndex.bucketIndex1(buf, 0, buf.length);
			case 2 -> FpGroupHotNgramBitIndex.bucketIndex2(buf, 0, buf.length);
			case 3 -> FpGroupHotNgramBitIndex.bucketIndex3(buf, 0, buf.length);
			default -> throw new IllegalArgumentException();
		};
	}
}
