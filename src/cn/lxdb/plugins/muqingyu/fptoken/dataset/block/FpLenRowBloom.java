package cn.lxdb.plugins.muqingyu.fptoken.dataset.block;

import java.io.IOException;

import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexInput;

/**
 * LenRow {@code sortedKeys} 的 Bloom 过滤器：selective 读时在段内扫描前做零 IO 负向过滤。
 * 目标假阳性率 {@link #TARGET_FPR}（约 3%）。
 */
public final class FpLenRowBloom {

	/** 魔数 {@code 'FPBL'} */
	public static final int MAGIC = 0x4650424C;
	/** 构建目标假阳性率 */
	public static final double TARGET_FPR = 0.03;

	private static final FpLenRowBloom PASSTHROUGH = new FpLenRowBloom(null, 0, 0);

	private final long[] bits;
	private final int numBits;
	private final int numHashes;

	private FpLenRowBloom(long[] bits, int numBits, int numHashes) {
		this.bits = bits;
		this.numBits = numBits;
		this.numHashes = numHashes;
	}

	public static FpLenRowBloom passthrough() {
		return PASSTHROUGH;
	}

	public static FpLenRowBloom build(int[] sortedKeys) {
		if (sortedKeys == null || sortedKeys.length == 0) {
			return PASSTHROUGH;
		}
		final int n = sortedKeys.length;
		final int numBits = optimalNumBits(n, TARGET_FPR);
		final int numHashes = optimalNumHashes(n, numBits);
		final long[] bitset = new long[(numBits + 63) >>> 6];
		for (int key : sortedKeys) {
			addToBitset(bitset, numBits, numHashes, key);
		}
		return new FpLenRowBloom(bitset, numBits, numHashes);
	}

	public boolean mightContain(int key) {
		if (numBits == 0) {
			return true;
		}
		final long h1 = spreadHash(key, 0);
		final long h2 = spreadHash(key, 1);
		for (int i = 0; i < numHashes; i++) {
			int bitIndex = (int) ((h1 + (long) i * h2) % numBits);
			if (bitIndex < 0) {
				bitIndex += numBits;
			}
			if (!getBit(bitIndex)) {
				return false;
			}
		}
		return true;
	}

	public void writeTo(DataOutput out) throws IOException {
		out.writeInt(MAGIC);
		out.writeInt(numBits);
		out.writeInt(numHashes);
		if (numBits == 0) {
			out.writeInt(0);
			return;
		}
		final byte[] bytes = toBytes();
		out.writeInt(bytes.length);
		out.writeBytes(bytes, 0, bytes.length);
	}

	public static void writeEmpty(DataOutput out) throws IOException {
		PASSTHROUGH.writeTo(out);
	}

	public static FpLenRowBloom readFrom(DataInput in) throws IOException {
		final int magic = in.readInt();
		if (magic != MAGIC) {
			throw new IOException("unexpected LenRow bloom magic: " + Integer.toHexString(magic));
		}
		final int numBits = in.readInt();
		final int numHashes = in.readInt();
		final int byteLen = in.readInt();
		if (numBits == 0 || byteLen == 0) {
			return PASSTHROUGH;
		}
		final byte[] bytes = new byte[byteLen];
		in.readBytes(bytes, 0, byteLen);
		return fromBytes(bytes, numBits, numHashes);
	}

	public static FpLenRowBloom readFrom(IndexInput in, long offset) throws IOException {
		if (offset <= 0) {
			return PASSTHROUGH;
		}
		in.seek(offset);
		return readFrom(in);
	}

	static int optimalNumBits(int n, double fpp) {
		if (n <= 0) {
			return 0;
		}
		final double bits = -n * Math.log(fpp) / (Math.log(2) * Math.log(2));
		return Math.max(64, (int) Math.ceil(bits));
	}

	static int optimalNumHashes(int n, int numBits) {
		if (n <= 0 || numBits <= 0) {
			return 0;
		}
		return Math.max(1, (int) Math.round((double) numBits / n * Math.log(2)));
	}

	private static void addToBitset(long[] bitset, int numBits, int numHashes, int key) {
		final long h1 = spreadHash(key, 0);
		final long h2 = spreadHash(key, 1);
		for (int i = 0; i < numHashes; i++) {
			int bitIndex = (int) ((h1 + (long) i * h2) % numBits);
			if (bitIndex < 0) {
				bitIndex += numBits;
			}
			bitset[bitIndex >>> 6] |= 1L << (bitIndex & 63);
		}
	}

	private static long spreadHash(int key, int seed) {
		long h = (key & 0xFFFFFFFFL) ^ ((long) seed * 0x9E3779B97F4A7C15L);
		h ^= (h >>> 33);
		h *= 0xff51afd7ed558ccdL;
		h ^= (h >>> 33);
		h *= 0xc4ceb9fe1a85ec53L;
		h ^= (h >>> 33);
		return h;
	}

	private boolean getBit(int bitIndex) {
		return (bits[bitIndex >>> 6] & (1L << (bitIndex & 63))) != 0L;
	}

	private byte[] toBytes() {
		final int byteLen = (numBits + 7) >>> 3;
		final byte[] bytes = new byte[byteLen];
		for (int i = 0; i < numBits; i++) {
			if (getBit(i)) {
				bytes[i >>> 3] |= (byte) (1 << (i & 7));
			}
		}
		return bytes;
	}

	private static FpLenRowBloom fromBytes(byte[] bytes, int numBits, int numHashes) {
		final long[] bitset = new long[(numBits + 63) >>> 6];
		for (int i = 0; i < numBits; i++) {
			if ((bytes[i >>> 3] & (1 << (i & 7))) != 0) {
				bitset[i >>> 6] |= 1L << (i & 63);
			}
		}
		return new FpLenRowBloom(bitset, numBits, numHashes);
	}
}
