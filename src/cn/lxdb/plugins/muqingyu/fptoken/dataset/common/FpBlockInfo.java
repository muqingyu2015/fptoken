package cn.lxdb.plugins.muqingyu.fptoken.dataset.common;

import java.io.IOException;

import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexInput;

/**
 * FP 块元数据：与 {@code termsbit} 等侧文件中的 ngram bit 区布局对应。
 */
public class FpBlockInfo {

	/** 与 {@link #writeto} / {@link #readfrom} 格式一致 */
	public static final int FORMAT_VERSION = 1;

	/**
	 * 交错区起点：{@code banksHot[0][0]} 在 bit 文件中的偏移。
	 * 第 {@code (li * 256 + b)} 对为
	 * {@code fpBanksHot00 + (li * 256 + b) * (bytesPerHotSerialized + bytesPerCommonSerialized)}。
	 */
	public long fpBanksHot00;
	/** {@code banksCommon[0][0]} 的偏移，等于 {@code fpBanksHot00 + bytesPerHotSerialized}。 */
	public long fpBanksCommon00;
	/** 序列化后单个热词侧 {@link org.apache.lucene.util.FixedBitSet} 的字节数（含首部 int 词数）。 */
	public int bytesPerHotSerialized;
	/** 序列化后单个普通词侧 {@link org.apache.lucene.util.FixedBitSet} 的字节数。 */
	public int bytesPerCommonSerialized;

	public int hotNumBits;
	public int commonNumBits;
	public int hotCount;
	public int commonCount;

	public void writeto(DataOutput out) throws IOException {
		out.writeInt(FORMAT_VERSION);
		out.writeLong(fpBanksHot00);
		out.writeLong(fpBanksCommon00);
		out.writeInt(bytesPerHotSerialized);
		out.writeInt(bytesPerCommonSerialized);
		out.writeInt(hotNumBits);
		out.writeInt(commonNumBits);
		out.writeInt(hotCount);
		out.writeInt(commonCount);
	}

	public void readfrom(IndexInput in) throws IOException {
		final int ver = in.readInt();
		if (ver != FORMAT_VERSION) {
			throw new IOException("FpBlockInfo unsupported format version: " + ver);
		}
		fpBanksHot00 = in.readLong();
		fpBanksCommon00 = in.readLong();
		bytesPerHotSerialized = in.readInt();
		bytesPerCommonSerialized = in.readInt();
		hotNumBits = in.readInt();
		commonNumBits = in.readInt();
		hotCount = in.readInt();
		commonCount = in.readInt();
	}

	/** 第 {@code li} 长度、桶 {@code b} 的热词 bitset 在 bit 文件中的起始偏移。 */
	public long hotBankOffset(int li, int b) {
		final long pair = (long) li * 256L + (long) b;
		return fpBanksHot00 + pair * (long) (bytesPerHotSerialized + bytesPerCommonSerialized);
	}

	/** 同上位置的普通词侧起始偏移。 */
	public long commonBankOffset(int li, int b) {
		return hotBankOffset(li, b) + (long) bytesPerHotSerialized;
	}
}
