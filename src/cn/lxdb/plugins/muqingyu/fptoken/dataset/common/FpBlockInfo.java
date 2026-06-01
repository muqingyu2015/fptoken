package cn.lxdb.plugins.muqingyu.fptoken.dataset.common;

import java.io.IOException;

import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BytesRef;

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
	public long fpBanksHot;
	@Override
	public String toString() {
		return "FpBlockInfo [fpBanksHot=" + fpBanksHot + ", fpBanksCommon=" + fpBanksCommon + ", bytesPerHotSerialized="
				+ bytesPerHotSerialized + ", bytesPerCommonSerialized=" + bytesPerCommonSerialized + ", hotNumBits="
				+ hotNumBits + ", commonNumBits=" + commonNumBits + ", hotCount=" + hotCount + ", commonCount="
				+ commonCount + ", docCount=" + docCount  + ", targetLevel=" + targetLevel + ", debug_mark=" + debug_mark +",fieldInfo="+(fieldInfo==null?"":fieldInfo.utf8ToString())+ "]";
	}

	/** {@code banksCommon[0][0]} 的偏移，等于 {@code fpBanksHot00 + bytesPerHotSerialized}。 */
	public long fpBanksCommon;
	/** 序列化后单个热词侧 {@link org.apache.lucene.util.FixedBitSet} 的字节数（含首部 int 词数）。 */
	public int bytesPerHotSerialized;
	/** 序列化后单个普通词侧 {@link org.apache.lucene.util.FixedBitSet} 的字节数。 */
	public int bytesPerCommonSerialized;

	public int hotNumBits;
	public int commonNumBits;
	public int hotCount;
	public int commonCount;
	public int targetLevel;
	public BytesRef fieldInfo=new BytesRef("d");
	public int docCount;
	public long debug_mark=System.currentTimeMillis();

	public void writeto(DataOutput out) throws IOException {
		out.writeInt(FORMAT_VERSION);
		out.writeLong(fpBanksHot);
		out.writeLong(fpBanksCommon);
		out.writeInt(bytesPerHotSerialized);
		out.writeInt(bytesPerCommonSerialized);
		out.writeInt(hotNumBits);
		out.writeInt(commonNumBits);
		out.writeInt(hotCount);
		out.writeInt(commonCount);
		out.writeInt(docCount);

		out.writeInt(targetLevel);
		out.writeLong(debug_mark);
		out.writeInt(fieldInfo.length);
		out.writeBytes(fieldInfo.bytes, fieldInfo.offset, fieldInfo.length);

	}

	public void readfrom(IndexInput in) throws IOException {
		final int ver = in.readInt();
		if (ver != FORMAT_VERSION) {
			throw new IOException("FpBlockInfo unsupported format version: " + ver);
		}
		fpBanksHot = in.readLong();
		fpBanksCommon = in.readLong();
		bytesPerHotSerialized = in.readInt();
		bytesPerCommonSerialized = in.readInt();
		hotNumBits = in.readInt();
		commonNumBits = in.readInt();
		hotCount = in.readInt();
		commonCount = in.readInt();
		docCount=in.readInt();
		targetLevel = in.readInt();
		debug_mark=in.readLong();
		byte[] data=new byte[in.readInt()];
		in.readBytes(data, 0, data.length);
		fieldInfo=new BytesRef(data);

	}

	/** 第 {@code li} 长度、桶 {@code b} 的热词 bitset 在 bit 文件中的起始偏移。 */
	public long hotBankOffset(int li, int b) {
		final long pair = (long) li * 256L + (long) b;
		return fpBanksHot + pair * (long) (bytesPerHotSerialized + bytesPerCommonSerialized);
	}

	/** 同上位置的普通词侧起始偏移。 */
	public long commonBankOffset(int li, int b) {
		return hotBankOffset(li, b) + (long) bytesPerHotSerialized;
	}
}
