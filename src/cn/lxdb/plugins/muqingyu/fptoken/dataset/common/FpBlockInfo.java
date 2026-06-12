package cn.lxdb.plugins.muqingyu.fptoken.dataset.common;

import java.io.IOException;

import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BytesRef;

/**
 * FP 块元数据：与 {@code termsbit} 等侧文件中的 ngram bit 区布局对应。
 */
public class FpBlockInfo {

	/** 与 {@link #writeto} / {@link #readfrom} 格式一致；v4 为段内 lenIdx 分池布局（含 Bloom 池） */
	public static final int FORMAT_VERSION = 4;

	/**
	 * 交错区起点：{@code banksHot[0][0]} 在 bit 文件中的偏移。
	 * 第 {@code bankPairIndex(li,b)} 对为
	 * {@code fpBanksHot + bankPairIndex(li,b) * (bytesPerHotSerialized + bytesPerCommonSerialized)}；
	 * len=1 行仅 {@link cn.lxdb.plugins.muqingyu.fptoken.config.Lucene80FPSearchConfig#BUCKETS_LEN1} 桶。
	 */
	public  long fpBanksHot;
	@Override
	public String toString() {
		return "targetLevel=L" + targetLevel
				+ " field=" + (fieldInfo == null ? "" : Utils.BytesReftoString(fieldInfo))
				+ " hotTerms=" + hotCount + " commonTerms=" + commonCount
				+ " hotBits=" + hotNumBits + " commonBits=" + commonNumBits
				+ " docs=" + docCount
				+ " fileOffHot=" + fpBanksHot + " fileOffCommon=" + fpBanksCommon
				+ " bytesHot=" + bytesPerHotSerialized + " bytesCommon=" + bytesPerCommonSerialized;
	}

	/** {@code banksCommon[0][0]} 的偏移，等于 {@code fpBanksHot00 + bytesPerHotSerialized}。 */
	public  long fpBanksCommon;
	/** 序列化后单个热词侧 {@link org.apache.lucene.util.FixedBitSet} 的字节数（含首部 int 词数）。 */
	public  int bytesPerHotSerialized;
	/** 序列化后单个普通词侧 {@link org.apache.lucene.util.FixedBitSet} 的字节数。 */
	public  int bytesPerCommonSerialized;
	public FpBlockInfo() {
		this.fpBanksHot = 0;
		this.fpBanksCommon = 0;
		this.bytesPerHotSerialized = 0;
		this.bytesPerCommonSerialized = 0;
		this.hotNumBits = 0;
		this.commonNumBits = 0;
		this.hotCount = 0;
		this.commonCount = 0;
		this.targetLevel = 0;
		this.fieldInfo = new BytesRef("d");
		this.docCount = 0;
	}
	public FpBlockInfo(long fpBanksHot, long fpBanksCommon, int bytesPerHotSerialized, int bytesPerCommonSerialized,
			int hotNumBits, int commonNumBits, int hotCount, int commonCount, int targetLevel, BytesRef fieldInfo,
			int docCount) {
		this.fpBanksHot = fpBanksHot;
		this.fpBanksCommon = fpBanksCommon;
		this.bytesPerHotSerialized = bytesPerHotSerialized;
		this.bytesPerCommonSerialized = bytesPerCommonSerialized;
		this.hotNumBits = hotNumBits;
		this.commonNumBits = commonNumBits;
		this.hotCount = hotCount;
		this.commonCount = commonCount;
		this.targetLevel = targetLevel;
		this.fieldInfo = fieldInfo;
		this.docCount = docCount;
	}

	public   int hotNumBits;
	public   int commonNumBits;
	public  int hotCount;
	public  int commonCount;
	public  int targetLevel;
	public  BytesRef fieldInfo;
	public  int docCount;
	public  long debug_mark=System.currentTimeMillis();

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
		if (ver != FORMAT_VERSION && ver != 3) {
			throw new IOException("FpBlockInfo unsupported format version: " + ver + " (expected " + FORMAT_VERSION
					+ " or 3)");
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

	/** @deprecated v3 位图区为 tier 布局，请使用 {@link cn.lxdb.plugins.muqingyu.fptoken.dataset.block.FpGroupHotNgramBitIndex} 内偏移。 */
	@Deprecated
	public long hotBankOffset(int li, int b) {
		throw new UnsupportedOperationException("hotBankOffset is deprecated in FpBlockInfo v3");
	}

	/** @deprecated v3 位图区为 tier 布局。 */
	@Deprecated
	public long commonBankOffset(int li, int b) {
		throw new UnsupportedOperationException("commonBankOffset is deprecated in FpBlockInfo v3");
	}
}
