package cn.lxdb.plugins.muqingyu.fptoken.dataset.block;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.offheap.OffheapPoolName;
import org.slf4j.Logger;

import cn.lucene.lxdb.params.LxdbLogerEncrypt;
import cn.lxdb.plugins.muqingyu.fptoken.config.Lucene80FPSearchConfig;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpBlockInfo;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpTermKey;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpTokenTermLayout;

/**
 * 热词 / 普通词各一套 byte ngram（{@value #NGRAM_MIN}~{@value #NGRAM_MAX}）倒排索引：
 * 均为 8×256 组 {@link FixedBitSet}，但<strong>两套 bitset 完全分开</strong>。
 * <ul>
 * <li><b>热词</b>：{@link FpGroupDataRebuild#rebuildHotTermOrderFromHotDocs()} 后按 {@link FpTermKey#ORDER_BY_LENGTH_THEN_BYTES} 编号 1..H；
 * 每位宽为 {@code max(1,H)}。</li>
 * <li><b>普通词</b>：单独一套 bitset，按 {@link FpGroupDataRebuild#commonTermMapInternal()} 的 {@link TreeMap} 序编号 1..C；
 * 每位宽为 {@code max(1,C)}。对每个 ngram 切片，若已作为键存在于 {@code hotTermToDocs} 则<strong>不写</strong> common 侧，
 * 其余与热词相同（同桶哈希、同 {@code set(order-1)} 规则）。</li>
 * </ul>
 * 调用：{@code FpGroupHotNgramBitIndex.execute(group, parentItem, 10);}（第三参数预留）。
 */
public final class FpGroupHotNgramBitIndex {
    public static final Logger LOG = LxdbLogerEncrypt.getLogger("mqy.fptoken");


	public final FixedBitSet[][] banksHot;
	public final FixedBitSet[][] banksCommon;
	private final int hotNumBits;
	private final int commonNumBits;
	private final int hotCount;
	private final int commonCount;
	private final int targetlevel;

	private FpGroupHotNgramBitIndex(int targetlevel,FixedBitSet[][] banksHot, FixedBitSet[][] banksCommon, int hotNumBits, int commonNumBits,
			int hotCount, int commonCount) {
		this.banksHot = banksHot;
		this.banksCommon = banksCommon;
		this.hotNumBits = hotNumBits;
		this.commonNumBits = commonNumBits;
		this.hotCount = hotCount;
		this.commonCount = commonCount;
		this.targetlevel=targetlevel;
	}

	/**
	 * 从 bit 文件按 {@link FpBlockInfo} 中记录的交错布局读回全部 banks。
	 */
	public static FpGroupHotNgramBitIndex readfrom(IndexInput in, FpBlockInfo blkinfo) throws IOException {
		final FixedBitSet[][] hot = new FixedBitSet[Lucene80FPSearchConfig.NGRAM_MAX][Lucene80FPSearchConfig.BUCKETS];
		final FixedBitSet[][] common = new FixedBitSet[Lucene80FPSearchConfig.NGRAM_MAX][Lucene80FPSearchConfig.BUCKETS];
		in.seek(blkinfo.fpBanksHot);
		for (int li = 0; li < Lucene80FPSearchConfig.NGRAM_MAX; li++) {
			for (int b = 0; b < Lucene80FPSearchConfig.BUCKETS; b++) {
				hot[li][b] = in.readBits(blkinfo.hotNumBits);
				common[li][b] = in.readBits(blkinfo.commonNumBits);
				
				
				if(Lucene80FPSearchConfig.PRINT_DEBUG)
				{  
					LOG.info("bitset readfrom:"+hot[li][b].cardinality()+" "+hot[li][b].nextSetBit(0)+" "+common[li][b].cardinality()+" "+common[li][b].nextSetBit(0)+" "+li+" " +b  +" "+blkinfo);
				}
			}
		}
		return new FpGroupHotNgramBitIndex(blkinfo.targetLevel,hot, common, blkinfo.hotNumBits, blkinfo.commonNumBits, blkinfo.hotCount,
				blkinfo.commonCount);
	}

	/**
	 * 按需读取：仅 {@code loadHot[li][b]} / {@code loadCommon[li][b]} 为 true 的位置调用 {@link IndexInput#seek(long)} 并
	 * {@link IndexInput#readBits(int)}；其余位置对应元素为 {@code null}。
	 *
	 * @param loadHot    长度 {@value #NGRAM_MAX}，每行长度 {@value Lucene80FPSearchConfig#BUCKETS}
	 * @param loadCommon 同上
	 */
	public static FpGroupHotNgramBitIndex readfromBanksSelective(IndexInput in, FpBlockInfo blkinfo, boolean[][] loadHot,
			boolean[][] loadCommon) throws IOException {
		final FixedBitSet[][] hot = new FixedBitSet[Lucene80FPSearchConfig.NGRAM_MAX][Lucene80FPSearchConfig.BUCKETS];
		final FixedBitSet[][] common = new FixedBitSet[Lucene80FPSearchConfig.NGRAM_MAX][Lucene80FPSearchConfig.BUCKETS];
		final int bh = blkinfo.bytesPerHotSerialized;
		for (int li = 0; li < Lucene80FPSearchConfig.NGRAM_MAX; li++) {
			for (int b = 0; b < Lucene80FPSearchConfig.BUCKETS; b++) {
				final long pairBase = blkinfo.hotBankOffset(li, b);
				final boolean rh = loadHot[li][b];
				final boolean rc = loadCommon[li][b];
				if (rh) {
					in.seek(pairBase);
					hot[li][b] = in.readBits(blkinfo.hotNumBits);
				} else {
					hot[li][b] = null;
				}
				if (rc) {
					if (rh) {
						common[li][b] = in.readBits(blkinfo.commonNumBits);
					} else {
						in.seek(pairBase + (long) bh);
						common[li][b] = in.readBits(blkinfo.commonNumBits);
					}
				} else {
					common[li][b] = null;
				}
				
				
				if(Lucene80FPSearchConfig.PRINT_DEBUG&&(rh||rc))
				{  
					LOG.info("bitset readfrom:"+(hot[li][b]==null?"null":hot[li][b].cardinality())+" "+(hot[li][b]==null?"null":hot[li][b].nextSetBit(0))+" "+(common[li][b]==null?"null":common[li][b].cardinality())+" "+(common[li][b]==null?"null":common[li][b].nextSetBit(0))+" "+li+" " +b  +" "+blkinfo);
				}
			}
		}
		return new FpGroupHotNgramBitIndex(blkinfo.targetLevel,hot, common, blkinfo.hotNumBits, blkinfo.commonNumBits, blkinfo.hotCount,
				blkinfo.commonCount);
	}

	/**
	 * 将 {@link #banksHot} / {@link #banksCommon} 按
	 * {@code hot[li][b], common[li][b]} 交错写入 {@code out}，并把偏移与步长写入 {@link FpBlockInfo}（由调用方再
	 * {@link FpBlockInfo#writeto} 到 meta）。
	 */
	public FpBlockInfo flushto(IndexOutput out, String from, BytesRef fieldInfo, int docCount) throws IOException {

		long fpBanksHot = out.getFilePointer();
		out.writeBits(banksHot[0][0]);
		long afterFirstHot = out.getFilePointer();
		out.writeBits(banksCommon[0][0]);
		long afterFirstCommon = out.getFilePointer();

		long fpBanksCommon = afterFirstHot;
		int bytesPerHotSerialized = (int) (afterFirstHot - fpBanksHot);
		int bytesPerCommonSerialized = (int) (afterFirstCommon - afterFirstHot);

		final FpBlockInfo info = new FpBlockInfo(fpBanksHot, fpBanksCommon, bytesPerHotSerialized,
				bytesPerCommonSerialized, hotNumBits, commonNumBits, hotCount, commonCount, this.targetlevel, fieldInfo,
				docCount);

		for (int li = 0; li < Lucene80FPSearchConfig.NGRAM_MAX; li++) {
			for (int b = 0; b < Lucene80FPSearchConfig.BUCKETS; b++) {
				if (li == 0 && b == 0) {// 第一个已经在之前写进去了
					continue;
				}
				out.writeBits(banksHot[li][b]);
				out.writeBits(banksCommon[li][b]);

				if (Lucene80FPSearchConfig.PRINT_DEBUG
						&& (banksHot[li][b].cardinality() > 0 || banksCommon[li][b].cardinality() > 0)) {
					LOG.info("bitset " + from + " flushto:" + banksHot[li][b].cardinality() + " "
							+ banksHot[li][b].nextSetBit(0) + " " + banksCommon[li][b].cardinality() + " "
							+ banksCommon[li][b].nextSetBit(0) + " " + li + " " + b + " " + info);
				}
			}
		}
		return info;
	}



	

	public static FpGroupHotNgramBitIndex execute(int targetLevel,FpGroupDataRebuild group) {
		

		
		final int h = group.hotTermOrderSize();
		
		final int c = group.commonTermOrderSize();
		final int numBitsHot = bitsLengthAlign(h);
		final int numBitsCommon = bitsLengthAlign(c);

		final FixedBitSet[][] hotBanks = allocBanks(numBitsHot);
		final FixedBitSet[][] commonBanks = allocBanks(numBitsCommon);

		final TreeMap<FpTermKey, Integer> hotOrder = group.hotTermOrderInternal();
		final TreeMap<FpTermKey, Integer> commonOrder = group.commonTermOrderInternal();

		for (Map.Entry<FpTermKey, Integer> e : hotOrder.entrySet()) {
			final int order = e.getValue().intValue();
			markNgramsForPayload(hotBanks, null, false, e.getKey().bytesRef(), order, numBitsHot);
		}

		for (Map.Entry<FpTermKey, Integer> e : commonOrder.entrySet()) {
			final int order = e.getValue().intValue();
			markNgramsForPayload(commonBanks, hotOrder, true, e.getKey().bytesRef(), order, numBitsCommon);
		}

		return new FpGroupHotNgramBitIndex(targetLevel,hotBanks, commonBanks, numBitsHot, numBitsCommon, h, c);
	}
	
	private static int BITS_ALIGN_SIZE=128;
	private static int bitsLengthAlign(int bits)
	{
		
		int rtn=Math.max(1, bits+1);
		if(rtn%BITS_ALIGN_SIZE==0)
		{
			return rtn;
		}
		
		int blks=(rtn/BITS_ALIGN_SIZE)+1;
		return blks*BITS_ALIGN_SIZE;
		
	}

	private static FixedBitSet[][] allocBanks(int numBits) {
		final FixedBitSet[][] banks = new FixedBitSet[Lucene80FPSearchConfig.NGRAM_MAX][Lucene80FPSearchConfig.BUCKETS];
		for (int li = 0; li < Lucene80FPSearchConfig.NGRAM_MAX; li++) {
			for (int b = 0; b < Lucene80FPSearchConfig.BUCKETS; b++) {
				banks[li][b] = new FixedBitSet(OffheapPoolName.fptokenbitset, numBits);
			}
		}
		return banks;
	}

	/** 1 字节：桶号即该字节；2~8 字节：多项式哈希折叠到 0..255。 */
	public static int bucketIndex(byte[] buf, int off, int len) {
		if (len == 1) {
			return buf[off] & 0xFF;
		}
		int hv = 0;
		for (int i = 0; i < len; i++) {
			hv = 31 * hv + (buf[off + i] & 0xFF);
		}
		return (hv ^ (hv >>> 8) ^ (hv >>> 16) ^ (hv >>> 24)) & 0xFF;
	}

	/**
	 * @param hotDocs       仅当 {@code skipIfInHot} 为 true 时使用：切片若已是热词整键则跳过
	 */
	private static void markNgramsForPayload(FixedBitSet[][] banks, TreeMap<FpTermKey, Integer> hotTermCheck, boolean skipIfInHot,
			BytesRef payload, int order, int numBits) {
		if (order < 1 || order > numBits) {
			return;
		}
		final int bit = order;
		final int payloadLen = payload.length;
		if (payloadLen <= 0) {
			return;
		}
		final int base = payload.offset;
		for (int start = 0; start < payloadLen; start++) {
			for (int n = Lucene80FPSearchConfig.NGRAM_MIN; n <= Lucene80FPSearchConfig.NGRAM_MAX && start + n <= payloadLen; n++) {
				final BytesRef slice = new BytesRef(payload.bytes, base + start, n);
				if (skipIfInHot && hotTermCheck != null && hotTermCheck.containsKey(FpTermKey.viewOf(slice))) {
					continue;
				}
				final int bucket = bucketIndex(slice.bytes, slice.offset, slice.length);
				banks[n - 1][bucket].set(bit);
				if(Lucene80FPSearchConfig.PRINT_DEBUG)
				{
					LOG.info("markNgramsForPayload " +(n - 1)+" "+bucket +" "+slice.utf8ToString());
				}
			}
		}
	}

	/** 热词侧：ngram 长度 1..8 对应 {@code ngramLen}，桶 0..255 */
	public FixedBitSet getHotBank(int ngramLen, int bucket0To255) {
		return banksHot[ngramLen - 1][bucket0To255];
	}

	/** 普通词侧（与热词完全独立） */
	public FixedBitSet getCommonBank(int ngramLen, int bucket0To255) {
		return banksCommon[ngramLen - 1][bucket0To255];
	}
	


	public FixedBitSet[][] getHotBanks() {
		return banksHot;
	}

	public FixedBitSet[][] getCommonBanks() {
		return banksCommon;
	}

	public int getHotNumBits() {
		return hotNumBits;
	}

	public int getCommonNumBits() {
		return commonNumBits;
	}

	public int getHotCount() {
		return hotCount;
	}

	public int getCommonCount() {
		return commonCount;
	}
}
