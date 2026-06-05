package cn.lxdb.plugins.muqingyu.fptoken.dataset.block;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.TreeMap;

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.offheap.OffheapPoolName;
import org.slf4j.Logger;

import cn.lucene.lxdb.params.LxdbLogerEncrypt;
import cn.lxdb.plugins.muqingyu.fptoken.config.Lucene80FPSearchConfig;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpBlockInfo;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpTermKey;

/**
 * 热词 / 普通词各一套 byte ngram（{@value #NGRAM_MIN}~{@value #NGRAM_MAX}）倒排索引：
 * 按 ngram 长度分行：len=1 为 {@link Lucene80FPSearchConfig#BUCKETS_LEN1} 桶，len≥2 为
 * {@link Lucene80FPSearchConfig#BUCKETS} 桶；<strong>hot/common 两套 bitset 完全分开</strong>。
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
		final FixedBitSet[][] hot = allocEmptyBankGrid();
		final FixedBitSet[][] common = allocEmptyBankGrid();
		in.seek(blkinfo.fpBanksHot);
		for (int li = 0; li < Lucene80FPSearchConfig.NGRAM_MAX; li++) {
			final int bucketCount = Lucene80FPSearchConfig.bucketsForLengthIndex(li);
			for (int b = 0; b < bucketCount; b++) {
				hot[li][b] = in.readBits(blkinfo.hotNumBits);
				common[li][b] = in.readBits(blkinfo.commonNumBits);
			}
		}
		return new FpGroupHotNgramBitIndex(blkinfo.targetLevel,hot, common, blkinfo.hotNumBits, blkinfo.commonNumBits, blkinfo.hotCount,blkinfo.commonCount);
	}

	/**
	 * 按需读取：仅 {@code loadHot[li][b]} / {@code loadCommon[li][b]} 为 true 的位置调用 {@link IndexInput#seek(long)} 并
	 * {@link IndexInput#readBits(int)}；其余位置对应元素为 {@code null}。
	 *
	 * @param loadHot    长度 {@link Lucene80FPSearchConfig#NGRAM_MAX}，每行长度 {@link Lucene80FPSearchConfig#maxBucketsPerRow()}
	 * @param loadCommon 同上
	 */
	public static FpGroupHotNgramBitIndex readfromBanksSelective(IndexInput in, FpBlockInfo blkinfo, boolean[][] loadHot,
			boolean[][] loadCommon) throws IOException {
		final FixedBitSet[][] hot = allocEmptyBankGrid();
		final FixedBitSet[][] common = allocEmptyBankGrid();
		final int bh = blkinfo.bytesPerHotSerialized;
		for (int li = 0; li < Lucene80FPSearchConfig.NGRAM_MAX; li++) {
			final int bucketCount = Lucene80FPSearchConfig.bucketsForLengthIndex(li);
			for (int b = 0; b < bucketCount; b++) {
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
			}
		}
		return new FpGroupHotNgramBitIndex(blkinfo.targetLevel,hot, common, blkinfo.hotNumBits, blkinfo.commonNumBits, blkinfo.hotCount,blkinfo.commonCount);
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
			final int bucketCount = Lucene80FPSearchConfig.bucketsForLengthIndex(li);
			for (int b = 0; b < bucketCount; b++) {
				if (li == 0 && b == 0) {// 第一个已经在之前写进去了
					continue;
				}
				out.writeBits(banksHot[li][b]);
				out.writeBits(banksCommon[li][b]);
			}
		}
		
		StringBuffer bitsetinfo=new StringBuffer();
		
		for (int li = 0; li < Lucene80FPSearchConfig.NGRAM_MAX; li++) {
			long sum_hot=0;
			long sum_common=0;
			long hot_max=0;
			long common_max=0;
			final int bucketCount = Lucene80FPSearchConfig.bucketsForLengthIndex(li);
			for (int b = 0; b < bucketCount; b++) {
				int hot=banksHot[li][b].cardinality();
				int common=banksCommon[li][b].cardinality();
				sum_hot+=hot;
				sum_common+=common;
				hot_max=Math.max(hot_max, hot);
				common_max=Math.max(common_max, common);
			}
			int rage_hot=(int) (((sum_hot*100)/bucketCount)/Math.max(hotNumBits, 1));
			int rage_common=(int) (((sum_common*100)/bucketCount)/Math.max(commonNumBits, 1));

			bitsetinfo.append(" len").append(li + 1).append("{hot avgFillPct=").append(rage_hot).append("% maxBucket=")
					.append(hot_max).append(" termCount=").append(hotNumBits).append(" | common avgFillPct=")
					.append(rage_common).append("% maxBucket=").append(common_max).append(" termCount=")
					.append(commonNumBits).append('}');
			
		}
		
		LOG.info("[fp_bitindex] flush phase=" + from + " perLength=" + bitsetinfo + " block=" + info);
		
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

		// common 位图跳过已是热词的切片：用 HashSet O(1)，勿对每个滑窗 TreeMap.containsKey
		final HashSet<FpTermKey> hotKeySet = new HashSet<>(Math.max(16, h * 2));
		for (FpTermKey hotKey : hotOrder.keySet()) {
			hotKeySet.add(hotKey);
		}

		for (Map.Entry<FpTermKey, Integer> e : hotOrder.entrySet()) {
			final int order = e.getValue().intValue();
			markHotNgramsForPayload(hotBanks, e.getKey().bytesRef(), order, numBitsHot);
		}

		for (Map.Entry<FpTermKey, Integer> e : commonOrder.entrySet()) {
			final int order = e.getValue().intValue();
			markCommonNgramsByUniqueSlices(hotKeySet, commonBanks, e.getKey().bytesRef(), order, numBitsCommon);
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

	private static FixedBitSet[][] allocEmptyBankGrid() {
		final FixedBitSet[][] banks = new FixedBitSet[Lucene80FPSearchConfig.NGRAM_MAX][];
		for (int li = 0; li < Lucene80FPSearchConfig.NGRAM_MAX; li++) {
			banks[li] = new FixedBitSet[Lucene80FPSearchConfig.bucketsForLengthIndex(li)];
		}
		return banks;
	}

	private static FixedBitSet[][] allocBanks(int numBits) {
		final FixedBitSet[][] banks = allocEmptyBankGrid();
		for (int li = 0; li < Lucene80FPSearchConfig.NGRAM_MAX; li++) {
			for (int b = 0; b < banks[li].length; b++) {
				banks[li][b] = new FixedBitSet(OffheapPoolName.fptokenbitset, numBits);
			}
		}
		return banks;
	}





	/** 1 字节：桶号即该字节（0..255）；2~N 字节：31*h+c 折叠到对应长度桶宽。 */
	public static int bucketIndex256(byte[] buf, int off, int len) {
		if (len <= 0) {
			return 0;
		}
		if (len == 1) {
			return buf[off] & 0xFF;
		}
		return StringHelper.murmurhash3_x86_32(buf,off , len, 0)& 0xFF;
	}


	public static int bucketIndex512(byte[] buf, int off, int len) {
		if (len <= 0) {
			return 0;
		}
		if (len == 1) {
			return buf[off] & 0xFF;
		}
		return StringHelper.murmurhash3_x86_32(buf,off , len, 0)& 0x1FF;
	}



	/**
	 * common 位图：先收集载荷内<strong>去重</strong> ngram 再查热词集并 set bit。
	 * 与逐滑窗 {@link #markNgramsForPayload} 等价（同切片同桶，set 幂等），但避免 O(滑窗数×|hotKeySet|) 次字节比较。
	 */
	private static void markCommonNgramsByUniqueSlices(HashSet<FpTermKey> hotKeySet, FixedBitSet[][] banks,
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
		final BytesRef sliceScratch = new BytesRef();
		sliceScratch.bytes = payload.bytes;
		final int uniqueCap = Math.max(16, Math.min(payloadLen * 4, 4096));
		final HashSet<FpTermKey> uniqueSlices = new HashSet<>(uniqueCap);
		for (int start = 0; start < payloadLen; start++) {
			for (int n = Lucene80FPSearchConfig.NGRAM_MIN; n <= Lucene80FPSearchConfig.NGRAM_MAX
					&& start + n <= payloadLen; n++) {
				sliceScratch.offset = base + start;
				sliceScratch.length = n;
				if (uniqueSlices.contains(FpTermKey.viewOf(sliceScratch))) {
					continue;
				}
				uniqueSlices.add(FpTermKey.copyOf(sliceScratch));
			}
		}
		for (FpTermKey key : uniqueSlices) {
			final BytesRef br = key.bytesRef();
			int n=br.length;
			
			boolean skip_current=Lucene80FPSearchConfig.COMMON_SKIP_LEN[n];
			if (hotKeySet.contains(key)) {
				continue;
			}
			
			if(!skip_current)
			{
				final int buckets = bucketIndex512(br.bytes, br.offset, n);
				banks[n - 1][buckets].set(bit);
				
				final int buckets1 = bucketIndex256(br.bytes, br.offset, n-1);
				banks[n - 2][buckets1].set(bit);
				final int buckets2 = 256+bucketIndex256(br.bytes, br.offset+1, n-1);
				banks[n - 2][buckets2].set(bit);
			
			}
			
			
			
			
		}
	}

	/**
	 * @param hotDocs       仅当 {@code skipIfInHot} 为 true 时使用：切片若已是热词整键则跳过
	 */
	private static void markHotNgramsForPayload(FixedBitSet[][] banks,
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
		final BytesRef sliceScratch = new BytesRef();
		sliceScratch.bytes = payload.bytes;
		for (int start = 0; start < payloadLen; start++) {
			for (int n = Lucene80FPSearchConfig.NGRAM_MIN; n <= Lucene80FPSearchConfig.NGRAM_MAX && start + n <= payloadLen; n++) {
				sliceScratch.offset = base + start;
				sliceScratch.length = n;
				
				final int bucket = bucketIndex512(sliceScratch.bytes, sliceScratch.offset, sliceScratch.length);
				banks[n - 1][bucket].set(bit);

				
			}
		}
	}

//	/** 热词侧：ngram 长度 1..8 对应 {@code ngramLen}，桶 0..255 */
//	public FixedBitSet getHotBank(int ngramLen, int bucket0To255) {
//		return banksHot[ngramLen - 1][bucket0To255];
//	}
//
//	/** 普通词侧（与热词完全独立） */
//	public FixedBitSet getCommonBank(int ngramLen, int bucket0To255) {
//		return banksCommon[ngramLen - 1][bucket0To255];
//	}
	


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
