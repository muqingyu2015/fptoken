package cn.lxdb.plugins.muqingyu.fptoken.dataset.block;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.TreeMap;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;

import cn.lxdb.plugins.muqingyu.fptoken.config.Lucene80FPSearchConfig;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpBlockInfo;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpLog;

/**
 * 段内 bitindex 暂存：v6 按 lenIdx 分池 skip/keys/order；finish 合并写入 {@code termsbit}。
 *
 * <p>每 lenIdx 一轮 append：hot skip → hot keys → hot order → common skip → common keys → common order。
 * 段尾写每组 hot/common tier 目录（每 len 槽 3 long：skipOff、keysOff、orderOff）。
 */
public final class FpBitIndexSegmentStaging implements AutoCloseable {

	public static final int SEGMENT_MAGIC = 0x4650544B;
	public static final int TIER_DIR_MAGIC_HOT = 0x46505448;
	public static final int TIER_DIR_MAGIC_COMMON = 0x46505443;

	private static final int TIER_DIR_MAGIC_BYTES = 4;
	/** 每个 lenIdx 槽：skip + keys + order 在 termsbit 中的绝对偏移 */
	public static final int TIER_DIR_LONGS_PER_LEN = 3;

	private static final String HOT = "hot";
	private static final String COMMON = "common";
	static final String PART_SKIP = "skip";
	static final String PART_KEYS = "keys";
	static final String PART_ORDER = "order";

	private final FpBitIndexTempDirectory.Session session;
	private final List<StagedGroup> groups = new ArrayList<>();

	public FpBitIndexSegmentStaging(FpBitIndexTempDirectory.Session session) {
		this.session = session;
	}

	public static int tierDirectorySerializedBytes() {
		return TIER_DIR_MAGIC_BYTES + Lucene80FPSearchConfig.NGRAM_MAX * TIER_DIR_LONGS_PER_LEN * Long.BYTES;
	}

	public boolean isEmpty() {
		return groups.isEmpty();
	}

	public void stage(int groupId, FpGroupHotNgramBitIndex bitinfo, String from, BytesRef fieldInfo, int docCount)
			throws IOException {
		final Path groupPath = session.createGroupDir(groupId);
		try (Directory dir = FSDirectory.open(groupPath)) {
			bitinfo.stageToTemp(dir);
		}
		groups.add(new StagedGroup(groupId, from, BytesRef.deepCopyOf(fieldInfo), docCount, bitinfo.targetLevelForWrite(),
				bitinfo.hotCountForWrite(), bitinfo.commonCountForWrite(), bitinfo.hotOrderBytesForWrite(),
				bitinfo.commonOrderBytesForWrite(), groupPath));
	}

	public void finalizeTo(IndexOutput bitOut, TreeMap<Integer, FpBlockInfo> fpblockList) throws IOException {
		if (groups.isEmpty()) {
			return;
		}
		final int n = groups.size();
		final int maxLen = Lucene80FPSearchConfig.NGRAM_MAX;
		final TierLenOffsets[][] hotOff = new TierLenOffsets[n][maxLen];
		final TierLenOffsets[][] commonOff = new TierLenOffsets[n][maxLen];
		for (int gi = 0; gi < n; gi++) {
			for (int li = 0; li < maxLen; li++) {
				hotOff[gi][li] = new TierLenOffsets();
				commonOff[gi][li] = new TierLenOffsets();
			}
		}

		bitOut.writeInt(SEGMENT_MAGIC);
		bitOut.writeInt(FpBlockInfo.FORMAT_VERSION);
		bitOut.writeInt(n);

		final long[] hotSkipBase = new long[maxLen];
		final long[] hotKeysBase = new long[maxLen];
		final long[] hotOrderBase = new long[maxLen];
		final long[] commonSkipBase = new long[maxLen];
		final long[] commonKeysBase = new long[maxLen];
		final long[] commonOrderBase = new long[maxLen];

		for (int lenIdx = 0; lenIdx < maxLen; lenIdx++) {
			hotSkipBase[lenIdx] = bitOut.getFilePointer();
			appendPartPool(bitOut, hotOff, lenIdx, HOT, PART_SKIP);
			hotKeysBase[lenIdx] = bitOut.getFilePointer();
			appendPartPool(bitOut, hotOff, lenIdx, HOT, PART_KEYS);
			hotOrderBase[lenIdx] = bitOut.getFilePointer();
			appendPartPool(bitOut, hotOff, lenIdx, HOT, PART_ORDER);

			commonSkipBase[lenIdx] = bitOut.getFilePointer();
			appendPartPool(bitOut, commonOff, lenIdx, COMMON, PART_SKIP);
			commonKeysBase[lenIdx] = bitOut.getFilePointer();
			appendPartPool(bitOut, commonOff, lenIdx, COMMON, PART_KEYS);
			commonOrderBase[lenIdx] = bitOut.getFilePointer();
			appendPartPool(bitOut, commonOff, lenIdx, COMMON, PART_ORDER);
		}

		for (int lenIdx = 0; lenIdx < maxLen; lenIdx++) {
			bitOut.writeLong(hotSkipBase[lenIdx]);
			bitOut.writeLong(hotKeysBase[lenIdx]);
			bitOut.writeLong(hotOrderBase[lenIdx]);
			bitOut.writeLong(commonSkipBase[lenIdx]);
			bitOut.writeLong(commonKeysBase[lenIdx]);
			bitOut.writeLong(commonOrderBase[lenIdx]);
		}

		for (int gi = 0; gi < n; gi++) {
			final StagedGroup group = groups.get(gi);
			final long hotDirOff = bitOut.getFilePointer();
			writeTierDirectory(bitOut, TIER_DIR_MAGIC_HOT, hotOff[gi]);
			final long commonDirOff = bitOut.getFilePointer();
			writeTierDirectory(bitOut, TIER_DIR_MAGIC_COMMON, commonOff[gi]);

			final FpBlockInfo info = new FpBlockInfo(hotDirOff, commonDirOff,
					(int) (commonDirOff - hotDirOff),
					(int) (bitOut.getFilePointer() - commonDirOff),
					group.hotOrderBytes, group.commonOrderBytes, group.hotCount, group.commonCount, group.targetLevel,
					group.fieldInfo, group.docCount);
			fpblockList.put(group.groupId, info);

			final StringBuilder sb = FpLog.kv();
			FpLog.append(sb, "event", "bitindexStageFinalize");
			FpLog.append(sb, "phase", group.from);
			FpLog.append(sb, "groupId", group.groupId);
			FpLog.append(sb, "format", "v6");
			FpLog.append(sb, "block", info);
			FpLog.infoLine(FpGroupHotNgramBitIndex.LOG, FpLog.TAG_BITINDEX, sb);
		}
	}

	private void appendPartPool(IndexOutput bitOut, TierLenOffsets[][] allOff, int lenIdx, String tier, String part)
			throws IOException {
		long poolBytes = 0L;
		for (int gi = 0; gi < groups.size(); gi++) {
			final long start = bitOut.getFilePointer();
			final long off = copyPartFile(bitOut, groups.get(gi).groupPath, tier, part, lenIdx);
			if (PART_SKIP.equals(part)) {
				allOff[gi][lenIdx].skipOff = off;
			} else if (PART_KEYS.equals(part)) {
				allOff[gi][lenIdx].keysOff = off;
			} else if (PART_ORDER.equals(part)) {
				allOff[gi][lenIdx].orderOff = off;
			}
			poolBytes += bitOut.getFilePointer() - start;
		}
		FpBitIndexDiag.finalizePool(FpGroupHotNgramBitIndex.LOG, tier, lenIdx, part, poolBytes);
	}

	/** @return 文件起始绝对偏移；无文件时为 0 */
	private static long copyPartFile(IndexOutput bitOut, Path groupPath, String tier, String part, int lenIdx)
			throws IOException {
		final Path file = partPath(groupPath, tier, part, lenIdx);
		if (!Files.isRegularFile(file) || Files.size(file) == 0) {
			return 0L;
		}
		final long start = bitOut.getFilePointer();
		try (Directory dir = FSDirectory.open(groupPath);
				IndexInput in = dir.openInput(fileName(tier, part, lenIdx), IOContext.READ)) {
			bitOut.copyBytes(in, in.length());
		}
		return start;
	}

	static String tierDirFileName(String tier) {
		return String.format(Locale.ROOT, "%s_tier_dir.dat", tier);
	}

	/**
	 * 暂存目录尾部写出 tier 目录：magic + 每 len 的 skipOff/keysOff/orderOff。
	 * <p>值为该 tier 内按 len0→len4、每 len 顺序 skip|keys|order 虚拟拼接后的起始字节（便于对照上方 *.dat 列表）。
	 */
	static void writeStagingTierDirectory(Directory dir, String tier, long[][] perLenOff) throws IOException {
		final int magic = HOT.equals(tier) ? TIER_DIR_MAGIC_HOT : TIER_DIR_MAGIC_COMMON;
		try (IndexOutput out = dir.createOutput(tierDirFileName(tier), IOContext.DEFAULT)) {
			out.writeInt(magic);
			for (int lenIdx = 0; lenIdx < Lucene80FPSearchConfig.NGRAM_MAX; lenIdx++) {
				out.writeLong(perLenOff[lenIdx][0]);
				out.writeLong(perLenOff[lenIdx][1]);
				out.writeLong(perLenOff[lenIdx][2]);
			}
		}
	}

	static long partFileSize(Directory dir, String tier, String part, int lenIdx) throws IOException {
		final String name = fileName(tier, part, lenIdx);
		for (String n : dir.listAll()) {
			if (name.equals(n)) {
				try (IndexInput in = dir.openInput(name, IOContext.READ)) {
					return in.length();
				}
			}
		}
		return 0L;
	}

	private static void writeTierDirectory(IndexOutput bitOut, int magic, TierLenOffsets[] perLen) throws IOException {
		bitOut.writeInt(magic);
		for (int lenIdx = 0; lenIdx < Lucene80FPSearchConfig.NGRAM_MAX; lenIdx++) {
			final TierLenOffsets o = perLen[lenIdx];
			bitOut.writeLong(o.skipOff);
			bitOut.writeLong(o.keysOff);
			bitOut.writeLong(o.orderOff);
		}
	}

	static Path partPath(Path groupPath, String tier, String part, int lenIdx) {
		return groupPath.resolve(fileName(tier, part, lenIdx));
	}

	static String fileName(String tier, String part, int lenIdx) {
		return String.format(Locale.ROOT, "%s_%s_%d.dat", tier, part, lenIdx);
	}

	@Override
	public void close() {
	}

	static final class TierLenOffsets {
		long skipOff;
		long keysOff;
		long orderOff;
	}

	private static final class StagedGroup {
		final int groupId;
		final String from;
		final BytesRef fieldInfo;
		final int docCount;
		final int targetLevel;
		final int hotCount;
		final int commonCount;
		final int hotOrderBytes;
		final int commonOrderBytes;
		final Path groupPath;

		StagedGroup(int groupId, String from, BytesRef fieldInfo, int docCount, int targetLevel, int hotCount,
				int commonCount, int hotOrderBytes, int commonOrderBytes, Path groupPath) {
			this.groupId = groupId;
			this.from = from;
			this.fieldInfo = fieldInfo;
			this.docCount = docCount;
			this.targetLevel = targetLevel;
			this.hotCount = hotCount;
			this.commonCount = commonCount;
			this.hotOrderBytes = hotOrderBytes;
			this.commonOrderBytes = commonOrderBytes;
			this.groupPath = groupPath;
		}
	}
}
