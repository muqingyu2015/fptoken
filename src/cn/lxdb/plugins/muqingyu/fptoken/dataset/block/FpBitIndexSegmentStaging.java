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
 * 段内 bitindex 暂存：按 lenIdx 分池，finish 时合并写入 {@code termsbit}（v4 布局）。
 *
 * <p>v4 段布局（写入 {@code bitOut}）：
 * <pre>
 * segmentHeader (magic + version + groupCount + poolBase[6][6])
 * hot/skipkeys/len0..5  各组同 lenIdx 连续拼接
 * hot/arena/len0..5
 * hot/bloom/len0..5
 * common/skipkeys/len0..5
 * common/arena/len0..5
 * common/bloom/len0..5
 * 每组 hot tierDirectory + common tierDirectory（绝对偏移指向上面各池）
 * </pre>
 */
public final class FpBitIndexSegmentStaging implements AutoCloseable {

	/** 段头魔数 {@code 'FPTK'} */
	public static final int SEGMENT_MAGIC = 0x4650544B;
	/** hot tier 目录魔数 {@code 'FPTH'} */
	public static final int TIER_DIR_MAGIC_HOT = 0x46505448;
	/** common tier 目录魔数 {@code 'FPTC'} */
	public static final int TIER_DIR_MAGIC_COMMON = 0x46505443;

	private static final String HOT = "hot";
	private static final String COMMON = "common";
	private static final String SKIP = "skipkeys";
	private static final String ARENA = "arena";
	private static final String BLOOM = "bloom";

	private final FpBitIndexTempDirectory.Session session;
	private final List<StagedGroup> groups = new ArrayList<>();

	public FpBitIndexSegmentStaging(FpBitIndexTempDirectory.Session session) {
		this.session = session;
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
				bitinfo.hotCountForWrite(), bitinfo.commonCountForWrite(), bitinfo.hotArenaBytesForWrite(),
				bitinfo.commonArenaBytesForWrite(), groupPath));
	}

	public void finalizeTo(IndexOutput bitOut, TreeMap<Integer, FpBlockInfo> fpblockList) throws IOException {
		if (groups.isEmpty()) {
			return;
		}
		final int n = groups.size();
		final int maxLen = Lucene80FPSearchConfig.NGRAM_MAX;
		final long[][] hotSkipOff = new long[n][maxLen];
		final long[][] hotArenaOff = new long[n][maxLen];
		final long[][] hotBloomOff = new long[n][maxLen];
		final long[][] commonSkipOff = new long[n][maxLen];
		final long[][] commonArenaOff = new long[n][maxLen];
		final long[][] commonBloomOff = new long[n][maxLen];
		final long[] hotSkipBase = new long[maxLen];
		final long[] hotArenaBase = new long[maxLen];
		final long[] hotBloomBase = new long[maxLen];
		final long[] commonSkipBase = new long[maxLen];
		final long[] commonArenaBase = new long[maxLen];
		final long[] commonBloomBase = new long[maxLen];

		bitOut.writeInt(SEGMENT_MAGIC);
		bitOut.writeInt(FpBlockInfo.FORMAT_VERSION);
		bitOut.writeInt(n);

		for (int lenIdx = 0; lenIdx < maxLen; lenIdx++) {
			hotSkipBase[lenIdx] = bitOut.getFilePointer();
			appendLenIdxPool(bitOut, hotSkipOff, lenIdx, HOT, SKIP);
			hotArenaBase[lenIdx] = bitOut.getFilePointer();
			appendLenIdxPool(bitOut, hotArenaOff, lenIdx, HOT, ARENA);
			hotBloomBase[lenIdx] = bitOut.getFilePointer();
			appendLenIdxPool(bitOut, hotBloomOff, lenIdx, HOT, BLOOM);
			commonSkipBase[lenIdx] = bitOut.getFilePointer();
			appendLenIdxPool(bitOut, commonSkipOff, lenIdx, COMMON, SKIP);
			commonArenaBase[lenIdx] = bitOut.getFilePointer();
			appendLenIdxPool(bitOut, commonArenaOff, lenIdx, COMMON, ARENA);
			commonBloomBase[lenIdx] = bitOut.getFilePointer();
			appendLenIdxPool(bitOut, commonBloomOff, lenIdx, COMMON, BLOOM);
		}
		for (int lenIdx = 0; lenIdx < maxLen; lenIdx++) {
			bitOut.writeLong(hotSkipBase[lenIdx]);
			bitOut.writeLong(hotArenaBase[lenIdx]);
			bitOut.writeLong(hotBloomBase[lenIdx]);
			bitOut.writeLong(commonSkipBase[lenIdx]);
			bitOut.writeLong(commonArenaBase[lenIdx]);
			bitOut.writeLong(commonBloomBase[lenIdx]);
		}

		for (int gi = 0; gi < n; gi++) {
			final StagedGroup group = groups.get(gi);
			final long hotDirOff = bitOut.getFilePointer();
			writeTierDirectory(bitOut, TIER_DIR_MAGIC_HOT, hotSkipOff[gi], hotArenaOff[gi], hotBloomOff[gi]);
			final long commonDirOff = bitOut.getFilePointer();
			writeTierDirectory(bitOut, TIER_DIR_MAGIC_COMMON, commonSkipOff[gi], commonArenaOff[gi],
					commonBloomOff[gi]);

			final FpBlockInfo info = new FpBlockInfo(hotDirOff, commonDirOff,
					(int) (commonDirOff - hotDirOff),
					(int) (bitOut.getFilePointer() - commonDirOff),
					group.hotArenaBytes, group.commonArenaBytes, group.hotCount, group.commonCount, group.targetLevel,
					group.fieldInfo, group.docCount);
			fpblockList.put(group.groupId, info);

			final StringBuilder sb = FpLog.kv();
			FpLog.append(sb, "event", "bitindexStageFinalize");
			FpLog.append(sb, "phase", group.from);
			FpLog.append(sb, "groupId", group.groupId);
			FpLog.append(sb, "block", info);
			FpLog.infoLine(FpGroupHotNgramBitIndex.LOG, FpLog.TAG_BITINDEX, sb);
		}
	}

	private void appendLenIdxPool(IndexOutput bitOut, long[][] offsets, int lenIdx, String tier, String part)
			throws IOException {
		for (int gi = 0; gi < groups.size(); gi++) {
			offsets[gi][lenIdx] = bitOut.getFilePointer();
			copyPartFile(bitOut, groups.get(gi).groupPath, tier, part, lenIdx);
		}
	}

	private static void copyPartFile(IndexOutput bitOut, Path groupPath, String tier, String part, int lenIdx)
			throws IOException {
		final Path file = partPath(groupPath, tier, part, lenIdx);
		if (!Files.isRegularFile(file)) {
			return;
		}
		try (Directory dir = FSDirectory.open(groupPath);
				IndexInput in = dir.openInput(fileName(tier, part, lenIdx), IOContext.READ)) {
			bitOut.copyBytes(in, in.length());
		}
	}

	private static void writeTierDirectory(IndexOutput bitOut, int magic, long[] skipOff, long[] arenaOff,
			long[] bloomOff) throws IOException {
		bitOut.writeInt(magic);
		for (int lenIdx = 0; lenIdx < Lucene80FPSearchConfig.NGRAM_MAX; lenIdx++) {
			bitOut.writeLong(skipOff[lenIdx]);
			bitOut.writeLong(arenaOff[lenIdx]);
			bitOut.writeLong(bloomOff[lenIdx]);
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
		// 会话目录由 FpBitIndexTempDirectory.Session 负责关闭删除
	}

	private static final class StagedGroup {
		final int groupId;
		final String from;
		final BytesRef fieldInfo;
		final int docCount;
		final int targetLevel;
		final int hotCount;
		final int commonCount;
		final int hotArenaBytes;
		final int commonArenaBytes;
		final Path groupPath;

		StagedGroup(int groupId, String from, BytesRef fieldInfo, int docCount, int targetLevel, int hotCount,
				int commonCount, int hotArenaBytes, int commonArenaBytes, Path groupPath) {
			this.groupId = groupId;
			this.from = from;
			this.fieldInfo = fieldInfo;
			this.docCount = docCount;
			this.targetLevel = targetLevel;
			this.hotCount = hotCount;
			this.commonCount = commonCount;
			this.hotArenaBytes = hotArenaBytes;
			this.commonArenaBytes = commonArenaBytes;
			this.groupPath = groupPath;
		}
	}
}
