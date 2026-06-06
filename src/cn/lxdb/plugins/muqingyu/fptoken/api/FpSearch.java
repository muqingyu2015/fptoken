package cn.lxdb.plugins.muqingyu.fptoken.api;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.index.TermsEnum$SeekStatus;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.offheap.OffheapPoolName;
import org.slf4j.Logger;

import cn.lucene.lxdb.params.LxdbLogerEncrypt;
import cn.lxdb.plugins.muqingyu.fptoken.config.FpTokenBlockLevelPolicy;
import cn.lxdb.plugins.muqingyu.fptoken.config.Lucene80FPSearchConfig;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.block.FpGroupHotNgramBitIndex;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpBlockInfo;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpLog;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpSearchStat;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpTokenTermLayout;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.Utils;

/**
 * 查询侧：按 ngram {@link BytesRef} 切片在组内 hot/common 精确 bucketIndex 倒排中定位 order 列表，再
 * {@link Terms#iterator_fp()} seek 倒排并合并 doc，多切片 AND、同切片多组 OR。
 */
public class FpSearch {

	public FpSearchStat stat;
	public final String DEBUG_UUID;

	public FpSearch(FpSearchStat stat) {
		this(stat, "");
	}

	public FpSearch(FpSearchStat stat, String traceId) {
		this.stat = stat;
		if (traceId != null && !traceId.isEmpty()) {
			this.DEBUG_UUID = traceId;
		} else if (Lucene80FPSearchConfig.LOG_FP_SEARCH || Lucene80FPSearchConfig.PRINT_DEBUG) {
			this.DEBUG_UUID = java.util.UUID.randomUUID().toString();
		} else {
			this.DEBUG_UUID = "";
		}
	}

	public static final Logger LOG = LxdbLogerEncrypt.getLogger("mqy.fptoken");

	/**
	 * @param fpblock_list 段内 group_id → 位图区元数据
	 * @param slices       查询串滑窗切片（长度须在 {@link Lucene80FPSearchConfig#NGRAM_MIN}..{@link Lucene80FPSearchConfig#NGRAM_MAX}）
	 * @return 同时命中全部 slice 的 doc 集合（AND）；无切片或任一切片无命中则返回空集
	 */
	public FixedBitSet search( TreeMap<Integer, FpBlockInfo> fpblock_list, Terms terms, int maxDoc, BytesRef columnName,
			BytesRef[] slices) throws IOException {

		stat.doccount+=maxDoc;
		final FixedBitSet rtn = new FixedBitSet(OffheapPoolName.fptokenbitset, maxDoc);
		if (slices == null || slices.length == 0) {
			if (Lucene80FPSearchConfig.LOG_FP_SEARCH) {
				final StringBuilder sb = FpLog.kv();
				FpLog.append(sb, "event", "searchAbort");
				FpLog.append(sb, "reason", "noSlices");
				FpLog.append(sb, "column", Utils.BytesReftoString(columnName));
				LOG.info(FpLog.trace(DEBUG_UUID, FpLog.TAG_SEARCH, sb));
			}
			return rtn;
		}

		final FixedBitSet[] collect = new FixedBitSet[slices.length];
		int maxGroupId = -1;
		int columnMatchedGroups = 0;
		int columnSkippedGroups = 0;
		String indexColumnSample = null;
		final long[] bucketKeys = FpGroupHotNgramBitIndex.selectiveKeysForSlices(slices);
		if (Lucene80FPSearchConfig.LOG_FP_SEARCH || Lucene80FPSearchConfig.PRINT_DEBUG) {
			final StringBuilder sb = FpLog.kv();
			FpLog.append(sb, "event", "searchBegin");
			FpLog.append(sb, "column", Utils.BytesReftoString(columnName));
			FpLog.append(sb, "maxDoc", maxDoc);
			FpLog.appendSliceSummary(sb, slices);
			FpLog.append(sb, "bucketKeyCount", bucketKeys.length);
			FpLog.appendBucketKeys(sb, bucketKeys);
			FpLog.append(sb, "fpGroupCount", fpblock_list == null ? 0 : fpblock_list.size());
			LOG.info(FpLog.trace(DEBUG_UUID, FpLog.TAG_SEARCH, sb));
		}

		for (Entry<Integer, FpBlockInfo> e : fpblock_list.entrySet()) {
			final FpBlockInfo blkinfo = e.getValue();
			if (indexColumnSample == null && blkinfo.fieldInfo != null) {
				indexColumnSample = Utils.BytesReftoString(blkinfo.fieldInfo);
			}
			if (!fieldInfoMatchesColumn(blkinfo, columnName)) {
				columnSkippedGroups++;
				continue;
			}
			columnMatchedGroups++;
			stat.blkCount[blkinfo.targetLevel]+=slices.length;
			final int groupId = e.getKey().intValue();
			if (groupId > maxGroupId) {
				maxGroupId = groupId;
			}
			

			final FpGroupHotNgramBitIndex bitsetIndex = loadBitIndex(terms, groupId, blkinfo, bucketKeys, slices);
			if (bitsetIndex == null) {
				if (Lucene80FPSearchConfig.LOG_FP_SEARCH || Lucene80FPSearchConfig.PRINT_DEBUG) {
					final StringBuilder sb = FpLog.kv();
					FpLog.append(sb, "event", "skipGroup");
					FpLog.append(sb, "reason", "fpBitsNull");
					FpLog.append(sb, "groupId", groupId);
					FpLog.append(sb, "level", "L" + blkinfo.targetLevel);
					LOG.info(FpLog.trace(DEBUG_UUID, FpLog.TAG_SEARCH, sb));
				}
				continue;
			}

			for (int i = 0; i < slices.length; i++) {
				searchSliceInGroup(bitsetIndex, columnName, slices[i], blkinfo, groupId, terms, maxDoc, collect, i);
			}
		}

		if (Lucene80FPSearchConfig.LOG_FP_SEARCH && columnMatchedGroups == 0 && fpblock_list.size() > 0) {
			final StringBuilder sb = FpLog.kv();
			FpLog.append(sb, "event", "columnMismatch");
			FpLog.append(sb, "queryColumn", Utils.BytesReftoString(columnName));
			FpLog.append(sb, "indexColumnSample", indexColumnSample);
			FpLog.append(sb, "fpGroups", fpblock_list.size());
			FpLog.append(sb, "skippedGroups", columnSkippedGroups);
			LOG.info(FpLog.trace(DEBUG_UUID, FpLog.TAG_SEARCH, sb));
		}

		searchSparseNoBitIndexTerms(terms, columnName, maxDoc, slices, collect, maxGroupId);

		boolean merged = false;
		for (int i = 0; i < slices.length; i++) {
			if (collect[i] == null) {
				if (Lucene80FPSearchConfig.LOG_FP_SEARCH || Lucene80FPSearchConfig.PRINT_DEBUG) {
					final StringBuilder sb = FpLog.kv();
					FpLog.append(sb, "event", "searchAbort");
					FpLog.append(sb, "reason", "sliceNoHit");
					FpLog.append(sb, "sliceIdx", i);
					FpLog.append(sb, "slice", Utils.BytesReftoString(slices[i]));
					FpLog.append(sb, "columnMatchedGroups", columnMatchedGroups);
					LOG.info(FpLog.trace(DEBUG_UUID, FpLog.TAG_SEARCH, sb));
				}
				rtn.clear(0, maxDoc);
				return rtn;
			}
			if (!merged) {
				rtn.or(collect[i]);
				merged = true;
			} else {
				rtn.and(collect[i]);
			}
		}
		if (Lucene80FPSearchConfig.LOG_FP_SEARCH || Lucene80FPSearchConfig.PRINT_DEBUG) {
			final StringBuilder sb = FpLog.kv();
			FpLog.append(sb, "event", "searchEnd");
			FpLog.append(sb, "hitDocs", rtn.cardinality());
			FpLog.append(sb, "columnMatchedGroups", columnMatchedGroups);
			FpLog.append(sb, "stat", stat);
			LOG.info(FpLog.trace(DEBUG_UUID, FpLog.TAG_SEARCH, sb));
		}
		return rtn;
	}

	/**
	 * 加载组位图（selective {@code fpBits}）。失败时不默认全量读；见 {@link #diagnoseSelectiveLoad}。
	 * 仅当 {@link Lucene80FPSearchConfig#SELECTIVE_FP_BITS_FALLBACK} 显式开启时才回退全量读。
	 */
	private FpGroupHotNgramBitIndex loadBitIndex(Terms terms, int groupId, FpBlockInfo blkinfo, long[] bucketKeys,
			BytesRef[] slices) throws IOException {
		final FpGroupHotNgramBitIndex selective = terms.fpBits(Lucene80FPSearchConfig.DEFAULT_INDEX_ID, groupId,
				bucketKeys, bucketKeys);
//		if (selective == null) {
//			return null;
//		}
//		final boolean selectiveLookupEmpty = !selectiveLookupHitsAnySlice(selective, slices);
//		if (Lucene80FPSearchConfig.LOG_FP_SEARCH) {
//			final boolean emptyTier = selective.isSparse()
//					&& selective.loadedHotBucketCount() + selective.loadedCommonBucketCount() == 0
//					&& (blkinfo.hotCount > 0 || blkinfo.commonCount > 0);
//			if (selectiveLookupEmpty || emptyTier) {
//				diagnoseSelectiveLoad(terms, groupId, blkinfo, bucketKeys, slices, selective, selectiveLookupEmpty);
//			}
//		}
//		if (!Lucene80FPSearchConfig.SELECTIVE_FP_BITS_FALLBACK || !selectiveLookupEmpty) {
			return selective;
//		}
//		if (Lucene80FPSearchConfig.LOG_FP_SEARCH || Lucene80FPSearchConfig.PRINT_DEBUG) {
//			final StringBuilder sb = FpLog.kv();
//			FpLog.append(sb, "event", "selectiveFallback");
//			FpLog.append(sb, "note", "SELECTIVE_FP_BITS_FALLBACK=true");
//			FpLog.append(sb, "groupId", groupId);
//			FpLog.appendBucketKeys(sb, bucketKeys);
//			FpLog.appendSliceSummary(sb, slices);
//			LOG.info(FpLog.trace(DEBUG_UUID, FpLog.TAG_SEARCH, sb));
//		}
//		return terms.fpBits(Lucene80FPSearchConfig.DEFAULT_INDEX_ID, groupId, null, null);
	}

	/**
	 * selective 读盘/lookup 异常诊断（不改查询路径）。
	 * <ul>
	 *   <li>{@code selectiveTierEmpty} — sparse 实例但 hot/common 已加载 bucket 均为 0，块元数据却有词</li>
	 *   <li>{@code selectiveBucketMiss} — sparse 有数据但请求的 bucketKey 未命中</li>
	 *   <li>{@code selectiveIoBroken} — 对比全量读：全量 lookup 有 order、selective 无（Lucene selective IO 问题）</li>
	 *   <li>{@code ngramAbsent} — 对比全量读：全量也无 order（语料无此 ngram 或 query 字节不一致）</li>
	 *   <li>{@code fullIndexLookupMiss} — 非 sparse 全量实例 lookup 为空</li>
	 * </ul>
	 */
//	private void diagnoseSelectiveLoad(Terms terms, int groupId, FpBlockInfo blkinfo, long[] bucketKeys,
//			BytesRef[] slices, FpGroupHotNgramBitIndex selective, boolean selectiveLookupEmpty) throws IOException {
//		final int loadedHot = selective.loadedHotBucketCount();
//		final int loadedCommon = selective.loadedCommonBucketCount();
//		final StringBuilder sb = FpLog.kv();
//		FpLog.append(sb, "event", "selectiveLoadDiagnosis");
//		FpLog.append(sb, "groupId", groupId);
//		FpLog.append(sb, "level", "L" + blkinfo.targetLevel);
//		FpLog.append(sb, "sparse", selective.isSparse());
//		FpLog.append(sb, "indexHotTerms", blkinfo.hotCount);
//		FpLog.append(sb, "indexCommonTerms", blkinfo.commonCount);
//		FpLog.append(sb, "loadedHotBuckets", loadedHot);
//		FpLog.append(sb, "loadedCommonBuckets", loadedCommon);
//		FpLog.append(sb, "fpBanksHot", blkinfo.fpBanksHot);
//		FpLog.append(sb, "fpBanksCommon", blkinfo.fpBanksCommon);
//		FpLog.appendBucketKeys(sb, bucketKeys);
//		FpLog.appendSliceSummary(sb, slices);
//
//		String reason;
//		int fullHotOrders = -1;
//		int fullCommonOrders = -1;
//		if (selective.isSparse() && loadedHot == 0 && loadedCommon == 0
//				&& (blkinfo.hotCount > 0 || blkinfo.commonCount > 0)) {
//			reason = "selectiveTierEmpty";
//		} else if (selective.isSparse() && selectiveLookupEmpty && (loadedHot > 0 || loadedCommon > 0)) {
//			reason = "selectiveBucketMiss";
//		} else if (!selective.isSparse() && selectiveLookupEmpty) {
//			reason = "fullIndexLookupMiss";
//		} else {
//			reason = "selectiveLookupEmpty";
//		}
//
//		if (Lucene80FPSearchConfig.SELECTIVE_FP_BITS_DIAG_COMPARE && selectiveLookupEmpty) {
//			final FpGroupHotNgramBitIndex full = terms.fpBits(Lucene80FPSearchConfig.DEFAULT_INDEX_ID, groupId, null,
//					null);
//			if (full != null && slices != null && slices.length > 0 && slices[0] != null) {
//				fullHotOrders = full.lookupHotOrders(slices[0]).length;
//				fullCommonOrders = full.lookupCommonOrders(slices[0]).length;
//				FpLog.append(sb, "compareFullHotOrders", fullHotOrders);
//				FpLog.append(sb, "compareFullCommonOrders", fullCommonOrders);
//				FpLog.append(sb, "compareFullSparse", full.isSparse());
//				if (fullHotOrders > 0 || fullCommonOrders > 0) {
//					reason = "selectiveIoBroken";
//				} else if (selective.isSparse()) {
//					reason = "ngramAbsent";
//				}
//			}
//		}
//		FpLog.append(sb, "diagReason", reason);
//		LOG.info(FpLog.trace(DEBUG_UUID, FpLog.TAG_SEARCH, sb));
//	}

//	private static boolean selectiveLookupHitsAnySlice(FpGroupHotNgramBitIndex bitIndex, BytesRef[] slices) {
//		for (BytesRef slice : slices) {
//			if (slice == null) {
//				continue;
//			}
//			if (bitIndex.lookupHotOrders(slice).length > 0 || bitIndex.lookupCommonOrders(slice).length > 0) {
//				return true;
//			}
//		}
//		return false;
//	}

	private void searchSliceInGroup(FpGroupHotNgramBitIndex bitIndex, BytesRef columnName, BytesRef anchorSlice,
			FpBlockInfo blkinfo, int groupid, Terms terms, int maxDoc, FixedBitSet[] collect, int sliceIndex)
			throws IOException {
		final FixedBitSet acc = ensureCollect(collect, sliceIndex, maxDoc);
		final int[] hotOrders = bitIndex.lookupHotOrders(anchorSlice);
		final int[] commonOrders = bitIndex.lookupCommonOrders(anchorSlice);
		if (Lucene80FPSearchConfig.LOG_FP_SEARCH && hotOrders.length == 0 && commonOrders.length == 0) {
			final StringBuilder sb = FpLog.kv();
			FpLog.append(sb, "event", "sliceLookupMiss");
			FpLog.append(sb, "groupId", groupid);
			FpLog.append(sb, "level", "L" + blkinfo.targetLevel);
			FpLog.append(sb, "sparse", bitIndex.isSparse());
			FpLog.append(sb, "loadedHotBuckets", bitIndex.loadedHotBucketCount());
			FpLog.append(sb, "loadedCommonBuckets", bitIndex.loadedCommonBucketCount());
			FpLog.append(sb, "sliceIdx", sliceIndex);
			FpLog.append(sb, "slice", Utils.BytesReftoString(anchorSlice));
			if (anchorSlice != null && anchorSlice.length > 0) {
				FpLog.append(sb, "sliceHex",
						FpLog.bytesToHex(anchorSlice.bytes, anchorSlice.offset, anchorSlice.length));
			}
			FpLog.append(sb, "lenIdx", anchorSlice.length - 1);
			FpLog.append(sb, "bucket", FpGroupHotNgramBitIndex.bucketIndex(anchorSlice));
			FpLog.append(sb, "bucketKey", Long.toHexString(
					FpGroupHotNgramBitIndex.packBucketKey(anchorSlice.length - 1,
							FpGroupHotNgramBitIndex.bucketIndex(anchorSlice))));
			LOG.info(FpLog.trace(DEBUG_UUID, FpLog.TAG_SEARCH, sb));
		}
		if (Lucene80FPSearchConfig.PRINT_DEBUG) {
			final StringBuilder sb = FpLog.kv();
			FpLog.append(sb, "event", "sliceInGroup");
			FpLog.append(sb, "groupId", groupid);
			FpLog.append(sb, "level", "L" + blkinfo.targetLevel);
			FpLog.append(sb, "sliceIdx", sliceIndex);
			FpLog.append(sb, "slice", Utils.BytesReftoString(anchorSlice));
			FpLog.append(sb, "hotOrderCount", hotOrders.length);
			FpLog.append(sb, "commonOrderCount", commonOrders.length);
			LOG.info(FpLog.trace(DEBUG_UUID, FpLog.TAG_SEARCH, sb));
		}
		final boolean hotHit = searchOrdersHot(bitIndex, columnName, anchorSlice, blkinfo, groupid, terms, maxDoc, acc,
				hotOrders);
		if (Lucene80FPSearchConfig.PRINT_DEBUG) {
			final StringBuilder sb = FpLog.kv();
			FpLog.append(sb, "event", "searchOrdersHot");
			FpLog.append(sb, "hit", hotHit);
			FpLog.append(sb, "slice", Utils.BytesReftoString(anchorSlice));
			FpLog.append(sb, "groupId", groupid);
			LOG.info(FpLog.trace(DEBUG_UUID, FpLog.TAG_SEARCH, sb));
		}
		if (!hotHit) {
			final boolean commonHit = searchOrdersCommon(bitIndex, columnName, anchorSlice, blkinfo, groupid, terms,
					maxDoc, acc, commonOrders);
			if (Lucene80FPSearchConfig.PRINT_DEBUG) {
				final StringBuilder sb = FpLog.kv();
				FpLog.append(sb, "event", "searchOrdersCommon");
				FpLog.append(sb, "hit", commonHit);
				FpLog.append(sb, "slice", Utils.BytesReftoString(anchorSlice));
				FpLog.append(sb, "groupId", groupid);
				LOG.info(FpLog.trace(DEBUG_UUID, FpLog.TAG_SEARCH, sb));
			}
		}
	}

	private static FixedBitSet ensureCollect(FixedBitSet[] collect, int sliceIndex, int maxDoc) {
		if (collect[sliceIndex] == null) {
			collect[sliceIndex] = new FixedBitSet(OffheapPoolName.fptokenbitset, maxDoc);
		}
		return collect[sliceIndex];
	}

	private boolean searchOrdersHot(FpGroupHotNgramBitIndex bitIndex, BytesRef columnName, BytesRef anchorSlice,
			FpBlockInfo blkinfo, int groupid, Terms terms, int maxDoc, FixedBitSet collect, int[] orders)
			throws IOException {
		return seekOrderList(orders, true, columnName, anchorSlice, blkinfo, groupid, terms, maxDoc, collect);
	}

	private boolean searchOrdersCommon(FpGroupHotNgramBitIndex bitIndex, BytesRef columnName, BytesRef anchorSlice,
			FpBlockInfo blkinfo, int groupid, Terms terms, int maxDoc, FixedBitSet collect, int[] orders)
			throws IOException {
		return seekOrderList(orders, false, columnName, anchorSlice, blkinfo, groupid, terms, maxDoc, collect);
	}

	private boolean seekOrderList(int[] orders, boolean hotMark, BytesRef columnName, BytesRef anchorSlice,
			FpBlockInfo blkinfo, int groupid, Terms terms, int maxDoc, FixedBitSet collect) throws IOException {
		if (orders == null || orders.length == 0) {
			return false;
		}
		final AtomicReference<PostingsEnum> reusePosting = new AtomicReference<PostingsEnum>(null);
		final TermsEnum termsEnum = terms.iterator();
		final BytesRef reuse = new BytesRef(new byte[512]);
		boolean anyHit = false;
		final AtomicInteger maxHotPayloadLen = new AtomicInteger(0);
		boolean payloadLenCapSet = false;
		boolean first = false;
		for (int termIndex : orders) {
			if (hotMark) {
				stat.bitHitHot[blkinfo.targetLevel]++;
			} else {
				stat.bitHitCommon[blkinfo.targetLevel]++;
			}
			if (!first) {
				if (hotMark) {
					stat.blkHitHot[blkinfo.targetLevel]++;
				} else {
					stat.blkHitCommon[blkinfo.targetLevel]++;
				}
				first = true;
			}
			if (hotMark && !payloadLenCapSet) {
				final int status = seekTermAndOrDocs(reusePosting, maxHotPayloadLen, true, termsEnum, reuse,
						Lucene80FPSearchConfig.DEFAULT_INDEX_ID, groupid, (byte) blkinfo.targetLevel, hotMark,
						termIndex, columnName, anchorSlice, maxDoc, collect);
				if (status == SEEK_OK) {
					anyHit = true;
					payloadLenCapSet = true;
				}
				continue;
			}
			final int status = seekTermAndOrDocs(reusePosting, hotMark ? maxHotPayloadLen : null, false, termsEnum,
					reuse, Lucene80FPSearchConfig.DEFAULT_INDEX_ID, groupid, (byte) blkinfo.targetLevel, hotMark,
					termIndex, columnName, anchorSlice, maxDoc, collect);
			if (status == SEEK_OK) {
				anyHit = true;
			}
			if (status == SEEK_BREAK_PAYLOAD_LEN) {
				break;
			}
		}
		return anyHit;
	}

	private static boolean fieldInfoMatchesColumn(FpBlockInfo blkinfo, BytesRef columnName) {
		if (blkinfo == null || blkinfo.fieldInfo == null) {
			return false;
		}
		return blkinfo.fieldInfo.equals(columnName);
	}

	/**
	 * 稀疏列（写段时 common 词数 ≤ {@link FpTokenBlockLevelPolicy#NO_INDEX_THRESHOLD}、无 ngram 位图）：
	 * 从 {@code (column, index_id, group_id, level=0, …)} seek 起扫描，校验头字段后按 payload 子串命中 OR 进各 slice 的 doc 集。
	 */
	private void searchSparseNoBitIndexTerms(Terms terms, BytesRef columnName, int maxDoc, BytesRef[] slices,
			FixedBitSet[] collect, int maxGroupId) throws IOException {
		final short indexId = Lucene80FPSearchConfig.DEFAULT_INDEX_ID;
		final AtomicReference<PostingsEnum> reusePosting = new AtomicReference<PostingsEnum>(null);
		final TermsEnum termsEnum = terms.iterator();
		final BytesRef reuse = new BytesRef(new byte[512]);

		// 稀疏列 term 的 group_level=0，按列+index_id+group_id 字典序分散；从 group_id=0 起扫，勿用 maxGroupId 作下界。
		FpTokenTermLayout.make_fp_search_prefix(reuse, columnName, indexId, maxGroupId+1,
				(byte) FpTokenBlockLevelPolicy.BLOCK_LEVEL_NOGROUP, false, 0);
		if (termsEnum.seekCeil(reuse) == TermsEnum$SeekStatus.END) {
			stat.termMiss0++;
			if (Lucene80FPSearchConfig.PRINT_DEBUG) {
				final StringBuilder sb = FpLog.kv();
				FpLog.append(sb, "event", "sparseScanMiss");
				FpLog.append(sb, "reason", "seekCeilEnd");
				FpLog.append(sb, "column", Utils.BytesReftoString(columnName));
				LOG.info(FpLog.trace(DEBUG_UUID, FpLog.TAG_SEARCH, sb));
			}
			return;
		}

		if (Lucene80FPSearchConfig.PRINT_DEBUG) {
			final StringBuilder sb = FpLog.kv();
			FpLog.append(sb, "event", "sparseScanBegin");
			FpLog.append(sb, "column", Utils.BytesReftoString(columnName));
			FpLog.append(sb, "maxGroupId", maxGroupId);
			LOG.info(FpLog.trace(DEBUG_UUID, FpLog.TAG_SEARCH, sb));
		}

		for (BytesRef found = termsEnum.term(); found != null; found = termsEnum.next()) {
			if (!FpTokenTermLayout.columnNameEquals(found, columnName)) {
				
				break;
			}
			
			final int level = FpTokenTermLayout.readLevel(found);
			if (level != FpTokenBlockLevelPolicy.BLOCK_LEVEL_NOGROUP) {
				continue;
			}
			
			

			final BytesRef payload = FpTokenTermLayout.removeColumnAndHeaderBytes(found);
			for (int i = 0; i < slices.length; i++) {
				if (!payloadContainsSlice(payload, slices[i])) {
					stat.termMiss0++;
					continue;
				}
				stat.termHit0++;
				final FixedBitSet acc = ensureCollect(collect, i, maxDoc);
				orPostingsInto(reusePosting, termsEnum, maxDoc, acc,false);
			}
		}
	}

	/** payload 是否包含 slice 连续子串（indexOf 语义）。 */
	private static boolean payloadContainsSlice(BytesRef payload, BytesRef slice) {
		return payloadMatchesSlice(false, payload, slice);
	}

	private static final int SEEK_OK = 0;
	private static final int SEEK_BREAK_PAYLOAD_LEN = 1;
	private static final int SEEK_MISS = 2;

	private static String seekStatusLabel(int status) {
		if (status == SEEK_OK) {
			return "OK";
		}
		if (status == SEEK_BREAK_PAYLOAD_LEN) {
			return "BREAK_PAYLOAD_LEN";
		}
		if (status == SEEK_MISS) {
			return "MISS";
		}
		return String.valueOf(status);
	}

	private  int seekTermAndOrDocs(AtomicReference<PostingsEnum> reusePosting,
			AtomicInteger maxHotPayloadLenOrNull, boolean requireExactPayloadMatch, TermsEnum termsEnum, BytesRef reuse,
			short indexId, int groupid, byte groupLevel, boolean hotMark, int termIndex, BytesRef columnName,
			BytesRef slice, int maxDoc, FixedBitSet collect) throws IOException {
		FpTokenTermLayout.make_fp_search_prefix(reuse, columnName, indexId, groupid, groupLevel, hotMark, termIndex);

		if (Lucene80FPSearchConfig.PRINT_DEBUG) {
			final StringBuilder sb = FpLog.kv();
			FpLog.append(sb, "event", "seekCeil");
			FpLog.append(sb, "indexId", indexId);
			FpLog.append(sb, "groupId", groupid);
			FpLog.append(sb, "level", "L" + groupLevel);
			FpLog.append(sb, "hot", hotMark);
			FpLog.append(sb, "termIndex", termIndex);
			FpLog.append(sb, "exactPayload", requireExactPayloadMatch);
			FpLog.append(sb, "slice", Utils.BytesReftoString(slice));
			LOG.info(FpLog.trace(DEBUG_UUID, FpLog.TAG_SEARCH, sb));
		}
		if (termsEnum.seekCeil(reuse) == TermsEnum$SeekStatus.END) {
			if (hotMark) {
				stat.termMissHot1[groupLevel]++;
			} else {
				stat.termMissCommon1[groupLevel]++;
			}
			if (Lucene80FPSearchConfig.PRINT_DEBUG) {
				final StringBuilder sb = FpLog.kv();
				FpLog.append(sb, "event", "seekResult");
				FpLog.append(sb, "status", seekStatusLabel(SEEK_MISS));
				FpLog.append(sb, "stage", "seekCeilEnd");
				FpLog.append(sb, "groupId", groupid);
				FpLog.append(sb, "termIndex", termIndex);
				FpLog.append(sb, "hot", hotMark);
				LOG.info(FpLog.trace(DEBUG_UUID, FpLog.TAG_SEARCH, sb));
			}
			return SEEK_MISS;
		}
		final BytesRef found = termsEnum.term();
		boolean isDelTerm = FpTokenTermLayout.readIsDelTerm(found);
		if (Lucene80FPSearchConfig.PRINT_DEBUG) {
			final StringBuilder sb = FpLog.kv();
			FpLog.append(sb, "event", "seekResult");
			FpLog.append(sb, "status", "FOUND");
			FpLog.append(sb, "groupId", groupid);
			FpLog.append(sb, "termIndex", termIndex);
			FpLog.append(sb, "hot", hotMark);
			FpLog.append(sb, "isDel", isDelTerm);
			FpLog.append(sb, "term", FpTokenTermLayout.toReadableString(found));
			LOG.info(FpLog.trace(DEBUG_UUID, FpLog.TAG_SEARCH, sb));
		}
		if (!termHeaderMatches(found, columnName, groupid, groupLevel, hotMark, termIndex)) {
			if (hotMark) {
				stat.termMissHot2[groupLevel]++;
			} else {
				stat.termMissCommon2[groupLevel]++;
			}
			if (Lucene80FPSearchConfig.PRINT_DEBUG) {
				final StringBuilder sb = FpLog.kv();
				FpLog.append(sb, "event", "seekResult");
				FpLog.append(sb, "status", seekStatusLabel(SEEK_MISS));
				FpLog.append(sb, "stage", "headerMismatch");
				FpLog.append(sb, "groupId", groupid);
				FpLog.append(sb, "termIndex", termIndex);
				LOG.info(FpLog.trace(DEBUG_UUID, FpLog.TAG_SEARCH, sb));
			}
			return SEEK_MISS;
		}
		final BytesRef payload = FpTokenTermLayout.removeColumnAndHeaderBytes(found);
		if (!payloadMatchesSlice(requireExactPayloadMatch, payload, slice)) {
			if (hotMark) {
				stat.termMissHot3[groupLevel]++;
			} else {
				stat.termMissCommon3[groupLevel]++;
			}
			if (Lucene80FPSearchConfig.PRINT_DEBUG) {
				final StringBuilder sb = FpLog.kv();
				FpLog.append(sb, "event", "seekResult");
				FpLog.append(sb, "status", seekStatusLabel(SEEK_MISS));
				FpLog.append(sb, "stage", "payloadMismatch");
				FpLog.append(sb, "groupId", groupid);
				FpLog.append(sb, "termIndex", termIndex);
				FpLog.append(sb, "payload", Utils.BytesReftoString(payload));
				FpLog.append(sb, "slice", Utils.BytesReftoString(slice));
				LOG.info(FpLog.trace(DEBUG_UUID, FpLog.TAG_SEARCH, sb));
			}
			return SEEK_MISS;
		}

		if (hotMark && maxHotPayloadLenOrNull != null) {
			if (requireExactPayloadMatch) {
				maxHotPayloadLenOrNull.set(FpTokenTermLayout.maxHotPayloadLenFromHeader(found, payload.length));
			}
			if (maxHotPayloadLenOrNull.get() < payload.length) {
				if (Lucene80FPSearchConfig.PRINT_DEBUG) {
					final StringBuilder sb = FpLog.kv();
					FpLog.append(sb, "event", "seekResult");
					FpLog.append(sb, "status", seekStatusLabel(SEEK_BREAK_PAYLOAD_LEN));
					FpLog.append(sb, "maxHotPayloadLen", maxHotPayloadLenOrNull.get());
					FpLog.append(sb, "payloadLen", payload.length);
					LOG.info(FpLog.trace(DEBUG_UUID, FpLog.TAG_SEARCH, sb));
				}
				return SEEK_BREAK_PAYLOAD_LEN;
			}
		}

		if (!isDelTerm) {
			orPostingsInto(reusePosting, termsEnum, maxDoc, collect,hotMark);
		}
		
		if(hotMark)
		{
			stat.termHitHot[groupLevel]++;
		}else {
			stat.termHitCommon[groupLevel]++;
		}
		return SEEK_OK;
	}

	private static boolean termHeaderMatches(BytesRef found, BytesRef columnName, int groupid, byte groupLevel,
			boolean hotMark, int termIndex) {
		if (found.length <= 0) {
			return false;
		}
		return FpTokenTermLayout.columnNameEquals(found, columnName)
				&& FpTokenTermLayout.read_group_id(found) == groupid
				&& FpTokenTermLayout.readLevel(found) == (groupLevel & 0xFF)
				&& FpTokenTermLayout.isHotTerm(found) == hotMark
				&& FpTokenTermLayout.readTermIndex(found) == termIndex;
	}

	private static boolean payloadMatchesSlice(boolean requireExactPayloadMatch, BytesRef payload, BytesRef slice) {
		final int plen = payload.length;
		final int slen = slice.length;
		if (plen < slen) {
			return false;
		}
		if (requireExactPayloadMatch || plen == slen) {
			return payload.equals(slice);
		}

		final byte[] pb = payload.bytes;
		final int po = payload.offset;
		final byte[] sb = slice.bytes;
		final int so = slice.offset;

		for (int i = 0; i <= plen - slen; i++) {
			boolean match = true;
			for (int j = 0; j < slen; j++) {
				if (pb[po + i + j] != sb[so + j]) {
					match = false;
					break;
				}
			}
			if (match) {
				return true;
			}
		}
		return false;
	}

	private  void orPostingsInto(AtomicReference<PostingsEnum> reuse, TermsEnum termsEnum, int maxDoc,
			FixedBitSet collect,boolean hotMark) throws IOException {
		final PostingsEnum pe = termsEnum.postings(reuse.get(), PostingsEnum.NONE);
		if (pe == null) {
			return;
		}

		reuse.set(pe);
		int doc;
		while ((doc = pe.nextDoc()) != PostingsEnum.NO_MORE_DOCS) {
			if (doc >= 0 && doc < maxDoc) {
				collect.set(doc);
				if(hotMark)
				{
					stat.hothit++;
				}else {
					stat.commonhit++;
				}
			}
		}
	}

	private static int ngramLengthIndex(int sliceLength) {
		if (sliceLength < Lucene80FPSearchConfig.NGRAM_MIN || sliceLength > Lucene80FPSearchConfig.NGRAM_MAX) {
			throw new IllegalArgumentException("slice length out of range: " + sliceLength);
		}
		return sliceLength - 1;
	}
}
