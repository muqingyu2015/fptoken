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
	public static final Logger LOG = LxdbLogerEncrypt.getLogger("mqy.fptoken");

	public FpSearchStat stat;
	public final String traceId;

	public FpSearch(FpSearchStat stat) {
		this(stat, "");
	}

	public FpSearch(FpSearchStat stat, String traceId) {
		this.stat = stat;
		if (traceId != null && !traceId.isEmpty()) {
			this.traceId = traceId;
		} else if (Lucene80FPSearchConfig.LOG_FP_SEARCH || Lucene80FPSearchConfig.PRINT_DEBUG) {
			this.traceId = java.util.UUID.randomUUID().toString();
		} else {
			this.traceId = "";
		}
	}


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
			return rtn;
		}

		final FixedBitSet[] collect = new FixedBitSet[slices.length];
		int maxGroupId = -1;
		int columnMatchedGroups = 0;
		int columnSkippedGroups = 0;
		String indexColumnSample = null;
		final long[] bucketKeys = FpGroupHotNgramBitIndex.selectiveKeysForSlices(slices);

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
			
			//通过skip index按需加载命中的hot和common的term_order_num
			final FpGroupHotNgramBitIndex bitsetIndex = loadBitIndex(terms, groupId, bucketKeys);
			if (bitsetIndex == null) {
				if (Lucene80FPSearchConfig.LOG_FP_SEARCH || Lucene80FPSearchConfig.PRINT_DEBUG) {
					final StringBuilder sb = FpLog.kv();
					FpLog.append(sb, "event", "skipGroup");
					FpLog.append(sb, "reason", "fpBitsNull");
					FpLog.append(sb, "groupId", groupId);
					FpLog.append(sb, "level", "L" + blkinfo.targetLevel);
					FpLog.debugTrace(LOG, traceId, sb);
				}
				continue;
			}

			//根据term_order_num返回命中的docid
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
			FpLog.searchTrace(LOG, traceId, sb);
		}

		//有些小组，暴力处理
		searchSparseNoBitIndexTerms(terms, columnName, maxDoc, slices, collect, maxGroupId);

		boolean merged = false;
		for (int i = 0; i < slices.length; i++) {
			if (collect[i] == null) {
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
			FpLog.debugTrace(LOG, traceId, sb);
		}
		return rtn;
	}

	/**
	 * 加载组位图（selective {@code fpBits}）；不做全量 {@code fpBits(..., null, null)} 回退。
	 */
	private FpGroupHotNgramBitIndex loadBitIndex(Terms terms, int groupId, long[] bucketKeys) throws IOException {
		return terms.fpBits(Lucene80FPSearchConfig.DEFAULT_INDEX_ID, groupId, bucketKeys, bucketKeys);
	}

	private void searchSliceInGroup(FpGroupHotNgramBitIndex bitIndex, BytesRef columnName, BytesRef anchorSlice,
			FpBlockInfo blkinfo, int groupid, Terms terms, int maxDoc, FixedBitSet[] collect, int sliceIndex)
			throws IOException {
		final FixedBitSet acc = ensureCollect(collect, sliceIndex, maxDoc);
		final int[] hotOrders = bitIndex.lookupHotOrders(anchorSlice);
		final int[] commonOrders = bitIndex.lookupCommonOrders(anchorSlice);
		if (Lucene80FPSearchConfig.PRINT_DEBUG && hotOrders.length == 0 && commonOrders.length == 0) {
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
			FpLog.debugTrace(LOG, traceId, sb);
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
			FpLog.debugTrace(LOG, traceId, sb);
		}
		final boolean hotHit = searchOrdersHot(bitIndex, columnName, anchorSlice, blkinfo, groupid, terms, maxDoc, acc,
				hotOrders);
		if (Lucene80FPSearchConfig.PRINT_DEBUG) {
			final StringBuilder sb = FpLog.kv();
			FpLog.append(sb, "event", "searchOrdersHot");
			FpLog.append(sb, "hit", hotHit);
			FpLog.append(sb, "slice", Utils.BytesReftoString(anchorSlice));
			FpLog.append(sb, "groupId", groupid);
			FpLog.debugTrace(LOG, traceId, sb);
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
				FpLog.debugTrace(LOG, traceId, sb);
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
	 * 仅扫 level=NOGROUP 的 term；有 {@link FpBlockInfo} 的 group 已在主循环走位图路径，此处从 {@code maxGroupId+1} seek 起补扫。
	 */
	private void searchSparseNoBitIndexTerms(Terms terms, BytesRef columnName, int maxDoc, BytesRef[] slices,
			FixedBitSet[] collect, int maxGroupId) throws IOException {
		final short indexId = Lucene80FPSearchConfig.DEFAULT_INDEX_ID;
		final AtomicReference<PostingsEnum> reusePosting = new AtomicReference<PostingsEnum>(null);
		final TermsEnum termsEnum = terms.iterator();
		final BytesRef reuse = new BytesRef(new byte[512]);

		// 稀疏列 NOGROUP term 的 group_id 在写段时递增分配，通常落在已索引 group 之后；
		// 从 maxGroupId+1 seek，跳过 fpblock_list 中已有位图的 group，避免从 0 暴力扫全字典。
		FpTokenTermLayout.make_fp_search_prefix(reuse, columnName, indexId, maxGroupId + 1,
				(byte) FpTokenBlockLevelPolicy.BLOCK_LEVEL_NOGROUP, false, 0);
		if (termsEnum.seekCeil(reuse) == TermsEnum$SeekStatus.END) {
			stat.termMiss0++;
			if (Lucene80FPSearchConfig.PRINT_DEBUG) {
				final StringBuilder sb = FpLog.kv();
				FpLog.append(sb, "event", "sparseScanMiss");
				FpLog.append(sb, "reason", "seekCeilEnd");
				FpLog.append(sb, "column", Utils.BytesReftoString(columnName));
				FpLog.debugTrace(LOG, traceId, sb);
			}
			return;
		}

		if (Lucene80FPSearchConfig.PRINT_DEBUG) {
			final StringBuilder sb = FpLog.kv();
			FpLog.append(sb, "event", "sparseScanBegin");
			FpLog.append(sb, "column", Utils.BytesReftoString(columnName));
			FpLog.append(sb, "maxGroupId", maxGroupId);
			FpLog.debugTrace(LOG, traceId, sb);
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
			FpLog.debugTrace(LOG, traceId, sb);
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
				FpLog.debugTrace(LOG, traceId, sb);
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
			FpLog.debugTrace(LOG, traceId, sb);
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
				FpLog.debugTrace(LOG, traceId, sb);
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
				FpLog.debugTrace(LOG, traceId, sb);
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
					FpLog.debugTrace(LOG, traceId, sb);
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

}
