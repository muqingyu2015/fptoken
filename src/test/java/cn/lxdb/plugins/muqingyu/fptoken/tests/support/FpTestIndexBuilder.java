package cn.lxdb.plugins.muqingyu.fptoken.tests.support;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.BytesRef;

import cn.lxdb.plugins.muqingyu.fptoken.config.FpTokenBlockLevelPolicy;
import cn.lxdb.plugins.muqingyu.fptoken.config.Lucene80FPSearchConfig;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.block.FpGroupDataRebuild;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.block.FpGroupHotNgramBitIndex;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.block.FpGroupHotNgramRebuild;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FPDocList;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpBlockInfo;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpTermKey;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpTokenTermLayout;

/**
 * 测试 harness：common 组数据 → ngram rebuild → bit index → 内存 Terms + fpblock_list。
 * 供写读与查询功能测试复用。
 */
public final class FpTestIndexBuilder {

	public static final int DEFAULT_GROUP_ID = 1;

	private FpTestIndexBuilder() {
	}

	public static final class BuiltIndex {
		public final int groupId;
		public final int maxDoc;
		public final BytesRef columnName;
		public final int targetLevel;
		public final FpGroupHotNgramBitIndex memoryBitIndex;
		public final FpBlockInfo blockInfo;
		public final FpGroupHotNgramBitIndex diskBitIndex;
		public final InMemoryFpTerms terms;
		public final TreeMap<Integer, FpBlockInfo> fpblockList;

		BuiltIndex(int groupId, int maxDoc, BytesRef columnName, int targetLevel,
				FpGroupHotNgramBitIndex memoryBitIndex, FpBlockInfo blockInfo, FpGroupHotNgramBitIndex diskBitIndex,
				InMemoryFpTerms terms, TreeMap<Integer, FpBlockInfo> fpblockList) {
			this.groupId = groupId;
			this.maxDoc = maxDoc;
			this.columnName = columnName;
			this.targetLevel = targetLevel;
			this.memoryBitIndex = memoryBitIndex;
			this.blockInfo = blockInfo;
			this.diskBitIndex = diskBitIndex;
			this.terms = terms;
			this.fpblockList = fpblockList;
		}
	}

	/** 对已有 rebuild 组跑完整索引流水线并写出位图区。 */
	public static BuiltIndex buildFromRebuildGroup(FpGroupDataRebuild group, BytesRef columnName, int groupId)
			throws IOException {
		FpGroupHotNgramRebuild.execute(group, null);
		group.rebuildHotTermOrderFromHotDocs();
		group.rebuildCommonTermToOrderFromHotDocs();

		final int targetLevel = FpTokenBlockLevelPolicy.resolveTargetBlockLevel(group.commonTermToDocs.size(),
				group.distinctDocUnion.cardinality());
		final FpGroupHotNgramBitIndex memoryBitIndex = FpGroupHotNgramBitIndex.execute(targetLevel, group);

		final Directory dir = new RAMDirectory();
		final FpBlockInfo blockInfo;
		try (IndexOutput out = dir.createOutput("bits", IOContext.DEFAULT)) {
			blockInfo = memoryBitIndex.flushto(out, "test", columnName, group.distinctDocUnion.cardinality());
		}

		FpGroupHotNgramBitIndex diskBitIndex;
		try (IndexInput in = dir.openInput("bits", IOContext.DEFAULT)) {
			diskBitIndex = FpGroupHotNgramBitIndex.readfrom(in, blockInfo);
		}

		final TreeMap<byte[], int[]> postings = buildPostings(columnName, groupId, targetLevel, group);
		final TreeMap<Integer, FpGroupHotNgramBitIndex> bitsByGroup = new TreeMap<>();
		bitsByGroup.put(groupId, memoryBitIndex);

		final TreeMap<Integer, FpBlockInfo> fpblockList = new TreeMap<>();
		fpblockList.put(groupId, blockInfo);

		return new BuiltIndex(groupId, group.maxDoc, columnName, targetLevel, memoryBitIndex, blockInfo, diskBitIndex,
				new InMemoryFpTerms(bitsByGroup, postings), fpblockList);
	}

	static TreeMap<byte[], int[]> buildPostings(BytesRef columnName, int groupId, int targetLevel,
			FpGroupDataRebuild group) throws IOException {
		final TreeMap<byte[], int[]> postings = new TreeMap<>(BytesRefLexicographicComparator.INSTANCE);
		for (Map.Entry<FpTermKey, FPDocList> e : group.hotTermToDocs.entrySet()) {
			final Integer order = group.hotTermToOrder.get(e.getKey());
			if (order == null) {
				continue;
			}
			final Integer budgetObj = group.hotTermDownTierBudget.get(e.getKey());
			final int budget = budgetObj == null ? 0 : budgetObj.intValue();
			putPosting(postings, columnName, groupId, targetLevel, true, order.intValue(), false, budget,
					e.getKey().bytesRef(), e.getValue());
		}
		for (Map.Entry<FpTermKey, FPDocList> e : group.commonTermToDocs.entrySet()) {
			final Integer order = group.commonTermToOrder.get(e.getKey());
			if (order == null || e.getValue().docsize() <= 0) {
				continue;
			}
			putPosting(postings, columnName, groupId, targetLevel, false, order.intValue(), false, 0,
					e.getKey().bytesRef(), e.getValue());
		}
		return postings;
	}

	private static void putPosting(TreeMap<byte[], int[]> postings, BytesRef columnName, int groupId, int targetLevel,
			boolean hot, int termIndex, boolean isDel, int downTierBudget, BytesRef payload, FPDocList docs)
			throws IOException {
		final byte[] buf = new byte[256];
		final BytesRef term = new BytesRef(buf);
		FpTokenTermLayout.make_fp_term(term, columnName, Lucene80FPSearchConfig.DEFAULT_INDEX_ID, groupId,
				(byte) targetLevel, hot, termIndex, isDel, (byte) downTierBudget, payload);
		final byte[] key = Arrays.copyOf(term.bytes, term.length);
		postings.put(key, docListToArray(docs));
	}

	private static int[] docListToArray(FPDocList docs) throws IOException {
		final List<Integer> list = new ArrayList<>();
		docs.foreach(list::add);
		final int[] arr = new int[list.size()];
		for (int i = 0; i < list.size(); i++) {
			arr[i] = list.get(i).intValue();
		}
		return arr;
	}
}
