package cn.lxdb.plugins.muqingyu.fptoken.tests.support;

import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.lucene.util.BytesRef;

import cn.lxdb.plugins.muqingyu.fptoken.dataset.block.FpGroupDataRebuild;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.block.FpGroupHotNgramBitIndex;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FPDocList;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpBlockInfo;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpTermKey;

/** 将 rebuild 写段产物转为 {@link InMemoryFpTerms} 可检索的内存索引。 */
public final class FpWriteReadTestIndex {

	private FpWriteReadTestIndex() {
	}

	public static final class Built {
		public final int groupId;
		public final byte targetLevel;
		public final FpGroupHotNgramBitIndex bitIndex;
		public final FpBlockInfo blockInfo;
		public final InMemoryFpTerms terms;

		Built(int groupId, byte targetLevel, FpGroupHotNgramBitIndex bitIndex, FpBlockInfo blockInfo,
				InMemoryFpTerms terms) {
			this.groupId = groupId;
			this.targetLevel = targetLevel;
			this.bitIndex = bitIndex;
			this.blockInfo = blockInfo;
			this.terms = terms;
		}
	}

	public static Built fromRebuildGroup(FpGroupDataRebuild group, int groupId, byte targetLevel,
			FpGroupHotNgramBitIndex bitIndex, FpBlockInfo blockInfo) {
		final TreeMap<byte[], int[]> postings = new TreeMap<>(BytesRefLexicographicComparator.INSTANCE);
		final byte[] reuseBuf = new byte[1024];
		final BytesRef reuse = new BytesRef(reuseBuf);
		for (Entry<FpTermKey, FPDocList> e : group.hotTermMapInternal().entrySet()) {
			final BytesRef term = FpTermFlushEncoding.encodeRebuildHot(reuse, groupId, targetLevel, e, group);
			if (term != null) {
				putPostings(postings, term, e.getValue());
			}
		}
		for (Entry<FpTermKey, FPDocList> e : group.commonTermMapInternal().entrySet()) {
			final BytesRef term = FpTermFlushEncoding.encodeRebuildCommon(reuse, groupId, targetLevel, e, group);
			if (term != null) {
				putPostings(postings, term, e.getValue());
			}
		}
		final TreeMap<Integer, FpGroupHotNgramBitIndex> bits = new TreeMap<>();
		bits.put(groupId, bitIndex);
		return new Built(groupId, targetLevel, bitIndex, blockInfo, new InMemoryFpTerms(bits, postings));
	}

	public static TreeMap<Integer, FpBlockInfo> blockList(int groupId, FpBlockInfo info) {
		final TreeMap<Integer, FpBlockInfo> list = new TreeMap<>();
		list.put(groupId, info);
		return list;
	}

	private static void putPostings(TreeMap<byte[], int[]> postings, BytesRef term, FPDocList docs) {
		final int[] arr = docsToArray(docs);
		postings.put(term.bytes.clone(), arr);
	}

	private static int[] docsToArray(FPDocList docs) {
		final int[] out = new int[docs.docsize()];
		final int[] idx = { 0 };
		try {
			docs.foreach(doc -> out[idx[0]++] = doc);
		} catch (java.io.IOException e) {
			throw new RuntimeException(e);
		}
		return out;
	}
}
