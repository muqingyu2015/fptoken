package cn.lxdb.plugins.muqingyu.fptoken.tests.support;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;

import cn.lxdb.plugins.muqingyu.fptoken.dataset.block.FpGroupHotNgramBitIndex;

/**
 * 测试用 {@link Terms}：按 group_id 挂载位图，词项表与 posting 均在内存。
 */
public final class InMemoryFpTerms extends Terms {

	private final TreeMap<Integer, FpGroupHotNgramBitIndex> bitsByGroupId;
	private final TreeMap<byte[], int[]> postingsByTerm;

	public InMemoryFpTerms(TreeMap<Integer, FpGroupHotNgramBitIndex> bitsByGroupId,
			TreeMap<byte[], int[]> postingsByTerm) {
		this.bitsByGroupId = bitsByGroupId == null ? new TreeMap<>() : bitsByGroupId;
		this.postingsByTerm = postingsByTerm == null ? new TreeMap<>() : postingsByTerm;
	}

	@Override
	public FpGroupHotNgramBitIndex fpBits(short indexId, int groupId, long[] loadHot, long[] loadCommon)
			throws IOException {
		final FpGroupHotNgramBitIndex bits = bitsByGroupId.get(groupId);
		if (bits == null) {
			throw new IOException("no fp bits for group_id=" + groupId);
		}
		if (loadHot == null && loadCommon == null) {
			return bits;
		}
		return bits.viewSelective(loadHot, loadCommon);
	}

	@Override
	public TermsEnum iterator() {
		return new SortedBytesTermsEnum(postingsByTerm);
	}

	@Override
	public TermsEnum intersect(org.apache.lucene.util.automaton.CompiledAutomaton compiled, BytesRef startTerm)
			throws IOException {
		return iterator();
	}

	@Override
	public long size() {
		return postingsByTerm.size();
	}

	@Override
	public long guess_size() {
		return size();
	}

	@Override
	public long getSumTotalTermFreq() {
		return size();
	}

	@Override
	public long getSumDocFreq() {
		return size();
	}

	@Override
	public int getDocCount() {
		return 0;
	}

	@Override
	public boolean hasFreqs() {
		return false;
	}

	@Override
	public boolean hasOffsets() {
		return false;
	}

	@Override
	public boolean hasPositions() {
		return false;
	}

	@Override
	public boolean hasPayloads() {
		return false;
	}
}
