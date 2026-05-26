package cn.lxdb.plugins.muqingyu.fptoken.tests.unit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.junit.jupiter.api.Test;

import cn.lxdb.plugins.muqingyu.fptoken.api.FpSearch;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.block.FpGroupDataRebuild;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.block.FpGroupHotNgramBitIndex;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.block.FpGroupHotNgramRebuild;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FPDocList;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpBlockInfo;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpTermKey;
import cn.lxdb.plugins.muqingyu.fptoken.tests.support.FpWriteReadTestIndex;
import cn.lxdb.plugins.muqingyu.fptoken.tests.support.FpWriteReadTestIndex.Built;
import cn.lxdb.plugins.muqingyu.fptoken.tests.support.InMemoryFpTerms;
import cn.lxdb.plugins.muqingyu.fptoken.tests.support.SortedBytesTermsEnum;

/**
 * 写段（rebuild + 位图 + term 头）与 {@link FpSearch#search} 读路径对齐。
 */
class FpSearchWriteReadAlignmentTest {

	@Test
	void search_findsDocsWrittenByRebuildFlush() throws Exception {
		final int maxDoc = 32;
		final int groupId = 7;
		final byte targetLevel = 1;
		final Built index = buildRebuildIndex(maxDoc, groupId, targetLevel);

		final FpSearch search = new FpSearch();
		final BytesRef[] slices = { new BytesRef(new byte[] { 'a', 'b' }) };
		final FixedBitSet hits = search.search(FpWriteReadTestIndex.blockList(groupId, index.blockInfo), index.terms,
				maxDoc, slices);

		assertTrue(hits.get(1));
		assertTrue(hits.get(2));
		assertFalse(hits.get(3));
	}

	@Test
	void search_hotHit_skipsCommonBank() throws Exception {
		final int maxDoc = 16;
		final Built index = buildRebuildIndex(maxDoc, 1, (byte) 1);
		// 仅 common 含 "xy"，热词路径对 slice "xy" 无命中时应走 common；此处热词已含 ab，对 xy 应无 hot
		final FpSearch search = new FpSearch();
		final BytesRef[] slices = { new BytesRef(new byte[] { 'x', 'y' }) };
		final FixedBitSet hits = search.search(FpWriteReadTestIndex.blockList(index.groupId, index.blockInfo),
				index.terms, maxDoc, slices);
		assertFalse(hits.get(1));
	}

	@Test
	void search_twoSlices_andSemantics() throws Exception {
		final int maxDoc = 32;
		final FpGroupDataRebuild group = new FpGroupDataRebuild(maxDoc);
		final FPDocList both = new FPDocList(maxDoc);
		both.addDoc(10);
		group.commonTermMapInternal().put(FpTermKey.copyOf(new BytesRef("abxy".getBytes())), both);
		final FPDocList onlyAb = new FPDocList(maxDoc);
		onlyAb.addDoc(11);
		group.commonTermMapInternal().put(FpTermKey.copyOf(new BytesRef("abab".getBytes())), onlyAb);
		FpGroupHotNgramRebuild.execute(group, null, 2);
		final FpGroupHotNgramBitIndex bits = FpGroupHotNgramBitIndex.execute((byte) 1, group);
		final FpBlockInfo info = new FpBlockInfo();
		info.targetLevel = 1;
		final Built index = FpWriteReadTestIndex.fromRebuildGroup(group, 3, (byte) 1, bits, info);

		final FpSearch search = new FpSearch();
		final BytesRef[] slices = { new BytesRef(new byte[] { 'a', 'b' }), new BytesRef(new byte[] { 'x', 'y' }) };
		final FixedBitSet hits = search.search(FpWriteReadTestIndex.blockList(3, info), index.terms, maxDoc, slices);
		assertTrue(hits.get(10));
		assertFalse(hits.get(11));
	}

	@Test
	void search_emptySlices_returnsEmptyBitSet() throws Exception {
		final Built index = buildRebuildIndex(8, 1, (byte) 0);
		final FixedBitSet hits = new FpSearch().search(FpWriteReadTestIndex.blockList(index.groupId, index.blockInfo),
				index.terms, 8, new BytesRef[0]);
		assertFalse(hits.get(0));
	}

	@Test
	void search_invalidSliceLength_throws() {
		final Built index = buildRebuildIndexQuiet(8, 1, (byte) 0);
		assertThrows(IllegalArgumentException.class, () -> new FpSearch().search(
				FpWriteReadTestIndex.blockList(index.groupId, index.blockInfo), index.terms, 8,
				new BytesRef[] { new BytesRef(new byte[10]) }));
	}

	@Test
	void ingestTermPostings_skipsHotHeader_onlyMergesCommon() throws Exception {
		final int maxDoc = 16;
		final FpGroupDataRebuild group = new FpGroupDataRebuild(maxDoc);
		final byte[] buf = new byte[32];
		final BytesRef hotWithHeader = new BytesRef(buf);
		cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpTokenTermLayout.make_fp_term(hotWithHeader, (short) 0, 1,
				(byte) 0, true, 1, false, (byte) 0, new BytesRef(new byte[] { 9 }));
		final byte[] commonBuf = new byte[32];
		final BytesRef commonWithHeader = new BytesRef(commonBuf);
		cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpTokenTermLayout.make_fp_term(commonWithHeader, (short) 0, 1,
				(byte) 0, false, 1, false, (byte) 0, new BytesRef(new byte[] { 5, 6 }));
		final java.util.TreeMap<byte[], int[]> postings = new java.util.TreeMap<>(
				cn.lxdb.plugins.muqingyu.fptoken.tests.support.BytesRefLexicographicComparator.INSTANCE);
		postings.put(hotWithHeader.bytes.clone(), new int[] { 4 });
		postings.put(commonWithHeader.bytes.clone(), new int[] { 2 });
		final InMemoryFpTerms terms = new InMemoryFpTerms(new java.util.TreeMap<>(), postings);
		final SortedBytesTermsEnum te = (SortedBytesTermsEnum) terms.iterator();
		te.seekExact(hotWithHeader);
		group.ingestTermPostings(hotWithHeader, te, maxDoc);
		assertTrue(group.hotTermMapInternal().isEmpty());
		assertTrue(group.commonTermMapInternal().isEmpty());

		final SortedBytesTermsEnum te2 = (SortedBytesTermsEnum) terms.iterator();
		te2.seekExact(commonWithHeader);
		group.ingestTermPostings(commonWithHeader, te2, maxDoc);
		assertEquals(1, group.commonTermMapInternal().size());
	}

	private static Built buildRebuildIndex(int maxDoc, int groupId, byte targetLevel) throws Exception {
		final FpGroupDataRebuild group = new FpGroupDataRebuild(maxDoc);
		final FPDocList docs = new FPDocList(maxDoc);
		docs.addDoc(1);
		docs.addDoc(2);
		group.commonTermMapInternal().put(FpTermKey.copyOf(new BytesRef("abab".getBytes())), docs);
		final FPDocList other = new FPDocList(maxDoc);
		other.addDoc(3);
		group.commonTermMapInternal().put(FpTermKey.copyOf(new BytesRef("xyzz".getBytes())), other);
		FpGroupHotNgramRebuild.execute(group, null, 2);
		final FpGroupHotNgramBitIndex bits = FpGroupHotNgramBitIndex.execute(targetLevel, group);
		final FpBlockInfo info = new FpBlockInfo();
		info.targetLevel = targetLevel;
		return FpWriteReadTestIndex.fromRebuildGroup(group, groupId, targetLevel, bits, info);
	}

	private static Built buildRebuildIndexQuiet(int maxDoc, int groupId, byte targetLevel) {
		try {
			return buildRebuildIndex(maxDoc, groupId, targetLevel);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

}
