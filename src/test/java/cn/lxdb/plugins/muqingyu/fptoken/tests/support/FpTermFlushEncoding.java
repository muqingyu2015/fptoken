package cn.lxdb.plugins.muqingyu.fptoken.tests.support;

import java.util.Map.Entry;

import org.apache.lucene.util.BytesRef;

import cn.lxdb.plugins.muqingyu.fptoken.dataset.block.FpGroupDataRebuild;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FPDocList;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpTermKey;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpTokenTermLayout;

/**
 * 与写段 {@code flushto} 相同的 {@link FpTokenTermLayout#make_fp_term} 编码，供读写对齐测试复用。
 */
public final class FpTermFlushEncoding {

	private FpTermFlushEncoding() {
	}

	/** 对齐 {@link FpGroupDataRebuild#flushto} 热词写出。 */
	public static BytesRef encodeRebuildHot(BytesRef reuse, int groupId, byte targetLevel, Entry<FpTermKey, FPDocList> e,
			FpGroupDataRebuild group) {
		final FpTermKey key = e.getKey();
		final Integer level = group.hotTermToLevelInternal().get(key);
		final Integer order = group.hotTermOrderInternal().get(key);
		if (order == null) {
			return null;
		}
		final int scanlevel = level != null ? level.intValue() : 0;
		final FPDocList val = e.getValue();
		final int index = order.intValue();
		final boolean isDelTerm = val.docsize() <= 0;
		FpTokenTermLayout.make_fp_term(reuse, (short) 0, groupId, targetLevel, FpTokenTermLayout.TERM_MARK_HOT, index,
				isDelTerm, (byte) scanlevel, key.bytesRef());
		return BytesRef.deepCopyOf(reuse);
	}

	/** 对齐 {@link FpGroupDataRebuild#flushto} 普通词写出（空 doc 的 del 跳过）。 */
	public static BytesRef encodeRebuildCommon(BytesRef reuse, int groupId, byte targetLevel, Entry<FpTermKey, FPDocList> e,
			FpGroupDataRebuild group) {
		final FpTermKey key = e.getKey();
		final FPDocList val = e.getValue();
		if (val.docsize() <= 0) {
			return null;
		}
		final Integer order = group.commonTermOrderInternal().get(key);
		if (order == null) {
			return null;
		}
		FpTokenTermLayout.make_fp_term(reuse, (short) 0, groupId, targetLevel, FpTokenTermLayout.TERM_MARK_COMMON,
				order.intValue(), false, (byte) 0, key.bytesRef());
		return BytesRef.deepCopyOf(reuse);
	}

	/** 对齐 {@link cn.lxdb.plugins.muqingyu.fptoken.dataset.block.FpGroupDataOriginal#flushto} 热词透传写出。 */
	public static BytesRef encodeOriginalHot(BytesRef reuse, int newGroupId, byte targetLevel, FpTermKey key,
			FPDocList val) {
		final int scanlevel = FpTokenTermLayout.readHotTermScanLevel(key.bytesRef());
		final boolean isDelTerm = FpTokenTermLayout.readIsDelTerm(key.bytesRef()) || val.docsize() <= 0;
		final int index = FpTokenTermLayout.readTermIndex(key.bytesRef());
		final BytesRef payload = FpTokenTermLayout.removeHeaderBytes(key.bytesRef());
		FpTokenTermLayout.make_fp_term(reuse, (short) 0, newGroupId, targetLevel, FpTokenTermLayout.TERM_MARK_HOT,
				index, isDelTerm, (byte) scanlevel, payload);
		return BytesRef.deepCopyOf(reuse);
	}

	/** 对齐 {@link cn.lxdb.plugins.muqingyu.fptoken.dataset.block.FpGroupDataOriginal#flushto} 普通词透传写出。 */
	public static BytesRef encodeOriginalCommon(BytesRef reuse, int newGroupId, byte targetLevel, FpTermKey key,
			FPDocList val) {
		if (val.docsize() <= 0) {
			return null;
		}
		final int index = FpTokenTermLayout.readTermIndex(key.bytesRef());
		final BytesRef payload = FpTokenTermLayout.removeHeaderBytes(key.bytesRef());
		FpTokenTermLayout.make_fp_term(reuse, (short) 0, newGroupId, targetLevel, FpTokenTermLayout.TERM_MARK_COMMON,
				index, false, (byte) 0, payload);
		return BytesRef.deepCopyOf(reuse);
	}
}
