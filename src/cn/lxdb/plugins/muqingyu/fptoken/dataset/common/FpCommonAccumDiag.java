package cn.lxdb.plugins.muqingyu.fptoken.dataset.common;

import java.util.Map.Entry;
import java.util.TreeMap;

/**
 * 段合并 / 写段时 common 组内 term 来源诊断：区分 {@code group_id==0} 直 ingest 与 {@code group_id!=0} 来源，
 * 以及 high 组 {@link cn.lxdb.plugins.muqingyu.fptoken.dataset.block.FpGroupDataOriginal#mergeIntoRebuild} 批量并入规模。
 */
public final class FpCommonAccumDiag {

	private long ingestGroupZero;
	private long ingestGroupNonZero;
	private final TreeMap<Integer, Long> ingestByNonZeroGroupId = new TreeMap<>();
	/** 源 fp {@code group_id} → {@code [hotTermsMerged, commonTermsMerged]}（累计） */
	private final TreeMap<Integer, int[]> mergeBatchByGroupId = new TreeMap<>();
	private int lastLoggedAtTerms;

	public void reset() {
		ingestGroupZero = 0;
		ingestGroupNonZero = 0;
		ingestByNonZeroGroupId.clear();
		mergeBatchByGroupId.clear();
		lastLoggedAtTerms = 0;
	}

	public void recordIngest(int sourceGroupId) {
		if (sourceGroupId == 0) {
			ingestGroupZero++;
		} else {
			ingestGroupNonZero++;
			ingestByNonZeroGroupId.merge(sourceGroupId, 1L, Long::sum);
		}
	}

	public void recordMergeBatch(int sourceGroupId, int hotTerms, int commonTerms) {
		int[] acc = mergeBatchByGroupId.get(sourceGroupId);
		if (acc == null) {
			mergeBatchByGroupId.put(sourceGroupId, new int[] { hotTerms, commonTerms });
		} else {
			acc[0] += hotTerms;
			acc[1] += commonTerms;
		}
	}

	public long ingestGroupZero() {
		return ingestGroupZero;
	}

	public long ingestGroupNonZero() {
		return ingestGroupNonZero;
	}

	/** 首次 ≥ threshold，之后每再增 {@code step} 打一次，避免 8 万 term 期间完全静默。 */
	public boolean shouldLogNow(int commonTerms, int threshold, int step) {
		if (commonTerms < threshold) {
			return false;
		}
		if (lastLoggedAtTerms < threshold) {
			return true;
		}
		return commonTerms >= lastLoggedAtTerms + step;
	}

	public void markLogged(int commonTerms) {
		lastLoggedAtTerms = commonTerms;
	}

	public void appendFields(StringBuilder sb) {
		FpLog.append(sb, "ingestGroupZero", ingestGroupZero);
		FpLog.append(sb, "ingestGroupNonZero", ingestGroupNonZero);
		appendIngestBySourceGroup(sb);
		appendMergeFromGroup(sb);
	}

	private void appendIngestBySourceGroup(StringBuilder sb) {
		if (ingestByNonZeroGroupId.isEmpty()) {
			FpLog.append(sb, "ingestBySourceGroup", "");
			return;
		}
		sb.append(' ').append("ingestBySourceGroup=");
		boolean first = true;
		for (Entry<Integer, Long> e : ingestByNonZeroGroupId.entrySet()) {
			if (!first) {
				sb.append(',');
			}
			first = false;
			sb.append(e.getKey()).append(':').append(e.getValue().longValue());
		}
	}

	private void appendMergeFromGroup(StringBuilder sb) {
		if (mergeBatchByGroupId.isEmpty()) {
			FpLog.append(sb, "mergeFromGroup", "");
			return;
		}
		sb.append(' ').append("mergeFromGroup=");
		boolean first = true;
		for (Entry<Integer, int[]> e : mergeBatchByGroupId.entrySet()) {
			if (!first) {
				sb.append(';');
			}
			first = false;
			final int[] v = e.getValue();
			sb.append(e.getKey()).append("{hot=").append(v[0]).append(",common=").append(v[1]).append('}');
		}
	}
}
