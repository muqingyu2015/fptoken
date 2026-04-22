package cn.lxdb.plugins.muqingyu.fptoken.tests;

import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.CandidateItemset;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;

/** 构造 {@link CandidateItemset} 及互斥性检查（按 termId）。 */
public final class CandidateFixture {

    private CandidateFixture() {
    }

    public static CandidateItemset itemset(int[] termIds, int... docIds) {
        BitSet bits = new BitSet();
        for (int d : docIds) {
            bits.set(d);
        }
        return new CandidateItemset(termIds, bits);
    }

    /** 与 {@link CandidateItemset} 构造函数三参版一致（显式 support）。 */
    public static CandidateItemset itemset(int[] termIds, BitSet docBits, int support) {
        return new CandidateItemset(termIds, docBits, support);
    }

    /** 语义相等（termIds、docBits、support；estimatedSaving 由前两者推导）。 */
    public static boolean sameContent(CandidateItemset a, CandidateItemset b) {
        return Arrays.equals(a.getTermIds(), b.getTermIds())
                && a.getSupport() == b.getSupport()
                && a.getDocBits().equals(b.getDocBits());
    }

    public static boolean samePickOrder(List<CandidateItemset> a, List<CandidateItemset> b) {
        if (a.size() != b.size()) {
            return false;
        }
        for (int i = 0; i < a.size(); i++) {
            if (!sameContent(a.get(i), b.get(i))) {
                return false;
            }
        }
        return true;
    }

    public static boolean mutuallyExclusiveByTermId(List<CandidateItemset> selected) {
        BitSet used = new BitSet();
        for (CandidateItemset c : selected) {
            for (int t : c.getTermIds()) {
                if (t < 0) {
                    throw new IllegalArgumentException("negative termId");
                }
                if (used.get(t)) {
                    return false;
                }
                used.set(t);
            }
        }
        return true;
    }
}
