package cn.lxdb.plugins.muqingyu.fptoken.tests;

import cn.lxdb.plugins.muqingyu.fptoken.model.DocTerms;
import cn.lxdb.plugins.muqingyu.fptoken.model.SelectedGroup;
import cn.lxdb.plugins.muqingyu.fptoken.util.ByteArrayUtils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** 测试数据构造与断言辅助（与生产包隔离）。 */
public final class ByteArrayTestSupport {

    private ByteArrayTestSupport() {
    }

    /** 十六进制字符串 → 字节（长度须为偶数）。 */
    public static byte[] hex(String s) {
        int n = s.length() / 2;
        byte[] out = new byte[n];
        for (int i = 0; i < n; i++) {
            out[i] = (byte) Integer.parseInt(s.substring(i * 2, i * 2 + 2), 16);
        }
        return out;
    }

    public static DocTerms doc(int docId, byte[]... terms) {
        List<byte[]> list = new ArrayList<>(Arrays.asList(terms));
        return new DocTerms(docId, list);
    }

    public static DocTerms doc(int docId, List<byte[]> terms) {
        return new DocTerms(docId, terms);
    }

    /**
     * 模拟 128 字节滑动窗口内的 1/2/3 字节 item（每个起始偏移各取三种长度，与典型 PCAP 特征抽取一致）。
     *
     * @param window 窗口原始字节（测试中可用较短数组以控规模）
     */
    public static List<byte[]> slidingItems123(byte[] window) {
        List<byte[]> terms = new ArrayList<>();
        int w = window.length;
        for (int i = 0; i < w; i++) {
            if (i + 1 <= w) {
                terms.add(Arrays.copyOfRange(window, i, i + 1));
            }
            if (i + 2 <= w) {
                terms.add(Arrays.copyOfRange(window, i, i + 2));
            }
            if (i + 3 <= w) {
                terms.add(Arrays.copyOfRange(window, i, i + 3));
            }
        }
        return terms;
    }

    /** 生成固定模式的「伪记录」字节（长度 {@code len}），便于批量造窗。 */
    public static byte[] pseudoRecord(int seed, int len) {
        byte[] r = new byte[len];
        for (int i = 0; i < len; i++) {
            r[i] = (byte) (0x30 + ((seed * 31 + i * 17) & 0x7F));
        }
        return r;
    }

    /**
     * 将每条伪记录按窗口切片，对每窗生成 sliding items，一条记录对应多个 {@link DocTerms}（docId 递增）。
     *
     * @return 扁平文档列表
     */
    public static List<DocTerms> pcapLikeBatch(int numRecords, int recordLen, int windowLen, int windowStep) {
        List<DocTerms> rows = new ArrayList<>();
        int docId = 0;
        for (int p = 0; p < numRecords; p++) {
            byte[] record = pseudoRecord(p, recordLen);
            for (int off = 0; off + windowLen <= recordLen; off += windowStep) {
                byte[] window = Arrays.copyOfRange(record, off, off + windowLen);
                rows.add(doc(docId++, slidingItems123(window)));
            }
        }
        return rows;
    }

    /** 任意两组 {@link SelectedGroup} 的词字节集合是否无交集（整段 byte[] 精确相等视为同一词）。 */
    public static boolean pairwiseTermsDisjoint(List<SelectedGroup> groups) {
        for (int i = 0; i < groups.size(); i++) {
            Set<ByteArrayHolder> a = termSet(groups.get(i));
            for (int j = i + 1; j < groups.size(); j++) {
                Set<ByteArrayHolder> b = termSet(groups.get(j));
                for (ByteArrayHolder x : a) {
                    if (b.contains(x)) {
                        return false;
                    }
                }
            }
        }
        return true;
    }

    private static Set<ByteArrayHolder> termSet(SelectedGroup g) {
        Set<ByteArrayHolder> s = new HashSet<>();
        for (byte[] t : g.getTerms()) {
            s.add(new ByteArrayHolder(t));
        }
        return s;
    }

    /** 稳定可比的 groups 指纹（用于确定性测试）。 */
    public static String groupsFingerprint(List<SelectedGroup> groups) {
        List<String> parts = new ArrayList<>();
        for (SelectedGroup g : groups) {
            List<String> hx = new ArrayList<>();
            for (byte[] t : g.getTerms()) {
                hx.add(ByteArrayUtils.toHex(t));
            }
            Collections.sort(hx);
            parts.add(String.join(",", hx) + ":" + g.getSupport());
        }
        Collections.sort(parts);
        return String.join("|", parts);
    }

    /** 仅按长度约束检查（挖掘近似时仍应满足配置上下界）。 */
    public static boolean allGroupsTermCountInRange(List<SelectedGroup> groups, int minLen, int maxLen) {
        for (SelectedGroup g : groups) {
            int n = g.getTerms().size();
            if (n < minLen || n > maxLen) {
                return false;
            }
        }
        return true;
    }

    /** 与 {@link cn.lxdb.plugins.muqingyu.fptoken.model.CandidateItemset} 中启发式一致。 */
    public static int expectedEstimatedSaving(int termCount, int support) {
        return Math.max(0, (termCount - 1) * support);
    }

    public static boolean isSortedAscending(List<Integer> ids) {
        for (int i = 1; i < ids.size(); i++) {
            if (ids.get(i) < ids.get(i - 1)) {
                return false;
            }
        }
        return true;
    }

    public static boolean listContainsTermBytes(List<byte[]> terms, byte[] needle) {
        for (byte[] t : terms) {
            if (Arrays.equals(t, needle)) {
                return true;
            }
        }
        return false;
    }

    public static boolean anyGroupContainsTerm(List<SelectedGroup> groups, byte[] needle) {
        for (SelectedGroup g : groups) {
            if (listContainsTermBytes(g.getTerms(), needle)) {
                return true;
            }
        }
        return false;
    }

    /** 是否存在恰好包含 {@code needles} 全部词（顺序无关）的组。 */
    public static boolean anyGroupHasExactlyTerms(List<SelectedGroup> groups, byte[][] needles) {
        outer:
        for (SelectedGroup g : groups) {
            if (g.getTerms().size() != needles.length) {
                continue;
            }
            List<byte[]> terms = g.getTerms();
            for (byte[] needle : needles) {
                if (!listContainsTermBytes(terms, needle)) {
                    continue outer;
                }
            }
            return true;
        }
        return false;
    }

    private static final class ByteArrayHolder {
        private final byte[] value;
        private final int hash;

        ByteArrayHolder(byte[] value) {
            this.value = value;
            this.hash = Arrays.hashCode(value);
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof ByteArrayHolder)) {
                return false;
            }
            return Arrays.equals(value, ((ByteArrayHolder) o).value);
        }

        @Override
        public int hashCode() {
            return hash;
        }
    }
}
