package cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.util;

import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.ByteRef;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * {@code byte[]} 工具：复制、内容哈希、无符号字典序比较、十六进制展示，以及文档词列表规范化。
 *
 * <p>集中在此是为了避免在业务类中散落数组比较/拷贝逻辑，并与 {@link ByteArrayKey} 保持一致语义。
 *
 * @author muqingyu
 */
public final class ByteArrayUtils {

    private static final char[] HEX = "0123456789ABCDEF".toCharArray();

    private ByteArrayUtils() {
    }

    /** 浅拷贝：返回新数组，长度与内容一致。 */
    public static byte[] copy(byte[] src) {
        byte[] out = new byte[src.length];
        System.arraycopy(src, 0, out, 0, src.length);
        return out;
    }

    /**
     * 与 {@link String#hashCode()} 同型多项式哈希（按无符号字节参与 {@code 31 * h + (b & 0xFF)}）。
     * <p>与 {@link ByteArrayKey} 配套使用。
     */
    public static int hash(byte[] arr) {
        int h = 1;
        for (byte b : arr) {
            h = 31 * h + (b & 0xFF);
        }
        return h;
    }

    /** 区间哈希：按 {@code [offset, offset+length)} 计算。 */
    public static int hash(byte[] arr, int offset, int length) {
        int h = 1;
        for (int i = 0; i < length; i++) {
            h = 31 * h + (arr[offset + i] & 0xFF);
        }
        return h;
    }

    /**
     * 无符号字节字典序：逐字节按 0–255 比较，较短者若为前缀则更短者更小。
     */
    public static int compareUnsigned(byte[] a, byte[] b) {
        int n = Math.min(a.length, b.length);
        for (int i = 0; i < n; i++) {
            int av = a[i] & 0xFF;
            int bv = b[i] & 0xFF;
            if (av != bv) {
                return av - bv;
            }
        }
        return a.length - b.length;
    }

    /** ByteRef 无符号字节字典序比较。 */
    public static int compareUnsigned(ByteRef a, ByteRef b) {
        int n = Math.min(a.getLength(), b.getLength());
        byte[] as = a.getSourceUnsafe();
        byte[] bs = b.getSourceUnsafe();
        int ao = a.getOffset();
        int bo = b.getOffset();
        for (int i = 0; i < n; i++) {
            int av = as[ao + i] & 0xFF;
            int bv = bs[bo + i] & 0xFF;
            if (av != bv) {
                return av - bv;
            }
        }
        return a.getLength() - b.getLength();
    }

    /** 将每个字节展开为两个十六进制字符（大写），长度 {@code bytes.length * 2}。 */
    public static String toHex(byte[] bytes) {
        char[] out = new char[bytes.length * 2];
        int p = 0;
        for (byte b : bytes) {
            int v = b & 0xFF;
            out[p++] = HEX[v >>> 4];
            out[p++] = HEX[v & 0x0F];
        }
        return new String(out);
    }

    /** 将词列表转为十六进制字符串列表，便于日志中展示非 UTF-8 词。 */
    public static List<String> formatTermsHex(List<byte[]> terms) {
        List<String> out = new ArrayList<>(terms.size());
        for (byte[] t : terms) {
            out.add(toHex(t));
        }
        return out;
    }

    /** 将 ByteRef 列表转为十六进制字符串列表。 */
    public static List<String> formatTermRefsHex(List<ByteRef> terms) {
        List<String> out = new ArrayList<>(terms.size());
        for (ByteRef t : terms) {
            out.add(toHex(t.copyBytes()));
        }
        return out;
    }

    /**
     * 单文档词规范化：跳过 null 与空数组；其余按出现顺序去重（{@link LinkedHashSet}）。
     *
     * @param terms 原始词集合
     * @return 新列表；元素为 {@link ByteArrayKey} 内拷贝后的数组引用
     */
    public static List<byte[]> normalizeTerms(Collection<byte[]> terms) {
        Set<ByteArrayKey> uniq = new LinkedHashSet<>();
        for (byte[] term : terms) {
            if (term == null || term.length == 0) {
                continue;
            }
            uniq.add(new ByteArrayKey(term));
        }
        List<byte[]> out = new ArrayList<>(uniq.size());
        for (ByteArrayKey key : uniq) {
            out.add(key.bytes());
        }
        return out;
    }

    /**
     * 单文档 ByteRef 规范化：跳过 null/空区间，其余按内容去重并保持首次出现顺序。
     */
    public static List<ByteRef> normalizeTermRefs(Collection<ByteRef> terms) {
        Set<ByteRefContentKey> uniq = new LinkedHashSet<>();
        for (ByteRef term : terms) {
            if (term == null || term.getLength() == 0) {
                continue;
            }
            uniq.add(new ByteRefContentKey(term));
        }
        List<ByteRef> out = new ArrayList<>(uniq.size());
        for (ByteRefContentKey key : uniq) {
            out.add(key.ref);
        }
        return out;
    }

    private static final class ByteRefContentKey {
        private final ByteRef ref;
        private final int hash;

        private ByteRefContentKey(ByteRef ref) {
            this.ref = ref;
            this.hash = hash(ref.getSourceUnsafe(), ref.getOffset(), ref.getLength());
        }

        @Override
        public int hashCode() {
            return hash;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof ByteRefContentKey)) {
                return false;
            }
            ByteRef a = ref;
            ByteRef b = ((ByteRefContentKey) obj).ref;
            if (a.getLength() != b.getLength()) {
                return false;
            }
            for (int i = 0; i < a.getLength(); i++) {
                if (a.getSourceUnsafe()[a.getOffset() + i] != b.getSourceUnsafe()[b.getOffset() + i]) {
                    return false;
                }
            }
            return true;
        }
    }
}
