package cn.lxdb.plugins.muqingyu.fptoken.util;

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
}
