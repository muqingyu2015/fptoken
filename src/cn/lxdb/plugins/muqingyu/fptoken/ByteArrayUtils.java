package cn.lxdb.plugins.muqingyu.fptoken;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * 作者：muqingyu
 *
 * byte[] 相关公共工具。
 *
 * 为什么需要这层工具：
 * - Java 原生 byte[] 没有按“内容”实现 equals/hashCode，不能直接当 Map 键。
 * - 这里集中放置复制、哈希、无符号比较、十六进制展示等能力。
 */
final class ByteArrayUtils {

    private static final char[] HEX = "0123456789ABCDEF".toCharArray();

    private ByteArrayUtils() {
    }

    static byte[] copy(byte[] src) {
        byte[] out = new byte[src.length];
        System.arraycopy(src, 0, out, 0, src.length);
        return out;
    }

    static int hash(byte[] arr) {
        // 与 ByteArrayKey 配套的内容哈希实现。
        int h = 1;
        for (byte b : arr) {
            h = 31 * h + (b & 0xFF);
        }
        return h;
    }

    static int compareUnsigned(byte[] a, byte[] b) {
        // 按无符号字节比较，避免负值字节影响顺序语义。
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

    static String toHex(byte[] bytes) {
        char[] out = new char[bytes.length * 2];
        int p = 0;
        for (byte b : bytes) {
            int v = b & 0xFF;
            out[p++] = HEX[v >>> 4];
            out[p++] = HEX[v & 0x0F];
        }
        return new String(out);
    }

    static List<String> formatTermsHex(List<byte[]> terms) {
        List<String> out = new ArrayList<String>(terms.size());
        for (byte[] t : terms) {
            out.add(toHex(t));
        }
        return out;
    }

    static List<byte[]> normalizeTerms(Collection<byte[]> terms) {
        // 单文档内做去重和空值过滤，避免重复词放大后续计算成本。
        Set<ByteArrayKey> uniq = new LinkedHashSet<ByteArrayKey>();
        for (byte[] term : terms) {
            if (term == null || term.length == 0) {
                continue;
            }
            uniq.add(new ByteArrayKey(term));
        }
        List<byte[]> out = new ArrayList<byte[]>(uniq.size());
        for (ByteArrayKey key : uniq) {
            out.add(key.bytes());
        }
        return out;
    }
}
