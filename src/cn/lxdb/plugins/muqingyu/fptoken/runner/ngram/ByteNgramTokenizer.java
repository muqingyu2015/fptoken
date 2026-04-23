package cn.lxdb.plugins.muqingyu.fptoken.runner.ngram;

import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.ByteRef;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * 将一条 byte 记录切分为 n-gram 词项（支持区间 n）。
 */
public final class ByteNgramTokenizer {

    private ByteNgramTokenizer() {
    }

    /**
     * @param lineBytes 输入字节数组
     * @param ngramStart 最小 n（含）
     * @param ngramEnd 最大 n（含）
     * @return n-gram 列表；当输入为空或参数非法时返回空
     */
    public static List<byte[]> tokenize(byte[] lineBytes, int ngramStart, int ngramEnd) {
        List<ByteRef> refs = tokenizeRefs(lineBytes, 0, lineBytes == null ? 0 : lineBytes.length, ngramStart, ngramEnd);
        List<byte[]> out = new ArrayList<>(refs.size());
        for (ByteRef ref : refs) {
            out.add(Arrays.copyOfRange(
                    ref.getSourceUnsafe(),
                    ref.getOffset(),
                    ref.getOffset() + ref.getLength()));
        }
        return out;
    }

    /**
     * ByteRef 版本：返回共享原始数组的切片引用，避免每个 token 都创建新 byte[]。
     */
    public static List<ByteRef> tokenizeRefs(
            byte[] lineBytes,
            int offset,
            int length,
            int ngramStart,
            int ngramEnd
    ) {
        if (lineBytes == null || length == 0) {
            return new ArrayList<>(0);
        }
        if (offset < 0 || length < 0 || offset > lineBytes.length || offset + length > lineBytes.length) {
            throw new IllegalArgumentException("offset/length out of range");
        }
        if (ngramStart <= 0 || ngramEnd < ngramStart) {
            return new ArrayList<>(0);
        }

        int estimatedSize = estimateTokenCount(length, ngramStart, ngramEnd);
        List<ByteRef> out = new ArrayList<>(estimatedSize);
        int endExclusive = offset + length;
        for (int n = ngramStart; n <= ngramEnd; n++) {
            if (n > length) {
                break;
            }
            for (int i = offset; i + n <= endExclusive; i++) {
                out.add(new ByteRef(lineBytes, i, n));
            }
        }
        return out;
    }

    private static int estimateTokenCount(int lineLength, int ngramStart, int ngramEnd) {
        int total = 0;
        for (int n = ngramStart; n <= ngramEnd; n++) {
            if (n > lineLength) {
                break;
            }
            total += (lineLength - n + 1);
        }
        return total;
    }
}
