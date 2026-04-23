package cn.lxdb.plugins.muqingyu.fptoken.runner.ngram;

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
        if (lineBytes == null || lineBytes.length == 0) {
            return new ArrayList<byte[]>(0);
        }
        if (ngramStart <= 0 || ngramEnd < ngramStart) {
            return new ArrayList<byte[]>(0);
        }

        int estimatedSize = estimateTokenCount(lineBytes.length, ngramStart, ngramEnd);
        List<byte[]> out = new ArrayList<byte[]>(estimatedSize);
        for (int n = ngramStart; n <= ngramEnd; n++) {
            if (n > lineBytes.length) {
                break;
            }
            for (int i = 0; i + n <= lineBytes.length; i++) {
                out.add(Arrays.copyOfRange(lineBytes, i, i + n));
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
