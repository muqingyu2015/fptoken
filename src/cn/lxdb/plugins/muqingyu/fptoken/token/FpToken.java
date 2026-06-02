package cn.lxdb.plugins.muqingyu.fptoken.token;

import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;

import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.BytesTermAttribute;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.slf4j.Logger;

import cn.lucene.lxdb.params.LxdbLogerEncrypt;
import cn.lxdb.plugins.muqingyu.fptoken.config.Lucene80FPSearchConfig;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpTokenTermLayout;

/**
 * 按二进制滑窗（{@link BinarySlidingWindowApi#bitsetWindows64Step32}）对字段字节流分词；
 * 使用 {@link BytesTermAttribute} 输出词项字节，并对相同字节序列去重。
 *
 * <p>Reader 文本如何变成字节由 {@link FpTokenBytesMode} 决定（UTF-8 或十六进制串）。</p>
 * <p>不注册、不设置 {@code OffsetAttribute} / {@code PositionIncrementAttribute}（偏移与位置增量不需要）。</p>
 * <p>暂不使用对象池：每个去重后的词项使用独立 {@code byte[]}，由 GC 回收。</p>
 * <p>每个词项前附加 8 字节 {@code long}，使用 {@link NumericUtils#longToSortableBytes(long, byte[], int)}：
 * 无符号按字节比较顺序与 {@link Long#compare(long, long)} 一致。</p>
 */
public class FpToken extends Tokenizer {

    public static final Logger LOG = LxdbLogerEncrypt.getLogger("cl.28118");

    /** 保留与历史构造参数一致；当前索引/查询共用同一分词逻辑。 */
    private final boolean isQuery;

    private final FpTokenBytesMode bytesMode;



    private final BytesTermAttribute bytesAtt = addAttribute(BytesTermAttribute.class);
    // private final OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);
    // private final PositionIncrementAttribute posIncrAtt = addAttribute(PositionIncrementAttribute.class);

    private final BytesRef termBytes = new BytesRef();
    private final StringBuilder textBuffer = new StringBuilder();
    private final Queue<PendingTerm> pending = new ArrayDeque<>();

    
 

    

    public FpToken(boolean isQuery, FpTokenBytesMode bytesMode) {
        super();
        this.isQuery = isQuery;
        this.bytesMode = Objects.requireNonNull(bytesMode, "bytesMode");
    }

  



    @Override
    public final boolean incrementToken() throws IOException {
        clearAttributes();
        PendingTerm next = pending.poll();
        if (next == null) {
            return false;
        }
        termBytes.bytes = next.buffer;
        termBytes.offset = 0;
        termBytes.length = next.length;
        bytesAtt.setBytesRef(termBytes);
        // offsetAtt.setOffset(next.startOffset, next.endOffset);
        // posIncrAtt.setPositionIncrement(1);
        return true;
    }

    @Override
    public final void end() throws IOException {
        super.end();
        // offsetAtt.setOffset(finalOffset, finalOffset);
    }

    @Override
    public void close() throws IOException {
        super.close();
    }
    
    public static String[] ParseFieldAndText(String tokentext)
    {
    	  
        String tokenField="d";
        if(tokentext.startsWith(JSON_KEY_PREFIX_MARK))
        {
        	int pos=tokentext.indexOf("@@",JSON_KEY_PREFIX_MARK.length());
        	if(pos>0)
        	{
        		tokenField=tokentext.substring(JSON_KEY_PREFIX_MARK.length(),pos);
            	tokentext=tokentext.substring(pos+"@@".length());
        	}
        	
        }
        
        return new String[] {tokenField,tokentext};
    }

    private static String JSON_KEY_PREFIX_MARK="@@jsonkey@@";
    @Override
    public void reset() throws IOException {
        super.reset();
        pending.clear();

        fillTextBuffer(textBuffer, input);
        
        String[] fieldParse=ParseFieldAndText(textBuffer.toString());
        String tokenField=fieldParse[0];
        String tokentext=fieldParse[1];
       
        byte[] sourceBytes = textToSourceBytes(tokentext, bytesMode);
        List<WindowTerm> windows = BinarySlidingWindowApi.bitsetWindows64Step32(sourceBytes, 0, sourceBytes.length);

        BytesRef columnName=new BytesRef(tokenField);
        BytesRef reuse=new BytesRef(new byte[1024+columnName.length]);
        Map<DedupKey, PendingTerm> firstOccurrence = new LinkedHashMap<>();
        for (int i = 0; i < windows.size(); i++) {
            WindowTerm window = windows.get(i);
            byte[] padded = window.getWindowBytes();
            FpTokenTermLayout.make_fp_term(reuse, columnName, (short)0, 0, (byte)0, false, 0, false, (byte)0, new BytesRef(padded));

            
            byte[] prefixed = copyBytes(reuse);
            
            if(Lucene80FPSearchConfig.PRINT_DEBUG)
			{
				LOG.info("token: data:"+FpTokenTermLayout.toReadableString(new BytesRef(prefixed)));

			
			}
            DedupKey probe = new DedupKey(prefixed, prefixed.length);
            if (firstOccurrence.containsKey(probe)) {
                continue;
            }
            PendingTerm pt = new PendingTerm(prefixed, prefixed.length);
            firstOccurrence.put(probe, pt);
        }
        pending.addAll(firstOccurrence.values());

        if (LOG.isDebugEnabled()) {
            LOG.info("fptoken reset isQuery=" + isQuery + " bytesMode=" + bytesMode
                    + " sourceLen=" + sourceBytes.length
                    + " windows=" + windows.size() + " unique=" + pending.size());
        }
    }

    /**
     * 在负载前拼接 8 字节：{@link NumericUtils#longToSortableBytes(long, byte[], int)}。
     * {@code payload} 可为 {@code null}，视为空数组。
     */
   private static byte[] copyBytes(BytesRef reuse) {
        byte[] out = new byte[reuse.length];
//        NumericUtils.longToSortableBytes(prefix, out, 0);
        System.arraycopy(reuse.bytes, reuse.offset, out, 0, reuse.length);
        return out;
    }

    public static byte[] textToSourceBytes(String text, FpTokenBytesMode mode) {
        if (mode == FpTokenBytesMode.UTF8) {
            return text.getBytes(StandardCharsets.UTF_8);
        }
        if (mode == FpTokenBytesMode.HEX_STRING) {
            return parseHexStringToBytes(text);
        }
        throw new IllegalArgumentException("unknown FpTokenBytesMode: " + mode);
    }

    /**
     * 将十六进制字符串转为字节：跳过所有空白；长度必须为偶数；仅允许 {@code 0-9 A-F a-f}。
     */
    static byte[] parseHexStringToBytes(String hexText) {
        if (hexText == null) {
            throw new IllegalArgumentException("hexText must not be null");
        }
        int n = hexText.length();
        if (n == 0) {
            return new byte[0];
        }
        StringBuilder compact = new StringBuilder(n);
        for (int i = 0; i < n; i++) {
            char c = hexText.charAt(i);
            if (Character.isWhitespace(c)) {
                continue;
            }
            compact.append(c);
        }
        int len = compact.length();
        if (len == 0) {
            return new byte[0];
        }
        if ((len & 1) == 1) {
            throw new IllegalArgumentException("hex string must have even number of hex digits (after removing whitespace)");
        }
        int outLen = len / 2;
        byte[] out = new byte[outLen];
        for (int i = 0; i < outLen; i++) {
            int hi = Character.digit(compact.charAt(i * 2), 16);
            int lo = Character.digit(compact.charAt(i * 2 + 1), 16);
            if (hi < 0 || lo < 0) {
                throw new IllegalArgumentException("invalid hex at position " + (i * 2));
            }
            out[i] = (byte) ((hi << 4) | lo);
        }
        return out;
    }

    private static void fillTextBuffer(StringBuilder sb, Reader input) throws IOException {
        final char[] buffer = new char[8192];
        sb.setLength(0);
        int len;
        while ((len = input.read(buffer)) > 0) {
            sb.append(buffer, 0, len);
        }
    }

    public static final class PendingTerm {
        public final byte[] buffer;
        public final int length;

        public PendingTerm(byte[] buffer, int length) {
            this.buffer = buffer;
            this.length = length;
        }
    }

    /** 仅按前缀 [0,len) 参与 equals/hash，用于内容去重。 */
    public static final class DedupKey {
        private final byte[] data;
        private final int len;
        private final int hash;

        public DedupKey(byte[] data, int len) {
            this.data = Objects.requireNonNull(data, "data");
            this.len = len;
            this.hash = prefixHash(data, len);
        }

        @Override
        public int hashCode() {
            return hash;
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof DedupKey)) {
                return false;
            }
            DedupKey d = (DedupKey) o;
            return len == d.len && bytesEqual(data, d.data, len);
        }
    }

    private static int prefixHash(byte[] a, int length) {
        int h = length;
        for (int i = 0; i < length; i++) {
            h = 31 * h + a[i];
        }
        return h;
    }

    private static boolean bytesEqual(byte[] a, byte[] b, int length) {
        for (int i = 0; i < length; i++) {
            if (a[i] != b[i]) {
                return false;
            }
        }
        return true;
    }
}
