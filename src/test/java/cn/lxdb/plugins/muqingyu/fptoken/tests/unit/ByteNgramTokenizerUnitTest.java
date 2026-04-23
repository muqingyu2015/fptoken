package cn.lxdb.plugins.muqingyu.fptoken.tests.unit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.ByteRef;
import cn.lxdb.plugins.muqingyu.fptoken.runner.ngram.ByteNgramTokenizer;
import java.util.List;
import org.junit.jupiter.api.Test;

class ByteNgramTokenizerUnitTest {

    @Test
    void tokenizeRefs_shouldReturnSlicesInStableOrder() {
        byte[] source = new byte[] {1, 2, 3, 4};
        List<ByteRef> refs = ByteNgramTokenizer.tokenizeRefs(source, 0, 4, 2, 3);

        assertEquals(5, refs.size());
        assertRef(refs.get(0), source, 0, 2, new byte[] {1, 2});
        assertRef(refs.get(1), source, 1, 2, new byte[] {2, 3});
        assertRef(refs.get(2), source, 2, 2, new byte[] {3, 4});
        assertRef(refs.get(3), source, 0, 3, new byte[] {1, 2, 3});
        assertRef(refs.get(4), source, 1, 3, new byte[] {2, 3, 4});
    }

    @Test
    void tokenizeRefs_shouldRespectOffsetAndLength() {
        byte[] source = new byte[] {9, 1, 2, 3, 4, 8};
        List<ByteRef> refs = ByteNgramTokenizer.tokenizeRefs(source, 1, 4, 2, 2);

        assertEquals(3, refs.size());
        assertRef(refs.get(0), source, 1, 2, new byte[] {1, 2});
        assertRef(refs.get(1), source, 2, 2, new byte[] {2, 3});
        assertRef(refs.get(2), source, 3, 2, new byte[] {3, 4});
    }

    @Test
    void tokenizeRefs_invalidRange_shouldThrow() {
        byte[] source = new byte[] {1, 2, 3};
        assertThrows(IllegalArgumentException.class, () -> ByteNgramTokenizer.tokenizeRefs(source, -1, 2, 2, 2));
        assertThrows(IllegalArgumentException.class, () -> ByteNgramTokenizer.tokenizeRefs(source, 1, 5, 2, 2));
    }

    private static void assertRef(ByteRef ref, byte[] source, int offset, int length, byte[] expected) {
        assertEquals(offset, ref.getOffset());
        assertEquals(length, ref.getLength());
        assertArrayEquals(expected, ref.copyBytes());
        // 强约束：引用同一份原始数组，避免切片复制。
        assertSame(source, ref.getSourceUnsafe());
    }
}
