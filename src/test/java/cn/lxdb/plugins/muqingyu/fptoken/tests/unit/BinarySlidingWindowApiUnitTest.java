package cn.lxdb.plugins.muqingyu.fptoken.tests.unit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import cn.lxdb.plugins.muqingyu.fptoken.api.BinarySlidingWindowApi;
import cn.lxdb.plugins.muqingyu.fptoken.api.BinarySlidingWindowApi.WindowTerm;
import cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.model.ByteRef;
import java.util.List;
import org.junit.jupiter.api.Test;

class BinarySlidingWindowApiUnitTest {

    @Test
    void bitsetWindows64Step32_shouldKeepCrossBoundaryWindowsInOrder() {
        byte[] bytes = sequence(96);
        List<WindowTerm> windows = BinarySlidingWindowApi.bitsetWindows64Step32(bytes, 0, bytes.length);

        assertEquals(3, windows.size());
        assertWindow(windows.get(0), 0, 64, sequence(0, 64));
        assertWindow(windows.get(1), 32, 64, sequence(32, 64));
        assertWindow(windows.get(2), 64, 32, sequence(64, 32));
    }

    @Test
    void bitsetWindows64Step32_shouldKeepTailShortWindow() {
        byte[] bytes = sequence(100);
        List<WindowTerm> windows = BinarySlidingWindowApi.bitsetWindows64Step32(bytes, 0, bytes.length);

        assertEquals(3, windows.size());
        assertWindow(windows.get(0), 0, 64, sequence(0, 64));
        assertWindow(windows.get(1), 32, 64, sequence(32, 64));
        assertWindow(windows.get(2), 64, 36, sequence(64, 36));
    }

    @Test
    void termVectors32_shouldSplitNonOverlap() {
        byte[] bytes = sequence(70);
        List<WindowTerm> vectors = BinarySlidingWindowApi.termVectors32(bytes, 0, bytes.length);

        assertEquals(3, vectors.size());
        assertWindow(vectors.get(0), 0, 32, sequence(0, 32));
        assertWindow(vectors.get(1), 32, 32, sequence(32, 32));
        assertWindow(vectors.get(2), 64, 6, sequence(64, 6));
    }

    @Test
    void slidingWindows_shouldRespectOffsetAndLengthRange() {
        byte[] bytes = sequence(30);
        List<WindowTerm> windows = BinarySlidingWindowApi.slidingWindows(bytes, 10, 14, 8, 4);

        assertEquals(3, windows.size());
        assertWindow(windows.get(0), 10, 8, sequence(10, 8));
        assertWindow(windows.get(1), 14, 8, sequence(14, 8));
        assertWindow(windows.get(2), 18, 6, sequence(18, 6));
    }

    @Test
    void slidingWindows_shouldReturnEmptyWhenLengthIsZero() {
        byte[] bytes = sequence(10);
        List<WindowTerm> windows = BinarySlidingWindowApi.slidingWindows(bytes, 5, 0, 4, 2);
        assertEquals(0, windows.size());
    }

    @Test
    void slidingWindows_shouldValidateBoundsAndSizes() {
        byte[] bytes = sequence(16);

        assertThrows(IllegalArgumentException.class, () ->
                BinarySlidingWindowApi.slidingWindows(null, 0, 1, 4, 2));
        assertThrows(IllegalArgumentException.class, () ->
                BinarySlidingWindowApi.slidingWindows(bytes, -1, 1, 4, 2));
        assertThrows(IllegalArgumentException.class, () ->
                BinarySlidingWindowApi.slidingWindows(bytes, 0, -1, 4, 2));
        assertThrows(IllegalArgumentException.class, () ->
                BinarySlidingWindowApi.slidingWindows(bytes, 0, 1, 0, 2));
        assertThrows(IllegalArgumentException.class, () ->
                BinarySlidingWindowApi.slidingWindows(bytes, 0, 1, 4, 0));
        assertThrows(IllegalArgumentException.class, () ->
                BinarySlidingWindowApi.slidingWindows(bytes, 17, 0, 4, 2));
        assertThrows(IllegalArgumentException.class, () ->
                BinarySlidingWindowApi.slidingWindows(bytes, 10, 7, 4, 2));
    }

    private static void assertWindow(WindowTerm term, int expectedOffset, int expectedLength, byte[] expectedBytes) {
        ByteRef ref = term.getSourceRef();
        assertEquals(expectedOffset, ref.getOffset());
        assertEquals(expectedLength, ref.getLength());
        assertArrayEquals(expectedBytes, term.getWindowBytes());
    }

    private static byte[] sequence(int length) {
        return sequence(0, length);
    }

    private static byte[] sequence(int start, int length) {
        byte[] out = new byte[length];
        for (int i = 0; i < length; i++) {
            out[i] = (byte) (start + i);
        }
        return out;
    }
}
