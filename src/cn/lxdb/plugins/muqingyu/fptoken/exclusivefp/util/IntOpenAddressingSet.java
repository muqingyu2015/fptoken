package cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.util;

import java.util.Arrays;

/**
 * 轻量 int 原始类型集合（开放寻址线性探测），用于减少 Integer 装箱开销。
 */
public final class IntOpenAddressingSet {
    private static final float LOAD_FACTOR = 0.65f;
    private static final int EMPTY = Integer.MIN_VALUE;

    private int[] table;
    private int mask;
    private int threshold;
    private int size;

    public IntOpenAddressingSet(int expectedSize) {
        int cap = 16;
        while (cap < Math.max(4, expectedSize * 2)) {
            cap <<= 1;
        }
        table = new int[cap];
        Arrays.fill(table, EMPTY);
        mask = cap - 1;
        threshold = Math.max(1, (int) (cap * LOAD_FACTOR));
    }

    public boolean add(int value) {
        if (value == EMPTY) {
            throw new IllegalArgumentException("value must not be Integer.MIN_VALUE");
        }
        if (size >= threshold) {
            rehash(table.length << 1);
        }
        int idx = mix(value) & mask;
        while (true) {
            int cur = table[idx];
            if (cur == EMPTY) {
                table[idx] = value;
                size++;
                return true;
            }
            if (cur == value) {
                return false;
            }
            idx = (idx + 1) & mask;
        }
    }

    public int size() {
        return size;
    }

    public int[] toArray() {
        int[] out = new int[size];
        int oi = 0;
        for (int i = 0; i < table.length; i++) {
            int v = table[i];
            if (v != EMPTY) {
                out[oi++] = v;
            }
        }
        return out;
    }

    private void rehash(int newCap) {
        int[] old = table;
        table = new int[newCap];
        Arrays.fill(table, EMPTY);
        mask = newCap - 1;
        threshold = Math.max(1, (int) (newCap * LOAD_FACTOR));
        size = 0;
        for (int i = 0; i < old.length; i++) {
            int v = old[i];
            if (v != EMPTY) {
                add(v);
            }
        }
    }

    private static int mix(int x) {
        x ^= (x >>> 16);
        x *= 0x7feb352d;
        x ^= (x >>> 15);
        x *= 0x846ca68b;
        x ^= (x >>> 16);
        return x;
    }
}
