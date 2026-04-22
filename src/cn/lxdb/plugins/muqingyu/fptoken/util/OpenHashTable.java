package cn.lxdb.plugins.muqingyu.fptoken.util;

/**
 * 开放寻址哈希表：byte[] → int（termId），专为 TermTidsetIndex.build() 优化。
 *
 * <p>相比 {@link java.util.HashMap} 的优势：
 * <ul>
 *   <li>没有 Entry 对象，数据在三个并行数组中连续存储，缓存友好</li>
 *   <li>没有 hash 扰动（直接使用 ByteArrayUtils.hash 的原始值）</li>
 *   <li>没有 equals() 虚方法调用，内联字节比较</li>
 *   <li>预分配容量，零 rehash</li>
 * </ul>
 *
 * <p>冲突处理：线性探测（cache-friendly，冲突概率低时性能优于二次探测）。</p>
 */
public final class OpenHashTable {

    private static final int DEFAULT_CAPACITY = 4096;
    private static final float LOAD_FACTOR = 0.75f;

    /** 存储的 term 字节数组（keys）。空槽为 null。 */
    private byte[][] keys;
    /** 与 keys 并行：term 的 hash 值。空槽为 0。 */
    private int[] hashes;
    /** 与 keys 并行：termId。空槽为 -1。 */
    private int[] values;
    private int size;
    private int mask; // capacity - 1，用于位运算取模

    public OpenHashTable() {
        this(DEFAULT_CAPACITY);
    }

    public OpenHashTable(int expectedSize) {
        int capacity = tableSizeFor(Math.max(expectedSize, DEFAULT_CAPACITY));
        this.keys = new byte[capacity][];
        this.hashes = new int[capacity];
        this.values = new int[capacity];
        java.util.Arrays.fill(values, -1);
        this.mask = capacity - 1;
        this.size = 0;
    }

    /**
     * 查找或插入 term。
     *
     * @param rawTerm term 字节，不拷贝（调用方需确保引用稳定）
     * @param hash    预计算的 hash 值
     * @param insertIfMissing 如果为 true 且不存在，则分配新 termId 并插入
     * @return 如果找到或成功插入，返回 termId；如果 insertIfMissing=false 且不存在，返回 -1
     */
    public int getOrPut(byte[] rawTerm, int hash, boolean insertIfMissing) {
        int idx = hash & mask;
        int firstTombstone = -1;

        while (true) {
            byte[] k = keys[idx];
            if (k == null) {
                // 空槽 → 没找到
                if (!insertIfMissing) {
                    return -1;
                }
                // 插入新值（拷贝 byte[] 以确保引用稳定性）
                int termId = size;
                keys[idx] = rawTerm.clone();
                hashes[idx] = hash;
                values[idx] = termId;
                size++;
                // 扩容检查
                if (size > keys.length * LOAD_FACTOR) {
                    rehash();
                }
                return termId;
            }
            if (hashes[idx] == hash && bytesEqual(k, rawTerm)) {
                // 找到已有 term
                return values[idx];
            }
            // 线性探测
            idx = (idx + 1) & mask;
        }
    }

    /**
     * 只查找，不插入。
     */
    public int get(byte[] rawTerm, int hash) {
        return getOrPut(rawTerm, hash, false);
    }

    /** 当前存储的 term 数量。 */
    public int size() {
        return size;
    }

    /**
     * 获取指定 termId 对应的 byte[] 引用。
     * termId 必须是由 getOrPut 返回的合法值。
     */
    public byte[] getKeyBytes(int termId) {
        for (int i = 0; i < keys.length; i++) {
            if (values[i] == termId) {
                return keys[i];
            }
        }
        throw new IllegalArgumentException("termId not found: " + termId);
    }

    // ===== 内部 =====

    private static boolean bytesEqual(byte[] a, byte[] b) {
        if (a.length != b.length) {
            return false;
        }
        for (int i = 0; i < a.length; i++) {
            if (a[i] != b[i]) {
                return false;
            }
        }
        return true;
    }

    private void rehash() {
        int newCapacity = keys.length * 2;
        byte[][] oldKeys = keys;
        int[] oldHashes = hashes;
        int[] oldValues = values;

        keys = new byte[newCapacity][];
        hashes = new int[newCapacity];
        values = new int[newCapacity];
        java.util.Arrays.fill(values, -1);
        mask = newCapacity - 1;
        size = 0;

        for (int i = 0; i < oldKeys.length; i++) {
            if (oldKeys[i] != null) {
                int idx = oldHashes[i] & mask;
                while (keys[idx] != null) {
                    idx = (idx + 1) & mask;
                }
                keys[idx] = oldKeys[i];
                hashes[idx] = oldHashes[i];
                values[idx] = oldValues[i];
                size++;
            }
        }
    }

    private static int tableSizeFor(int cap) {
        int n = cap - 1;
        n |= n >>> 1;
        n |= n >>> 2;
        n |= n >>> 4;
        n |= n >>> 8;
        n |= n >>> 16;
        return n + 1;
    }
}
