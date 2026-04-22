package cn.lxdb.plugins.muqingyu.fptoken.exclusivefp.util;

/**
 * 以 byte[] <b>内容</b> 为逻辑的键：可用于 {@link java.util.HashMap} 等结构。
 *
 * <p><b>语义</b>：
 * <ul>
 *   <li>构造时拷贝入参数组，调用方后续修改原数组不会影响键值。</li>
 *   <li>{@link #equals(Object)} / {@link #hashCode()} 按字节序列；{@link #compareTo} 使用无符号字节序（见 {@link ByteArrayUtils#compareUnsigned}）。</li>
 * </ul>
 *
 * @author muqingyu
 */
public final class ByteArrayKey implements Comparable<ByteArrayKey> {

    private byte[] value;
    private int hash;

    /**
     * @param value 原始词字节，非 null
     */
    public ByteArrayKey(byte[] value) {
        this.value = ByteArrayUtils.copy(value);
        this.hash = ByteArrayUtils.hash(this.value);
    }

    /**
     * 不做数组拷贝的快速查找键：直接引用入参数组，不做拷贝。
     * <b>调用方必须确保入参数组在查找期间不被修改。</b>
     */
    public static ByteArrayKey forLookup(byte[] value, int hash) {
        ByteArrayKey k = new ByteArrayKey();
        k.value = value;
        k.hash = hash;
        return k;
    }

    // 私有构造器，供 forLookup 使用
    private ByteArrayKey() {
        this.value = null;
        this.hash = 0;
    }

    /** 返回内部持有的字节数组引用（与构造时拷贝为同一块数组）。 */
    public byte[] bytes() {
        return value;
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
        if (!(obj instanceof ByteArrayKey)) {
            return false;
        }
        ByteArrayKey other = (ByteArrayKey) obj;
        if (value.length != other.value.length) {
            return false;
        }
        for (int i = 0; i < value.length; i++) {
            if (value[i] != other.value[i]) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int compareTo(ByteArrayKey o) {
        return ByteArrayUtils.compareUnsigned(value, o.value);
    }
}
