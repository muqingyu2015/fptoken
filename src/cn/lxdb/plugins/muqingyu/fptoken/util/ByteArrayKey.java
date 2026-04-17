package cn.lxdb.plugins.muqingyu.fptoken.util;

/**
 * 作者：muqingyu
 *
 * byte[] 内容键，支持按内容比较与哈希。
 *
 * 关键点：
 * - 构造时会拷贝数组，避免外部修改底层字节导致 Map 键失效。
 * - hashCode/equals 按字节内容实现，确保语义正确。
 */
public final class ByteArrayKey implements Comparable<ByteArrayKey> {

    private final byte[] value;
    private final int hash;

    public ByteArrayKey(byte[] value) {
        this.value = ByteArrayUtils.copy(value);
        this.hash = ByteArrayUtils.hash(this.value);
    }

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
