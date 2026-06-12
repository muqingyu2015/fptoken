package cn.lxdb.plugins.muqingyu.fptoken.pool;

import java.util.Objects;

/** 对象池槽位定义：固定 key/value 类型与初始容量。 */
final class FpHashMapPoolDef {

	private final Class<?> keyType;
	private final Class<?> valueType;
	private final int initialCapacity;
	private final FpHashMapPoolSettings settings;

	FpHashMapPoolDef(Class<?> keyType, Class<?> valueType, int initialCapacity, FpHashMapPoolSettings settings) {
		this.keyType = Objects.requireNonNull(keyType, "keyType");
		this.valueType = Objects.requireNonNull(valueType, "valueType");
		this.initialCapacity = Math.max(0, initialCapacity);
		this.settings = Objects.requireNonNull(settings, "settings");
	}

	Class<?> keyType() {
		return keyType;
	}

	Class<?> valueType() {
		return valueType;
	}

	int initialCapacity() {
		return initialCapacity;
	}

	FpHashMapPoolSettings settings() {
		return settings;
	}

	boolean matches(Class<?> keyType, Class<?> valueType) {
		return this.keyType == keyType && this.valueType == valueType;
	}
}
