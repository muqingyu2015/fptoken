package cn.lxdb.plugins.muqingyu.fptoken.dataset.block;

import java.lang.reflect.Field;

import org.apache.lucene.index.Terms;
import org.apache.lucene.store.IndexInput;

/**
 * 从 patched {@code FieldReader} 取出 termsbit {@code IndexInput}，供跨 group 分阶段 selective 读盘。
 */
public final class FpBitIndexTermsAccess {

	private static final String FIELD_READER = "org.apache.lucene.codecs.blocktree.FieldReader";
	private static final Field BITIN_FIELD;

	static {
		Field f = null;
		try {
			f = Class.forName(FIELD_READER).getDeclaredField("bitIn");
			f.setAccessible(true);
		} catch (ReflectiveOperationException ignored) {
			// patched Lucene 不可用时由调用方回退 fpBits 单组路径
		}
		BITIN_FIELD = f;
	}

	private FpBitIndexTermsAccess() {
	}

	/** @return termsbit 输入流（调用方勿 close）；无法取得时 null */
	public static IndexInput bitIndexInput(Terms terms) {
		if (terms == null || BITIN_FIELD == null) {
			return null;
		}
		if (!FIELD_READER.equals(terms.getClass().getName())) {
			return null;
		}
		try {
			return (IndexInput) BITIN_FIELD.get(terms);
		} catch (ReflectiveOperationException e) {
			return null;
		}
	}
}
