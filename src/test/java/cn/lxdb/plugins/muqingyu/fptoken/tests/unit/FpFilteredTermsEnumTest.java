package cn.lxdb.plugins.muqingyu.fptoken.tests.unit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.IOException;

import org.apache.lucene.index.ImpactsEnum;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.TermState;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.index.TermsEnum$SeekStatus;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.BytesRef;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import cn.lxdb.plugins.muqingyu.fptoken.api.FpFilteredTermsEnum;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpTokenTermLayout;

@Tag("lxdb-runtime")
class FpFilteredTermsEnumTest {

	private static final class StubTermsEnum extends TermsEnum {
		private int step;
		private final BytesRef[] terms;

		StubTermsEnum(BytesRef... terms) {
			this.terms = terms;
		}

		@Override
		public BytesRef next() {
			return step < terms.length ? terms[step++] : null;
		}

		@Override
		public AttributeSource attributes() {
			return new AttributeSource();
		}

		@Override
		public boolean seekExact(BytesRef text) {
			return false;
		}

		@Override
		public TermsEnum$SeekStatus seekCeil(BytesRef text) {
			return TermsEnum$SeekStatus.END;
		}

		@Override
		public void seekExact(long ord) {
		}

		@Override
		public void seekExact(BytesRef term, TermState state) {
		}

		@Override
		public BytesRef term() {
			return terms[Math.max(0, step - 1)];
		}

		@Override
		public long ord() {
			return step - 1;
		}

		@Override
		public int docFreq() {
			return 1;
		}

		@Override
		public long totalTermFreq() {
			return 1;
		}

		@Override
		public PostingsEnum postings(PostingsEnum reuse, int flags) {
			return null;
		}

		@Override
		public ImpactsEnum impacts(int flags) {
			return null;
		}

		@Override
		public TermState termState() {
			return null;
		}
	}

	@Test
	void next_injectsIndexId() throws IOException {
		BytesRef col = new BytesRef("col");
		BytesRef payload = new BytesRef("ab");
		BytesRef term = new BytesRef(new byte[128]);
		term.offset = 0;
		FpTokenTermLayout.make_fp_term(term, col, (short) 0, 1, (byte) 1, true, 7, false, (byte) 2, payload);
		StubTermsEnum inner = new StubTermsEnum(BytesRef.deepCopyOf(term));
		FpFilteredTermsEnum filtered = new FpFilteredTermsEnum(inner, 42);
		BytesRef out = filtered.next();
		assertNotNull(out);
		assertEquals((short) 42, FpTokenTermLayout.read_index_id(out));
		assertNull(filtered.next());
	}

	@Test
	void delegatesSeekAndStats() throws IOException {
		StubTermsEnum inner = new StubTermsEnum(new BytesRef("x"));
		FpFilteredTermsEnum filtered = new FpFilteredTermsEnum(inner, 1);
		assertEquals(1, filtered.docFreq());
		assertEquals(1, filtered.totalTermFreq());
		assertNotNull(filtered.attributes());
	}
}
