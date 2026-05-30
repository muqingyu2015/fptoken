package cn.lxdb.plugins.muqingyu.fptoken.dataset.common;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;

/**
 * FP 词项字节布局（UTF-8 列名 + 定长头 + ngram payload）：
 *
 * <pre>
 * | 0   | 4    | columnNameLen (int sortable)     |
 * | 4   | var  | columnName (JSON 字段 key 字节)   |
 * | 4+N | 14   | FP 定长头（见下表）                |
 * | 4+N+14 | — | payload（仅 ngram，不含列名）      |
 * </pre>
 *
 * FP 定长头（相对 {@link #headerOffset(BytesRef)}）：
 *
 * <pre>
 * | 0  | 2 | index_id           |
 * | 2  | 4 | group_id           |
 * | 6  | 1 | group_level        |
 * | 7  | 1 | term_flag          |
 * | 8  | 4 | termIndex          |
 * | 12 | 1 | isDelTerm          |
 * | 13 | 1 | hotDownTierBudget  |
 * </pre>
 */
public final class FpTokenTermLayout {

	private FpTokenTermLayout() {
	}

	public static final boolean TERM_MARK_HOT = true;
	public static final boolean TERM_MARK_COMMON = false;

	public static final int COLUMN_NAME_LEN_BYTES = 4;
	public static final int COLUMN_NAME_LEN_OFFSET = 0;

	public static final int INDEX_ID_OFFSET = 0;
	public static final int GROUP_ID_OFFSET = 2;
	public static final int GROUP_LEVEL_OFFSET = 6;
	public static final int TERM_FLAG_OFFSET = 7;
	public static final int TERM_INDEX_OFFSET = 8;
	public static final int TERM_STATUS_OFFSET = 12;
	public static final int HOT_TERM_DOWN_TIER_BUDGET_OFFSET = 13;

	public static final int INDEX_AND_GROUP_BYTES = 6;
	/** seek 前缀：列名 + FP 头至 termIndex（不含 del / budget）。 */
	public static final int TERM_PREFIX_BYTES = 12;
	public static final int FP_HEADER_BYTES = 14;

	public static int readColumnNameLen(BytesRef term) {
		if (term.length < COLUMN_NAME_LEN_BYTES) {
			return 0;
		}
		return NumericUtils.sortableBytesToInt(term.bytes, term.offset + COLUMN_NAME_LEN_OFFSET);
	}
	public static int readColumnNameLen(byte[] term) {
		if (term.length < COLUMN_NAME_LEN_BYTES) {
			return 0;
		}
		return NumericUtils.sortableBytesToInt(term,  COLUMN_NAME_LEN_OFFSET);
	}
	public static int columnNamePrefixBytes(BytesRef columnName) {
		return COLUMN_NAME_LEN_BYTES + columnName.length;
	}

	public static int columnNamePrefixBytesFromLen(int columnNameLen) {
		return COLUMN_NAME_LEN_BYTES + columnNameLen;
	}

	public static int headerOffset(byte[] term) {
		return columnNamePrefixBytesFromLen(readColumnNameLen(term));
	}
	/** 列名前缀 + FP 头起始偏移。 */
	public static int headerOffset(BytesRef term) {
		return columnNamePrefixBytesFromLen(readColumnNameLen(term));
	}

//	public static boolean hasFpHeader(BytesRef term) {
//		return term.length >= headerOffset(term) + FP_HEADER_BYTES;
//	}

	public static BytesRef readColumnName(BytesRef term) {
		final int len = readColumnNameLen(term);
		if (len <= 0 || term.length < COLUMN_NAME_LEN_BYTES + len) {
			return new BytesRef();
		}
		return new BytesRef(term.bytes, term.offset + COLUMN_NAME_LEN_BYTES, len);
	}

	public static boolean columnNameEquals(BytesRef term, BytesRef expectedColumnName) {
		final BytesRef found = readColumnName(term);
		return found.equals(expectedColumnName);
	}

	public static void writeColumnNamePrefix(BytesRef reuse, BytesRef columnName, int destOffset) {
		NumericUtils.intToSortableBytes(columnName.length, reuse.bytes, destOffset);
		System.arraycopy(columnName.bytes, columnName.offset, reuse.bytes, destOffset + COLUMN_NAME_LEN_BYTES,
				columnName.length);
	}

//	/** Map 键：[4][columnName][ngramPayload]。 */
//	public static void writeColumnPayloadKey1(BytesRef reuse, BytesRef columnName, BytesRef ngramPayload) {
//		final int colPrefix = columnNamePrefixBytes(columnName);
//		writeColumnNamePrefix(reuse, columnName, reuse.offset);
//		System.arraycopy(ngramPayload.bytes, ngramPayload.offset, reuse.bytes, reuse.offset + colPrefix,
//				ngramPayload.length);
//		reuse.length = colPrefix + ngramPayload.length;
//	}

//	public static BytesRef readNgramPayloadFromColumnKey1(BytesRef columnPayloadKey) {
//		final int colPrefix = columnNamePrefixBytesFromLen(readColumnNameLen(columnPayloadKey));
//		if (columnPayloadKey.length <= colPrefix) {
//			return new BytesRef();
//		}
//		return new BytesRef(columnPayloadKey.bytes, columnPayloadKey.offset + colPrefix,
//				columnPayloadKey.length - colPrefix);
//	}

	public static void make_fp_term(BytesRef reuse, BytesRef columnName, short index_id, int groupid, byte group_level,
			boolean hotmark, int termindex, boolean isDelTerm, byte hotDownTierBudget, BytesRef ngramPayload) {
		final int colPrefix = columnNamePrefixBytes(columnName);
		final int headerStart = reuse.offset + colPrefix;
		writeColumnNamePrefix(reuse, columnName, reuse.offset);

		NumericUtils.shortToSortableBytes(index_id, reuse.bytes, headerStart + INDEX_ID_OFFSET);
		NumericUtils.intToSortableBytes(groupid, reuse.bytes, headerStart + GROUP_ID_OFFSET);
		reuse.bytes[headerStart + GROUP_LEVEL_OFFSET] = (byte) (group_level & 0xFF);
		reuse.bytes[headerStart + TERM_FLAG_OFFSET] = (byte) ((hotmark ? 1 : 0) & 0xFF);
		NumericUtils.intToSortableBytes(termindex, reuse.bytes, headerStart + TERM_INDEX_OFFSET);
		reuse.bytes[headerStart + TERM_STATUS_OFFSET] = (byte) ((isDelTerm ? 1 : 0) & 0xFF);
		reuse.bytes[headerStart + HOT_TERM_DOWN_TIER_BUDGET_OFFSET] = hotDownTierBudget;

		System.arraycopy(ngramPayload.bytes, ngramPayload.offset, reuse.bytes, headerStart + FP_HEADER_BYTES,
				ngramPayload.length);
		reuse.length = colPrefix + FP_HEADER_BYTES + ngramPayload.length;
	}

	public static void make_fp_search_prefix(BytesRef reuse, BytesRef columnName, short index_id, int groupid,
			byte group_level, boolean hotmark, int termindex) {
		final int colPrefix = columnNamePrefixBytes(columnName);
		final int headerStart = reuse.offset + colPrefix;
		writeColumnNamePrefix(reuse, columnName, reuse.offset);

		NumericUtils.shortToSortableBytes(index_id, reuse.bytes, headerStart + INDEX_ID_OFFSET);
		NumericUtils.intToSortableBytes(groupid, reuse.bytes, headerStart + GROUP_ID_OFFSET);
		reuse.bytes[headerStart + GROUP_LEVEL_OFFSET] = (byte) (group_level & 0xFF);
		reuse.bytes[headerStart + TERM_FLAG_OFFSET] = (byte) ((hotmark ? 1 : 0) & 0xFF);
		NumericUtils.intToSortableBytes(termindex, reuse.bytes, headerStart + TERM_INDEX_OFFSET);
		reuse.length = colPrefix + TERM_PREFIX_BYTES;
	}
	
	
	public static void modify_index_id(BytesRef term, int index_id) {
		int h=headerOffset(term);
		if(term.length<(h+INDEX_ID_OFFSET+2))
		{
			return ;
		}
		NumericUtils.shortToSortableBytes(index_id, term.bytes, headerOffset(term) + INDEX_ID_OFFSET);
	}

	public static short read_index_id1(byte[] term) {
		return (short) NumericUtils.sortableBytesToShort(term, headerOffset(term)+INDEX_ID_OFFSET);
	}

	public static short read_index_id(BytesRef term) {
		return (short) NumericUtils.sortableBytesToShort(term.bytes, headerOffset(term) + INDEX_ID_OFFSET);
	}

	public static int read_group_id1(byte[] term) {
		return NumericUtils.sortableBytesToInt(term, GROUP_ID_OFFSET);
	}

	public static int read_group_id(BytesRef term) {
		return NumericUtils.sortableBytesToInt(term.bytes, headerOffset(term) + GROUP_ID_OFFSET);
	}

	public static int readGroupId(byte[] indexAndGroup6) {
		return NumericUtils.sortableBytesToInt(indexAndGroup6,headerOffset(indexAndGroup6) + GROUP_ID_OFFSET);
	}

	public static int readLevel(BytesRef term) {
		return term.bytes[headerOffset(term) + GROUP_LEVEL_OFFSET] & 0xFF;
	}

	public static boolean isHotTerm(BytesRef term) {
		
		return (term.bytes[headerOffset(term) + TERM_FLAG_OFFSET] & 0xFF) == 1;
	}

	public static int readTermIndex(BytesRef term) {
		return NumericUtils.sortableBytesToInt(term.bytes, headerOffset(term) + TERM_INDEX_OFFSET);
	}

	public static boolean readIsDelTerm(BytesRef term) {
		
		return (term.bytes[headerOffset(term) + TERM_STATUS_OFFSET] & 0xFF) == 1;
	}

	public static int readHotDownTierBudget(BytesRef term) {
		
		return term.bytes[headerOffset(term) + HOT_TERM_DOWN_TIER_BUDGET_OFFSET] & 0xFF;
	}

	public static int maxHotPayloadLen(int downTierBudget, int anchorPayloadLen) {
		return downTierBudget + anchorPayloadLen - 1;
	}

	public static int maxHotPayloadLenFromHeader(BytesRef termWithHeader, int anchorPayloadLen) {
		return maxHotPayloadLen(readHotDownTierBudget(termWithHeader), anchorPayloadLen);
	}



	/** 仅 ngram payload（不含列名与 FP 头）。 */
	public static BytesRef removeColumnAndHeaderBytes(BytesRef term) {
	
		final int h = headerOffset(term);
		return new BytesRef(term.bytes, term.offset + h + FP_HEADER_BYTES, term.length - h - FP_HEADER_BYTES);
	}

	public static BytesRef clearAndCopyGroupBytes(BytesRef term) {
		final BytesRef termnew = BytesRef.deepCopyOf(term);
		final int h = headerOffset(termnew);
		for (int i = 0; i < INDEX_AND_GROUP_BYTES; i++) {
			termnew.bytes[termnew.offset + h + i] = 0;
		}
		return termnew;
	}

	public static BytesRef copyGroupKey(BytesRef term) {
		final byte[] b = new byte[INDEX_AND_GROUP_BYTES];
		final int h = headerOffset(term);
		System.arraycopy(term.bytes, term.offset + h, b, 0, INDEX_AND_GROUP_BYTES);
		return new BytesRef(b);
	}

	public static byte[] column_index_group_copy(BytesRef term) {
		final int h = headerOffset(term);
		byte[] dest6=new byte[h+INDEX_AND_GROUP_BYTES];
		System.arraycopy(term.bytes, term.offset, dest6, 0, h+INDEX_AND_GROUP_BYTES);
		return dest6;
	}
	public static boolean column_equals(BytesRef term, byte[] six) {
		
		final int h = headerOffset(term);
		int len=h;
		for (int i = 0; i < len; i++) {
			if (term.bytes[term.offset + i] != six[i]) {
				return false;
			}
		}
		return true;
	}
	public static boolean column_index_group_equals(BytesRef term, byte[] six) {
		
		final int h = headerOffset(term);
		int len=h+INDEX_AND_GROUP_BYTES;
		for (int i = 0; i < len; i++) {
			if (term.bytes[term.offset + i] != six[i]) {
				return false;
			}
		}
		return true;
	}
}
