package cn.lxdb.plugins.muqingyu.fptoken.dataset.common;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;

/**
 * FP token 词项字节布局：前 6 字节为组号（与 {@link cn.lxdb.plugins.muqingyu.fptoken.utils.FpTokenProcessLocalId} 截断一致），
 * 第 7 字节（下标 6）为块级别 0~3；第 8 字节（下标 7）为热词标记（见 {@link #isHotTerm(BytesRef)}）。
 */
public final class FpTokenTermLayout {
	
	public static final boolean TERM_MARK_HOT=true;
	public static final boolean TERM_MARK_COMMON=false;


	public static final int INDEX_ID_OFFSET = 0;//short:2
	public static final int GROUP_ID_OFFSET = 2;//int:4
	
	public static final int LEVEL_BYTE_OFFSET = 6;//byte:1
	public static final int HOT_TERM_FLAG_BYTE_OFFSET = 7;//byte:1 
	public static final int TERM_INDEX_AND_STATUS_OFFSET = 8;//int:4 termindex+isDelTerm
	
	public static final int INDEX_AND_GROUP_BYTES = 6;//short:2 int:4=INDEX_ID_OFFSET+GROUP_ID_OFFSET
	public static final int FP_HEADER_BYTES = 12;


	//[index_id:2][groupid:4][group_level:1][hotmark:1][termindex+isDelTerm:4][termwindow]
	public static void make_fp_term(BytesRef reuse, short index_id, int groupid, byte group_level, boolean hotmark,
			int termindex, boolean isDelTerm, BytesRef term) {
		int offset = reuse.offset;

		int term_index_tp = termindex << 1;
		if (isDelTerm) {
			term_index_tp += 1;
		}

		NumericUtils.shortToSortableBytes(index_id, reuse.bytes, offset + INDEX_ID_OFFSET);// 仅仅是占位,会在索引合并的时候替换,平时都是0
		NumericUtils.intToSortableBytes(groupid, reuse.bytes, offset + GROUP_ID_OFFSET);
		reuse.bytes[offset + LEVEL_BYTE_OFFSET] = (byte) (group_level & 0xFF);
		reuse.bytes[offset + HOT_TERM_FLAG_BYTE_OFFSET] = (byte) ((hotmark?1:0) & 0xFF);
		NumericUtils.intToSortableBytes(term_index_tp, reuse.bytes, offset + TERM_INDEX_AND_STATUS_OFFSET);
		System.arraycopy(term.bytes, term.offset, reuse.bytes, offset + FP_HEADER_BYTES, term.length);
		reuse.length = term.length + FP_HEADER_BYTES;

	}


	public static short read_index_id(byte[] term) {

		int read_index_id=NumericUtils.sortableBytesToShort(term, INDEX_ID_OFFSET);
		
		return (short) read_index_id;
	}
	
	public static short read_group_id(byte[] term) {

		int read_index_id=NumericUtils.sortableBytesToInt(term, GROUP_ID_OFFSET);
		
		return (short) read_index_id;
	}
	
	/**
	 * 读取块级别字节（无符号 0~255；业务上常用 0~3）。
	 *
	 * @param term 须满足 {@link #hasFpPrefix(BytesRef)}
	 * @return 级别字节值
	 */
	public static int readLevel(BytesRef term) {
		return term.bytes[term.offset + LEVEL_BYTE_OFFSET] & 0xFF;
	}
	
	/**
	 * 根据第 8 字节判断是否为热词：仅当 {@code (bytes[offset+7] & 0xFF) == 1} 时为热词。
	 *
	 * @param term 词项；长度过短时视为非热词
	 * @return 热词为 true
	 */
	public static boolean isHotTerm(BytesRef term) {
		return (term.bytes[term.offset + HOT_TERM_FLAG_BYTE_OFFSET] & 0xFF) == 1;
	}
	
	public static boolean readIsDelTerm(BytesRef term) {
		int offset = term.offset;

		int term_index_tp=NumericUtils.sortableBytesToInt(term.bytes, offset+TERM_INDEX_AND_STATUS_OFFSET);
		
		return (term_index_tp&1)>0;
	}
	
	public static int readTermIndex(BytesRef term) {
		int offset = term.offset;

		int term_index_tp=NumericUtils.sortableBytesToInt(term.bytes, offset+TERM_INDEX_AND_STATUS_OFFSET);
		
		return term_index_tp>>1;
	}
	
	

	
	public static BytesRef removeHeaderBytes(BytesRef term) {
		return new BytesRef(term.bytes, term.offset+FP_HEADER_BYTES, term.length-FP_HEADER_BYTES);
	}
	
	public static BytesRef clearAndCopyGroupBytes(BytesRef term) {
		BytesRef termnew=BytesRef.deepCopyOf(term);
		for(int i=0;i<INDEX_AND_GROUP_BYTES;i++)
		{
			termnew.bytes[i+termnew.offset]=0;
		}
				
		return termnew;
	}



	

	/**
	 * 拷贝词项前 {@link #INDEX_AND_GROUP_BYTES} 字节为新的 {@link BytesRef}。
	 * <p>
	 * 热路径若可复用缓冲区，请直接对 {@code term.bytes[term.offset + i]} 与已有 {@code byte[6]} 比较，避免分配。
	 *
	 * @param term 源词项
	 * @return 仅含 6 字节组号的副本
	 */
	public static BytesRef copyGroupKey(BytesRef term) {
		final byte[] b = new byte[INDEX_AND_GROUP_BYTES];
		System.arraycopy(term.bytes, term.offset, b, 0, INDEX_AND_GROUP_BYTES);
		return new BytesRef(b);
	}


	public static void copyIndexAndGroup(BytesRef term, byte[] dest6) {
		System.arraycopy(term.bytes, term.offset, dest6, 0, INDEX_AND_GROUP_BYTES);
	}

	public static boolean indexAndGroupEquals(BytesRef term, byte[] six) {
		for (int i = 0; i < INDEX_AND_GROUP_BYTES; i++) {
			if (term.bytes[term.offset + i] != six[i]) {
				return false;
			}
		}
		return true;
	}
}
