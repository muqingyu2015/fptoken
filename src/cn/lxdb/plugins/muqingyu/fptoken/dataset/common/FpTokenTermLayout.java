package cn.lxdb.plugins.muqingyu.fptoken.dataset.common;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;

/**

+----------+------+---------------------------+-------------------------------------+------------------------------------------------+
| 偏移     | 长   | 字段                      | 写段                                | 查询                                           |
+----------+------+---------------------------+-------------------------------------+------------------------------------------------+
| 0        | 2    | index_id                  | 常为 0，合并时可被替换              | 12B 前缀内；建议校验                           |
| 2        | 4    | group_id（int）           | groupIndex++ 新号写入               | fpblock_list 的 key                            |
| 6        | 1    | 块级别 group_level        | targetLevel                         | FpBlockInfo.targetLevel                        |
| 7        | 1    | hotmark                   | 热 / 普                             | 位图 hot / common 分支                         |
| 8        | 4    | termIndex + del 低位      | order 从 1 编号                     | 位图 bit = order − 1                           |
| 12       | 1    | hotDownTierBudget         | 热词=hotTermDownTierBudget；普=0    | readHotDownTierBudget；maxHotPayloadLen 截断   |
| 13+      | —    | payload                   | 纯 ngram / 词字节                   | removeHeaderBytes 后与 slice 比                |
+----------+------+---------------------------+-------------------------------------+------------------------------------------------+
 *
 */
public final class FpTokenTermLayout {
	
	public static final boolean TERM_MARK_HOT=true;
	public static final boolean TERM_MARK_COMMON=false;


	public static final int INDEX_ID_OFFSET = 0;//short:2
	public static final int GROUP_ID_OFFSET = 2;//int:4
	
	public static final int LEVEL_BYTE_OFFSET = 6;//byte:1
	public static final int HOT_TERM_FLAG_BYTE_OFFSET = 7;//byte:1 
	public static final int TERM_INDEX_AND_STATUS_OFFSET = 8;//int:4 termindex+isDelTerm
	/** 热词头内「向下扩展档」预算字节（≥1 表示至少含锚点档）；与 {@link #maxHotPayloadLen(int, int)} 配合。 */
	public static final int HOT_TERM_DOWN_TIER_BUDGET_OFFSET = 12;//byte:1

	public static final int INDEX_AND_GROUP_BYTES = 6;//short:2 int:4=INDEX_ID_OFFSET+GROUP_ID_OFFSET
	public static final int TERM_PREFIX_BYTES = 12;//short:2 int:4=INDEX_ID_OFFSET+GROUP_ID_OFFSET

	public static final int FP_HEADER_BYTES = 13;


	//[index_id:2][groupid:4][group_level:1][hotmark:1][termindex+isDelTerm:4][termwindow]
	public static void make_fp_term(BytesRef reuse, short index_id, int groupid, byte group_level, boolean hotmark,
			int termindex, boolean isDelTerm, byte hotDownTierBudget, BytesRef term) {
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
		reuse.bytes[offset + HOT_TERM_DOWN_TIER_BUDGET_OFFSET] = hotDownTierBudget;

		System.arraycopy(term.bytes, term.offset, reuse.bytes, offset + FP_HEADER_BYTES, term.length);
		reuse.length = term.length + FP_HEADER_BYTES;

	}
	
	
	public static void make_fp_search_prefix(BytesRef reuse, short index_id, int groupid, byte group_level, boolean hotmark,
			int termindex, boolean isDelTerm) {
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

		reuse.length = TERM_PREFIX_BYTES;

	}


	public static short read_index_id(byte[] term) {

		int read_index_id=NumericUtils.sortableBytesToShort(term, INDEX_ID_OFFSET);
		
		return (short) read_index_id;
	}
	
	public static int read_group_id(byte[] term) {

		int read_index_id=NumericUtils.sortableBytesToInt(term, GROUP_ID_OFFSET);
		
		return read_index_id;
	}
	
	public static int read_group_id(BytesRef term) {

		int read_index_id=NumericUtils.sortableBytesToInt(term.bytes, term.offset+GROUP_ID_OFFSET);
		
		return  read_index_id;
	}

	/**
	 * 从 {@link #INDEX_AND_GROUP_BYTES}（6 字节）流式组键读 {@code group_id}，与 {@link #make_fp_term} 写出一致（int，不截断）。
	 * 用于透传路径 {@code fpBits(index_id, logicalGroup, …)}。
	 */
	public static int readGroupIdFromIndexAndGroupKey(byte[] indexAndGroup6) {
		return NumericUtils.sortableBytesToInt(indexAndGroup6, GROUP_ID_OFFSET);
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
	
	public static int readHotDownTierBudget(BytesRef term) {
		return term.bytes[term.offset + HOT_TERM_DOWN_TIER_BUDGET_OFFSET] & 0xFF;
	}

	/**
	 * 锚点 payload 长 {@code anchorPayloadLen}、头内预算 {@code downTierBudget}（≥1）时，
	 * 热词扫描允许的最大 payload 字节长（含锚点）。
	 */
	public static int maxHotPayloadLen(int downTierBudget, int anchorPayloadLen) {
		return downTierBudget + anchorPayloadLen - 1;
	}

	public static int maxHotPayloadLenFromHeader(BytesRef termWithHeader, int anchorPayloadLen) {
		return maxHotPayloadLen(readHotDownTierBudget(termWithHeader), anchorPayloadLen);
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
