package cn.lxdb.plugins.muqingyu.fptoken.api;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.TreeMap;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Terms;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.BitDocIdSet;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.slf4j.Logger;

import cn.lucene.lxdb.params.LxdbLogerEncrypt;
import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FpBlockInfo;
import cn.lxdb.plugins.muqingyu.fptoken.token.BinarySlidingWindowApi;
import cn.lxdb.plugins.muqingyu.fptoken.token.FpToken;
import cn.lxdb.plugins.muqingyu.fptoken.token.FpToken.DedupKey;
import cn.lxdb.plugins.muqingyu.fptoken.token.FpToken.PendingTerm;
import cn.lxdb.plugins.muqingyu.fptoken.token.FpTokenBytesMode;
import cn.lxdb.plugins.muqingyu.fptoken.token.WindowTerm;

public class FpTokenQuery extends Query {
	public static final Logger LOG = LxdbLogerEncrypt.getLogger("mqy.fptoken");
	private static final long serialVersionUID = -4451981415049319577L;
	private final String fieldName;
    private final BytesRef[] slices;


    public FpTokenQuery(String fieldName, String slices,FpTokenBytesMode mode) {
        this.fieldName = fieldName;
        byte[] sourceBytes=FpToken.textToSourceBytes(slices, mode);
        List<WindowTerm> windows = BinarySlidingWindowApi.slidingWindows(sourceBytes, 0, sourceBytes.length,BinarySlidingWindowApi.BITSET_STEP_SIZE,BinarySlidingWindowApi.BITSET_STEP_SIZE);

        LOG.info("search in "+slices+" "+fieldName);

        Map<FpToken.DedupKey, FpToken.PendingTerm> firstOccurrence = new LinkedHashMap<>();
        for (int i = 0; i < windows.size(); i++) {
            WindowTerm window = windows.get(i);
            byte[] padded = window.getWindowBytes();
            DedupKey probe = new FpToken.DedupKey(padded, padded.length);
            if (firstOccurrence.containsKey(probe)) {
                continue;
            }
            PendingTerm pt = new PendingTerm(padded, padded.length);
            firstOccurrence.put(probe, pt);
        }
        
        ArrayList<BytesRef> list=new ArrayList<BytesRef>();
        for(Entry<FpToken.DedupKey, FpToken.PendingTerm> e:firstOccurrence.entrySet())
        {
        	FpToken.PendingTerm term=e.getValue();
        	list.add(new BytesRef(term.buffer,0,term.length));
        }
        
        this.slices = list.toArray(new BytesRef[list.size()]);
    }

    public String getFieldName() {
        return fieldName;
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        return new BruteForceWeight(this, searcher, scoreMode, boost);
    }

    private class BruteForceWeight extends ConstantScoreWeight {
		private static final long serialVersionUID = 6282792942442401760L;
		private final IndexSearcher searcher;
        private final ScoreMode scoreMode;
        
        public BruteForceWeight(Query query, IndexSearcher searcher, ScoreMode scoreMode, float boost) {
            super(query, boost);
            this.searcher = searcher;
            this.scoreMode = scoreMode;
        }
        
        @Override
        public Scorer scorer(LeafReaderContext context) throws IOException {
            // 获取当前segment的字段Terms
            Terms terms = context.reader().terms(fieldName);
            if (terms == null) {
                return null;
            }
            
           
            
//           if(!(terms instanceof FieldReader))
//           {
//        	   throw new IOException("not FieldReader "+terms.getClass().getName());
//           }
           
            TreeMap<Integer, FpBlockInfo> blocklist=terms.getFpblock_list();
            
            FpSearch search=new FpSearch();
            FixedBitSet bitset=search.search(blocklist, terms, 0, slices);
            
            
            LOG.info("search "+slices[0].utf8ToString() +" "+blocklist.size()+" "+fieldName+" "+bitset.cardinality());
            BitDocIdSet docIdSet = new BitDocIdSet(bitset, 1, context.reader().maxDoc());
            
            
            DocIdSetIterator disi = docIdSet.iterator();
            if (disi == null) {
                return null;
            }
            
            return new ConstantScoreScorer(this, score(), scoreMode, disi);
        }
        
        @Override
        public boolean isCacheable(LeafReaderContext ctx) {
            return false;
        }
    }

    /**
     * 重写toString方法，用于显示查询
     */
    @Override
    public String toString(String field) {
        StringBuilder buffer = new StringBuilder();
        if (!this.fieldName.equals(field)) {
            buffer.append(this.fieldName);
            buffer.append(":");
        }
        buffer.append("BruteForceSubstring(\"");
        buffer.append(Arrays.toString(this.slices));
        buffer.append("\")");
        return buffer.toString();
    }

    /**
     * 重写equals方法，用于查询缓存
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        FpTokenQuery that = (FpTokenQuery) obj;
        return Objects.equals(fieldName, that.fieldName) && 
               Objects.equals(this.slices, that.slices) ;
    }

    /**
     * 重写hashCode方法，用于查询缓存
     */
    @Override
    public int hashCode() {
        return Objects.hash(fieldName, this.slices);
    }

    

}
