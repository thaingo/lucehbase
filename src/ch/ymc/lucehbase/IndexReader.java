package ch.ymc.lucehbase;


import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.log4j.Logger;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldSelector;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermDocs;
import org.apache.lucene.index.TermEnum;
import org.apache.lucene.index.TermFreqVector;
import org.apache.lucene.index.TermPositions;
import org.apache.lucene.index.TermVectorMapper;
import org.apache.lucene.search.DefaultSimilarity;
import org.apache.lucene.store.Directory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;

public class IndexReader extends org.apache.lucene.index.IndexReader {

    private final static int numDocs = 1000000;
    private final static byte[] norms = new byte[numDocs];
    static{
        Arrays.fill(norms, DefaultSimilarity.encodeNorm(1.0f));
    }
    
    private final String indexName;
    private final HTable table;
    private final Map<String,Integer> docIdToDocIndex;
    private final Map<Integer,String> docIndexToDocId;
    private final AtomicInteger docCounter;
   
    private final Map<Term, LucandraTermEnum> termEnumCache;
   

    private static final Logger logger = Logger.getLogger(IndexReader.class);

    public IndexReader(String name, HTable table) {
        super();
        this.indexName = name;
        this.table = table;
        

        docCounter         = new AtomicInteger(0);
        docIdToDocIndex    = new HashMap<String,Integer>();
        docIndexToDocId    = new HashMap<Integer,String>();
        
        termEnumCache = new HashMap<Term, LucandraTermEnum>();
    }
    
  
    @Override
    public IndexReader reopen(){
        
        docCounter.set(0);
        docIdToDocIndex.clear();
        docIndexToDocId.clear();
        termEnumCache.clear();
        
        return this;
    }
    
    @Override
    protected void doClose() throws IOException {
        
    } 
    
    protected void doCommit() throws IOException {
      
    }

    @Override
    protected void doDelete(int arg0) throws CorruptIndexException, IOException {
       // throw new UnsupportedOperationException();
    }

    @Override
    protected void doSetNorm(int arg0, String arg1, byte arg2) throws CorruptIndexException, IOException {
       // throw new UnsupportedOperationException();
    }

    @Override
    protected void doUndeleteAll() throws CorruptIndexException, IOException {
        //throw new UnsupportedOperationException();
    }

    @Override
    public int docFreq(Term term) throws IOException {

        LucandraTermEnum termEnum = termEnumCache.get(term);
        if (termEnum == null) {

            long start = System.currentTimeMillis();

            termEnum = new LucandraTermEnum(this);
            termEnum.skipTo(term);

            long end = System.currentTimeMillis();

            logger.info("docFreq() took: " + (end - start) + "ms");

            termEnumCache.put(term, termEnum);   
        }
        
        return termEnum.docFreq();
    }

    @Override
    public Document document(int docNum, FieldSelector selector) throws CorruptIndexException, IOException {

        String key = indexName +HBaseUtils.delimeter+docIndexToDocId.get(docNum);
        Get get = new Get( key.getBytes() );
        get.addFamily( HBaseUtils.docColumnFamily );
        
        long start = System.currentTimeMillis();

        try {
            Result result = table.get(get);
            NavigableMap<byte[],byte[]> cols = result.getFamilyMap(  HBaseUtils.docColumnFamily );

            Document doc = new Document();
            for ( byte[] col : cols.keySet() ) {
                
                Field  field = null;
                String fieldName = new String(col);          
                byte[] value = cols.get(col);      
                field = new Field(fieldName, new String(value), Store.YES, Index.ANALYZED);
                doc.add(field);
            }

            long end = System.currentTimeMillis();

            logger.debug("Document read took: " + (end - start) + "ms");

            return doc;
        } catch (Exception e) {
            throw new IOException(e.getLocalizedMessage());
        }
    }

    @Override
    public Collection getFieldNames(FieldOption fieldOption) {     
       return Arrays.asList(new String[]{});
    }

    @Override
    public TermFreqVector getTermFreqVector(int arg0, String arg1) throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void getTermFreqVector(int arg0, TermVectorMapper arg1) throws IOException {
        // TODO Auto-generated method stub

    }

    @Override
    public void getTermFreqVector(int arg0, String arg1, TermVectorMapper arg2) throws IOException {
        // TODO Auto-generated method stub

    }

    @Override
    public TermFreqVector[] getTermFreqVectors(int arg0) throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean hasDeletions() {

        return false;
    }

    @Override
    public boolean isDeleted(int arg0) {

        return false;
    }

    @Override
    public int maxDoc() {
        //if (numDocs == null)
        //    numDocs();

        return numDocs + 1;
    }

    @Override
    public byte[] norms(String term) throws IOException {
        return norms;     
    }

    @Override
    public void norms(String arg0, byte[] arg1, int arg2) throws IOException {
        // TODO Auto-generated method stub

    }

    @Override
    public int numDocs() {

        return numDocs;
    }

    @Override
    public TermDocs termDocs() throws IOException {
        return new LucandraTermDocs(this);
    }

    @Override
    public TermPositions termPositions() throws IOException {
        return new LucandraTermDocs(this);
    }

    @Override
    public TermEnum terms() throws IOException {
        return new LucandraTermEnum(this);
    }

    @Override
    public TermEnum terms(Term term) throws IOException {
       
        LucandraTermEnum termEnum = termEnumCache.get(term);
        
        if(termEnum == null){
        
            termEnum = new LucandraTermEnum(this);
            if( !termEnum.skipTo(term) )           
                termEnum = null;
            
        }
        
        return termEnum;
    }

    public int addDocument(byte[] docId) {       
        String id;
        try {
            id = new String(docId,"UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new IllegalStateException("Cant make docId a string");
        }
                
        Integer idx = docIdToDocIndex.get(id);
        
        if(idx == null){
            idx = docCounter.incrementAndGet();

            if(idx > numDocs)
                throw new IllegalStateException("numDocs reached");
            
            docIdToDocIndex.put(id, idx);
            docIndexToDocId.put(idx, id);         
        }
   
        return idx;
    }

    public String getDocumentId(int docNum) {
        return docIndexToDocId.get(docNum);
    }

    public String getIndexName() {
        return indexName;
    }

    public HTable getTable() {
        return table;
    }
    
    public LucandraTermEnum checkTermCache(Term term){
        return termEnumCache.get(term);
    }
    
    public void addTermEnumCache(Term term, LucandraTermEnum termEnum){
        termEnumCache.put(term, termEnum);
    }
    
    @Override
    public Directory directory() {
            return null;
    }

    @Override
    public long getVersion() {
            return 1;
    }

    @Override
    protected void doCommit(Map<String, String> commitUserData)
        throws IOException {
      // TODO Auto-generated method stub
      
    }

}
