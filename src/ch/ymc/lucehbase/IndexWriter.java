package ch.ymc.lucehbase;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.thrift.TException;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;

public class IndexWriter {

    private final String indexName;
    private final HTable table;
    private final String idField;

    /*private final ColumnPath docAllColumnPath;
    private final ColumnPath metaColumnPath;*/ 

    private static final Logger logger = Logger.getLogger(IndexWriter.class);

    public IndexWriter(String indexName, String idField, HTable table) {

        this.indexName = indexName;
        this.table = table;
        this.idField = idField;

        /*docAllColumnPath = new ColumnPath(HBaseUtils.docColumnFamily);
        
        metaColumnPath = new ColumnPath(HBaseUtils.docColumnFamily);
        metaColumnPath.setColumn(HBaseUtils.documentMetaField.getBytes());*/
        
    }

    @SuppressWarnings("unchecked")
    public void addDocument(Document doc, Analyzer analyzer) throws CorruptIndexException, IOException, Exception {

        Token token = new Token();
        List<String> allIndexedTerms = new ArrayList<String>();
        List<Put> puts = new ArrayList<Put>();
            
        //check for special field name
        String docId = doc.get(idField);
 
        if(docId == null)
        {
            throw new Exception("no document id");          
        }  
         
        int position = 0;

        for (Fieldable field : doc.getFields()) {

            // Indexed field
            if (field.isIndexed() && field.isTokenized()) {

                TokenStream tokens = field.tokenStreamValue();

                if (tokens == null) {
                    tokens = analyzer.tokenStream(field.name(), new StringReader(field.stringValue()));
                }
                TermAttribute termAtt = tokens.addAttribute(TermAttribute.class);
                tokens.reset();

                // collect term frequencies per doc
                Map<String, List<Integer>> termPositions = new HashMap<String, List<Integer>>();
                if (position > 0) {
                    position += analyzer.getPositionIncrementGap(field.name());
                }

                // Build the termPositions vector for all terms
                while ( tokens.incrementToken() ) {
                    String term = HBaseUtils.createColumnName(field.name(), termAtt.term());
                    allIndexedTerms.add(term);
                    
                    List<Integer> pvec = termPositions.get(term);

                    if (pvec == null) {
                        pvec = new ArrayList<Integer>();
                        termPositions.put(term, pvec);
                    }

                    position += (token.getPositionIncrement() - 1);
                    pvec.add(++position);
                }
                tokens.end();
                tokens.close();

                for (Map.Entry<String, List<Integer>> term : termPositions.entrySet()) {

                    // Terms are stored within a unique key combination
                    // This is required since cassandra loads all column
                    // families for a key into memory
                    String key = indexName + HBaseUtils.delimeter + term.getKey();
                    
                    Put p = new Put( key.getBytes() );
                    p.add( HBaseUtils.termVecColumnFamily, docId.getBytes(), HBaseUtils.intVectorToByteArray(term.getValue()) );
                    puts.add(p);
                }
            }

            // Untokenized fields go in without a termPosition
            else if (field.isIndexed() && !field.isTokenized()) {
                String term = HBaseUtils.createColumnName(field.name(), field.stringValue());
                allIndexedTerms.add(term);
                
                String key = indexName + HBaseUtils.delimeter + term;

                Put p = new Put( key.getBytes() );
                p.add( HBaseUtils.termVecColumnFamily, docId.getBytes(), HBaseUtils.intVectorToByteArray(Arrays.asList(new Integer[] { 0 })) );
                puts.add(p);
            }

            // Stores each field as a column under this doc key
            if (field.isStored()) {              
                byte[] _value = field.isBinary() ? field.getBinaryValue() : field.stringValue().getBytes();         
                
                //first byte flags if binary or not
                byte[] value = new byte[_value.length+1];
                System.arraycopy(_value, 0, value, 0, _value.length);
                
                value[value.length-1] = (byte) (field.isBinary() ? Byte.MAX_VALUE : Byte.MIN_VALUE);
                
                String key = indexName+HBaseUtils.delimeter+docId;
                
                Put p = new Put( key.getBytes() );
                p.add( HBaseUtils.docColumnFamily, field.name().getBytes(), value );
                puts.add(p);
            }
        }
        
        //Finally, Store meta-data so we can delete this document
        String docKey = indexName+HBaseUtils.delimeter+docId;
        
        Put p = new Put( docKey.getBytes() );
        p.add( HBaseUtils.docColumnFamily, HBaseUtils.documentMetaField, HBaseUtils.toBytes(allIndexedTerms) );
        puts.add(p); 
        //commit!
        table.put(puts);   
    }

    public void deleteDocuments(Query query) throws CorruptIndexException, IOException {
        
 /*       IndexReader   reader   = new IndexReader(indexName,table);
        IndexSearcher searcher = new IndexSearcher(reader);
       
        TopDocs results = searcher.search(query,1000);
    
        for(int i=0; i<results.totalHits; i++){
            ScoreDoc doc = results.scoreDocs[i];
            
            String docId = reader.getDocumentId(doc.doc); 
        }
   */     
    }
    
    @SuppressWarnings("unchecked")
    public void deleteDocuments(Term term) throws CorruptIndexException, IOException {
      /*  try {
                       
            ColumnParent cp = new ColumnParent(HBaseUtils.termVecColumnFamily);
            List<ColumnOrSuperColumn> docs = table.get_slice(HBaseUtils.keySpace, indexName+HBaseUtils.delimeter+HBaseUtils.createColumnName(term), cp, new SlicePredicate().setSlice_range(new SliceRange(new byte[]{}, new byte[]{},true,Integer.MAX_VALUE)), ConsistencyLevel.ONE);
                
            //delete by documentId
            for(ColumnOrSuperColumn docInfo : docs){
                deleteLucandraDocument(docInfo.column.name);
            }
                              
        } catch (InvalidRequestException e) {
            throw new RuntimeException(e);
        } catch (UnavailableException e) {
            throw new RuntimeException(e);
        } catch (TException e) {
            throw new RuntimeException(e);
        } catch (TimedOutException e) {
            throw new RuntimeException(e);
        } catch (NotFoundException e) {
            throw new RuntimeException(e);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }*/  
    }
    /*
    private void deleteLucandraDocument(byte[] docId) throws InvalidRequestException, NotFoundException, UnavailableException, TimedOutException, TException, IOException, ClassNotFoundException{
        Map<String,Map<String,List<Mutation>>> mutationMap = new HashMap<String,Map<String,List<Mutation>>>();

        ColumnOrSuperColumn column = table.get(HBaseUtils.keySpace, indexName+HBaseUtils.delimeter+new String(docId), metaColumnPath, ConsistencyLevel.ONE);
        
        List<String> terms = (List<String>) HBaseUtils.fromBytes(column.column.value);
    
        for(String termStr : terms){
            
            String key = indexName+HBaseUtils.delimeter+termStr;
            
            HBaseUtils.addToMutationMap(mutationMap, HBaseUtils.termVecColumnFamily, docId, key, null);                                        
        }
    
        HBaseUtils.robustBatchInsert(table, mutationMap);
        
        //finally delete ourselves
        String selfKey = indexName+HBaseUtils.delimeter+new String(docId);
        
        
        //FIXME: once cassandra batch mutation supports slice predicates in deletions
        client.remove(HBaseUtils.keySpace, selfKey, docAllColumnPath, System.currentTimeMillis(), ConsistencyLevel.ONE);

        
    }
    */
    
    public void updateDocument(Term updateTerm, Document doc, Analyzer analyzer) throws CorruptIndexException, IOException, Exception{       
        deleteDocuments(updateTerm);
        addDocument(doc, analyzer);    
    }

    public int docCount() {
/*
        try{
            String start = indexName + HBaseUtils.delimeter;
            String finish = indexName + HBaseUtils.delimeter+HBaseUtils.delimeter;

            ColumnParent columnParent = new ColumnParent(HBaseUtils.docColumnFamily);
            SlicePredicate slicePredicate = new SlicePredicate();

            // Get all columns
            SliceRange sliceRange = new SliceRange(new byte[] {}, new byte[] {}, true, Integer.MAX_VALUE);
            slicePredicate.setSlice_range(sliceRange);

            List<KeySlice> columns  = table.get_range_slice(HBaseUtils.keySpace, columnParent, slicePredicate, start, finish, 5000, ConsistencyLevel.ONE);
        
            return columns.size();
            
        }catch(Exception e){
            throw new RuntimeException(e);
        }
  */
      return 42;
    }

}
