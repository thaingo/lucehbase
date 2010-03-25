/**
 * Copyright 2009 T Jake Luciani
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ch.ymc.lucehbase;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.log4j.Logger;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermEnum;
import org.apache.thrift.TException;

/**
 * 
 * @author jake
 * 
 */
public class LucandraTermEnum extends TermEnum {

    private final IndexReader indexReader;
    private final String indexName;

    private int termPosition;
    private Term[] termBuffer;
    private SortedMap<Term, NavigableMap<byte[], byte[]>> termDocFreqBuffer;
    private SortedMap<Term, NavigableMap<byte[], byte[]>> termCache;

    // number of sequential terms to read initially
    private final int maxInitSize = 2;
    private final int maxChunkSize = 64;
    private int actualInitSize = -1;
    private Term initTerm;
    private int chunkCount = 0;

    private final HTable table;
    private final Term finalTerm = new Term("" + new Character((char) 255), "" + new Character((char) 255));

    private static final Logger logger = Logger.getLogger(LucandraTermEnum.class);

    public LucandraTermEnum(IndexReader indexReader) {
        this.indexReader = indexReader;
        this.indexName = indexReader.getIndexName();
        this.table = indexReader.getTable();
        this.termPosition = 0;
    }
    
    public boolean skipTo(Term term) throws IOException {

        if (term == null)
            return false;

        loadTerms(term);

        return termBuffer.length == 0 ? false : true;
    }

    @Override
    public void close() throws IOException {

    }
    
    @Override
    public int docFreq() {
        return termDocFreqBuffer.size();
    }

    @Override
    public boolean next() throws IOException {
        termPosition++;

        boolean hasNext = termPosition < termBuffer.length;

        if (hasNext && termBuffer[termPosition].equals(finalTerm)) {
            termPosition++;
            hasNext = termPosition < termBuffer.length;
        }

        if (!hasNext) {
            // if we've already done init try grabbing more
            if ((chunkCount == 1 && actualInitSize == maxInitSize) || (chunkCount > 1 && actualInitSize == maxChunkSize)) {
                loadTerms(initTerm);
                hasNext = termBuffer.length > 0;
            } else if ((chunkCount == 1 && actualInitSize < maxInitSize) || (chunkCount > 1 && actualInitSize < maxChunkSize)) {
                hasNext = false;
            }
            termPosition = 0;
        }

        return hasNext;
    }

    @Override
    public Term term() {
        return termBuffer[termPosition];
    }

    private void loadTerms(Term skipTo) throws IOException {
        // chose starting term
        String startTerm = indexName + HBaseUtils.delimeter + HBaseUtils.createColumnName(skipTo);
        // this is where we stop;
        String endTerm = indexName + HBaseUtils.delimeter + skipTo.field().substring(0, skipTo.field().length() - 1)
                + new Character((char) (skipTo.field().toCharArray()[skipTo.field().length() - 1] + 1)); // ;

        if ((!skipTo.equals(initTerm) || termPosition == 0) && termCache != null) {
            termDocFreqBuffer = termCache.subMap(skipTo, termCache.lastKey());
        } else {
            termDocFreqBuffer = null;
        }

        if (termDocFreqBuffer != null) {

            termBuffer = termDocFreqBuffer.keySet().toArray(new Term[] {});
            termPosition = 0;

            logger.debug("Found " + startTerm + " in cache");
            return;
        } else if (chunkCount > 1 && actualInitSize < maxChunkSize) {
            termBuffer = new Term[] {};
            termPosition = 0;
            return; // done!
        }

        chunkCount++;

        // The first time we grab just a few keys
        int count = maxInitSize;

        // otherwise we grab all the rest of the keys
        if (initTerm != null) {
            count = maxChunkSize;
            startTerm = indexName + HBaseUtils.delimeter + HBaseUtils.createColumnName(initTerm);
        }

        long start = System.currentTimeMillis();

        termDocFreqBuffer = new TreeMap<Term, NavigableMap<byte[], byte[]>>();

        // Get all columns
        Scan scan = new Scan( startTerm.getBytes(), endTerm.getBytes() );
        scan.addFamily( HBaseUtils.termVecColumnFamily );
        ResultScanner scanner = table.getScanner(scan);
        
        actualInitSize = 0;
        for( Result result : scanner )
        {
          ++actualInitSize;
          NavigableMap<byte[],byte[]> columns = result.getFamilyMap( HBaseUtils.termVecColumnFamily );
          byte[] row = result.getRow();
          String rowString = new String(row);
          
          // term keys look like wikipedia/body/wiki
          String termStr = rowString.substring(rowString.indexOf(HBaseUtils.delimeter) + HBaseUtils.delimeter.length());
          Term term;
          try {
              term = HBaseUtils.parseTerm(termStr.getBytes("UTF-8"));
          } catch (UnsupportedEncodingException e) {
              throw new RuntimeException(e);
          }

          logger.debug(termStr + " has " + columns.size());
          
          //check for tombstone keys
          if(columns.size() > 0)
          {              
            termDocFreqBuffer.put(term, columns);
          }

        }

        if(!termDocFreqBuffer.isEmpty())
        {
          initTerm = termDocFreqBuffer.lastKey();
        }
         
        // term to start with next time
        logger.debug("Found " + actualInitSize + " keys in range:" + startTerm + " to " + endTerm + " in " + (System.currentTimeMillis() - start) + "ms");

        // add a final key (excluded in submap below)
        termDocFreqBuffer.put(finalTerm, null);

        // put in cache
        for (Term termKey : termDocFreqBuffer.keySet()) {

            if (termCache == null) {
                termCache = termDocFreqBuffer;
            } else {
                termCache.putAll(termDocFreqBuffer);
            }

            indexReader.addTermEnumCache(termKey, this);
        }

        // cache the initial term too
        indexReader.addTermEnumCache(skipTo, this);
        termBuffer = termDocFreqBuffer.keySet().toArray(new Term[] {});
        termPosition = 0;
        long end = System.currentTimeMillis();

        logger.debug("loadTerms: " + startTerm + "(" + termBuffer.length + ") took " + (end - start) + "ms");
    }

    public final List<byte[][]> getTermDocFreq() {
        if (termBuffer.length == 0)
        {
            return null;
        }
        
        NavigableMap<byte[], byte[]> termDocs = termDocFreqBuffer.get(termBuffer[termPosition]);

        // create proper docIds.
        // Make sure these ids are sorted in ascending order since lucene
        // requires this.
        int docIds[] = new int[termDocs.size()];
        int idx = 0;
        
        List<byte[][]> sortedTermDocs = new ArrayList<byte[][]>(termDocs.size());
        Map<Integer, byte[][]> termDocMap = new HashMap<Integer, byte[][]>();

        for ( Entry<byte[], byte[]> row : termDocs.entrySet() ) {
            int docId = indexReader.addDocument( row.getKey() );
            byte[][] rowValuePair = { row.getKey(), row.getValue() };
            termDocMap.put(docId, rowValuePair );
            docIds[idx++] = docId;
        }

        // sort
        Arrays.sort(docIds);

        // move
        for (idx = 0; idx < termDocs.size(); idx++) {
            sortedTermDocs.add(termDocMap.get(docIds[idx]));
        }

        return sortedTermDocs;
    }

    /*public Set<Term> getCachedTerms() {
        return termCache.keySet();
    }*/

}
