This is a port of lucandra to HBase.

Lucandra stores a lucene index on cassandra. The concept is described here:

http://blog.sematext.com/2010/02/09/lucandra-a-cassandra-based-lucene-backend

As the author of lucandra writes: "I’m sure something similar could be built 
on hbase."

So here it is.

The advantages this project should bring:

 * manages really big indices
 * near instant searchability
 * redundancy and reliability provided by HBase
 * have your index data directly available for Hadoop MapReduce

This is only a first prototype which has not been tested on anything real yet. 
But if you're interested, please join me to get it production ready!
