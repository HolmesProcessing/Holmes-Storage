# Holmes-Storage: A Storage Planner for Holmes Processing

## Overview


## Dependencies


## Compilation


## Installation


### Supported Databases
Holmes-Storage supports multiple databases and splits them into two categories: Object Stores and Document Stores. This was done to provide users to more easily select their preferred solutions while also allowing the mixing of databases for optimization purposes. In production environments, we strongly recommend using an [S3](https://aws.amazon.com/documentation/s3/) compatible Object Store, such as [RIAK-CS](http://docs.basho.com/riak/cs/latest), and a clustered deployment of [Cassandra](http://cassandra.apache.org/) for the Document Store. 

#### Object Stores
We support three primary object storage databases. We recommend S3 for large deployments.
- Local File System
- S3 compatible
- (Soon) MongoDB Gridfs

#### Document Stores
We support two primary object storage databases. We recommend a Cassandra cluster for large deployments.
- Cassandra
- MongoDB

##### Cassandra 
Holmes-Storage supports a single or cluster installation of Cassandra version 3.5.x and higher. The version requirement is because of the significant improvement in system performance when leveraging the newly introduced [SASIIndex](https://github.com/apache/cassandra/blob/trunk/doc/SASI.md) for secondary indexing. We highly recommend deploying Cassandra as a cluster with a minimum of three Cassandra nodes in production environments.

###### Configuration

###### Indexing
Holmes-Storage uses [SASIIndex](https://github.com/apache/cassandra/blob/trunk/doc/SASI.md) for indexing the Cassandra database. This indexing allows for querying of large datasets with minimal overhead. When leveraging Cassandra, most of the Holmes Processing tools will automatically use SASI indexes for speed improvements. Power users wishing to learn more about how to utilize these indexes should please visit the excellent blog post by [Doan DyuHai](http://www.doanduyhai.com/blog/?p=2058).

However while SASI is powerful, it is not meant to be a replacement for advanced search and aggregation engines like [Solr](http://lucene.apache.org/solr/), [Elasticsearch](https://www.elastic.co/products/elasticsearch), or leveraging [Spark](https://spark.apache.org/). Additionally, Holmes Storage by default does not implement SASI on the table for storing the results of TOTEM Services (results.results). This is because indexing this field can increase storage costs by approximately 40% on standard deployments. If you still wish to leverage SASI on results.results, the following Cassandra command will provide a sane level of indexing.

SASI indexing of TOTEM Service results. WARNING: this will greatly increase storage requirement:
```SQL
CREATE CUSTOM INDEX results_results_idx ON holmes_testing.results (results) 
USING 'org.apache.cassandra.index.sasi.SASIIndex' 
WITH OPTIONS = {
	'analyzed' : 'true', 
	'analyzer_class' : 'org.apache.cassandra.index.sasi.analyzer.StandardAnalyzer', 
	'tokenization_enable_stemming' : 'false', 
	'tokenization_locale' : 'en', 
	'tokenization_normalize_lowercase' : 'true', 
	'tokenization_skip_stop_words' : 'true'
};

```
