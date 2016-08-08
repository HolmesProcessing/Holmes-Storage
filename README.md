# Holmes-Storage: A Storage Planner for Holmes Processing [![Build Status](https://travis-ci.org/HolmesProcessing/Holmes-Storage.svg?branch=master)](https://travis-ci.org/HolmesProcessing/Holmes-Storage)

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

###### Installation


###### Configuration
New Cassandra clusters will need to be configured before Cassandra is started for the first time. We have highlighted a few of the configuration options that are critical or will improve performance. For additional options, please see the [Cassandra instillation guide](http://cassandra.apache.org/doc/latest/getting_started/configuring.html#main-runtime-properties). 

To edit these values, please open the Cassandra configuration file in your favorite editor. The Cassandra configuration file is typically located in `/etc/cassandra/cassandra.yaml`. 

The Cassandra "cluster_name" must be set and the same on all nodes. The name you select does not much matter but again it should be identical on all nodes.
`cluster_name: 'Holmes Processing'`

Cassandra 3.x has an improved token allocation algorithm. As such, 256 is not necessary and should be decreased to 64 or 128 tokens. 
`num_tokens: 128`

You should populate the "seeds" value with the IP addresses for at least two additional Cassandra nodes.
`seeds: <ip node1>,<ip node2>`

The "listen_address" should be set to the external IP address for the current Cassandra node.
`listen_address: <external ip address>`

###### Best Practices
On a new cluster, Holmes-Storage will setup the database in an optimal way for the average user. However, we recommend Cassandra users to please read the [Cassandra's Operations website](http://wiki.apache.org/cassandra/Operations) for more information Cassandra best practices.  Additionally, it is critical that the Cassandra cluster be regularly repaired using `nodetool repair` command. We recommend that this is executed on every node, one at a time, at least once a weekly.

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
	'tokenization_skip_stop_words' : 'true',
	'max_compaction_flush_memory_in_mb': '512'
};

```
