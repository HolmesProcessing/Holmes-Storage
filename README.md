# Holmes-Storage: A Storage Planner for Holmes Processing [![Build Status](https://travis-ci.org/HolmesProcessing/Holmes-Storage.svg?branch=master)](https://travis-ci.org/HolmesProcessing/Holmes-Storage)

## Overview
Holmes-Storage is responsible for managing the interaction of Holmes Processing with the database backends. At its core, Holmes-Storage organizes the information contained in Holmes Processing and provides a RESTful and AMQP interface for accessing the data. Additionally, Holmes-Storage provides an abstraction layer between the specific database types. This allows a Holmes Processing system to change database types and combine different databases together for optimization.

When running, Holmes-Storage will:
- Automatically fetches the analysis reuslts from Holmes-Totem and Holmes-Totem-Dynamic over AMQP for storage
- Support the submission on objects via a RESTful API
- Support the retrieval of results, raw objects, object meta-data, and object submission information via a RESTful API

We have designed Holmes-Storage to operate as a reference implementation. In doing so, we have optimized the system to seamlessly plug into other parts of Holmes Processing and optimized the storage of information for generic machine learning algorithms and queries through a web frontend. Furthermore, we have seperated the storage of binary blobs and textural data in order to better handle how data is stored and compressed. As such, Holmes-Storage will store file based objects (i.e. ELF, PE32, PDF, etc) in a S3 compatible backend and the meta information of the objects and results from analysis in Cassandra. With respect to non-file type objects, these are purely stored in Cassandra. In our internal production systems, this scheme has supported 10s of million of objects along with the results from associated Totem and Totem-Dynamic Services with minimal effort. However, as with any enterprise system, customization will be required to improve the performance for custom use cases. Anyway, we hope this serves you well or at least helps guide you in developing custom Holmes-Storage Planners.

## Dependencies
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

##### RiakCS
Follow [this](http://docs.basho.com/riak/cs/2.1.1/tutorials/fast-track/local-testing-environment/) tutorial for installation of RiakCS.
###### Configuration
After successful installation, the user’s access key and secret key are returned in the `key_id` and `key_secret` fields respectively. Use these keys to update **key** and **secret** your config file _( storage.conf.example )_

Holmes-Storage uses Amazon S3 signature version 4 for authentication. To enable authV4 on riak-cs, add `{auth_v4_enabled, true}` to advanced.config file ( should be in `/riak-cs/etc/`)

## Installation
Copy the default configuration file located in config/storage.conf.example and change it according to your needs.
```
$ cp storage.conf.example storage.conf
```
Update the `storage.conf` file in config folder and adjust the ports if need accordingly.
To build the Holmes-Storage, just run
```
$ go build
```

Setup the database by calling
```
$ ./Holmes-Storage --config <path_to_config> --setup
```
This will create the configured keyspace if it does not exist yet. For cassandra, the default keyspace will use the following replication options:
```
 {'class': 'NetworkTopologyStrategy', 'dc': '2'}
```
If you want to change this, you can do so after the setup by connecting with cqlsh and changing it manually. For more information about that we refer to the official documentation of cassandra Cassandra Replication Altering Keyspace You can also create the keyspace with different replication options before executing the setup and the setup won't overwrite that. The setup will also create the necessary tables and indices.

Setup the object storer by calling:
```
$ ./Holmes-Storage --config <path_to_config> --objSetup
```

Execute storage by calling:
```
$ ./Holmes-Storage --config <path_to_config>
```

## Best Practices
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
