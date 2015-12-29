# HyperStore Analytics Platform
HyperStore Analytics Platform(HAP) is a project that allows Cloudian HyperStore users to make analysis on their existing Cloudian HyperStore clusters. Since the fundamental block is HyperStoreFileSystem, which is our own Hadoop FileSystem, users can use any Hadoop compatible libraries.

We recommend to run Spark rather than Hadoop to avoid resource contentions.

## Main use cases
1. Deep Learning with [DL4J](https://github.com/deeplearning4j/deeplearning4j)
2. Machine Learning on [Spark](https://github.com/apache/spark)
3. ETL with [Pig](https://github.com/apache/pig) on [Hadoop](https://github.com/apache/hadoop)
4. SQL with [Hive](https://github.com/apache/hive) on [Hadoop](https://github.com/apache/hadoop)
5. ETL and SQL on [Spark](https://github.com/apache/spark)

## Versions
This platform was tested against the following versions.

|name               |version|
|-------------------|-------|
|Cloudian HyperStore| 5.2.1 |
|     Spark         | 1.5.2 |
|     DL4J          | 0.4   |
|     Hadoop        | 2.7.1 |
|     Pig           | 0.15.0|
|     Hive          | 1.2.1 |

## Data Locality

The main purpose of hsfs is to minimize network transfers by delivering an analytical job to a node that actually stores its data.

To make this possible, Hadoop filesystem allows its subclass to return a block size for a particular path(file),
and a list of nodes that stores a particular chunk.

With those information, an analytical task is chunked into multiple analytical jobs,
each of which is delivered to a node, then finds its data locally.

In order to make this work in Cloudian HyperStore, there are some rules you need to care.

|method  |  size  |  data locality |
|--------|--------|----------------|
|  PUT   | <=MOCS |      YES       |
|  PUT   |  >MOCS |      YES       |
|  MPU   |  <MOCS |       NO       |
|  MPU   |  =MOCS |      YES       |
|  MPU   |  >MOCS |       NO       |
* MOCS: Max Object Chunk Size
* MPU: Multi Part Upload

As above, all the PUT requests will be correctly handled because they are chunked by MOCS.
While MPU doesn't work unless its part size is equal to MOCS.

Also note that Data Locality is not avaialble when a chunk can not be readable in the following cases.

1. Erasure Coding
2. Server Side Encryption

## Installation

Please follow INSTALL_SPARK for details.

## Usage
You can use hsfs protocol to access your files stored in Cloudian HyperStore as follows. If you have used s3n or s3a, then simply replace s3n/s3a with hsfs.

```
hsfs://BUCKET_NAME/OBJECT_KEY
```

Please note that hsfs does not support write operations because it is designed to provide data locality, which is not available in writes. For writes, please use s3a instead(no additional config is required).

## Questions
[![join the chat at https://gitter.im/cloudian](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/cloudian)  
If you have any problems, please file an issue.