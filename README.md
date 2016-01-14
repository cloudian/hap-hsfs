# HyperStore Analytics Platform
HyperStore Analytics Platform(HAP) is a project that allows Cloudian HyperStore users to make analysis on their existing Cloudian HyperStore clusters. Since the fundamental block is HyperStoreFileSystem, which is our own Hadoop FileSystem, users can use any Hadoop compatible libraries.

We recommend to run Spark rather than Hadoop to avoid resource contentions.

If you are new to Hadoop/Spark, please go ahead to [our wiki pages](https://github.com/cloudian/hap/wiki) to get some basics.

## Main use cases
1. Deep Learning with [DL4J](https://github.com/deeplearning4j/deeplearning4j)
1. Machine Learning on [Spark](https://github.com/apache/spark)
1. ETL and SQL on [Spark](https://github.com/apache/spark)
1. ETL with [Pig](https://github.com/apache/pig) on [Hadoop](https://github.com/apache/hadoop)
1. SQL with [Hive](https://github.com/apache/hive) on [Hadoop](https://github.com/apache/hadoop)

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

## Installation

Please follow [Installation](https://github.com/cloudian/hap/wiki/Installation) on our wiki.

## Usage
You can use hsfs protocol to access your files stored in Cloudian HyperStore as follows. If you have used s3n or s3a, then simply replace s3n/s3a with hsfs.

```
hsfs://BUCKET_NAME/OBJECT_KEY
```

Please note that hsfs does not support write operations because it is designed to provide data locality, which is not available in writes. For writes, please use s3a instead(no additional config is required).

## Questions
[![join the chat at https://gitter.im/cloudian](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/cloudian)  
If you have any problems, please file an issue.
