# HyperStore Analytics Platform
HyperStore Analytics Platform(HAP) is a project that allows Cloudian HyperStore users to make analysis on their existing Cloudian HyperStore clusters. Since the fundamental block is HyperStoreFileSystem, which is our own Hadoop FileSystem, users can use any Hadoop compatible libraries.

## Main use cases
1. ETL with ![Pig](https://github.com/apache/pig) on ![Hadoop](https://github.com/apache/hadoop)
2. SQL with ![Hive](https://github.com/apache/hive) on ![Hadoop](https://github.com/apache/hadoop)
3. ETL and SQL on ![Spark](https://github.com/apache/spark)
4. Machine Learning on ![Spark](https://github.com/apache/spark)
5. Deep Learning with ![DL4J](https://github.com/deeplearning4j/deeplearning4j)

## Versions
This platform was tested against the following versions.

|name               |version|
|-------------------|-------|
|Cloudian HyperStore| 5.2.1 |
|     Hadoop        | 2.7.1 |
|     Pig           |0.15.0 |
|     Hive          | 1.2.1 |
|     Spark         | 1.5.2 |


## Installation

Please follow INSTALLATION_HADOOP or INSTALLATION_SPARK for details.

## Usage
You can use hsfs protocol to access your files stored in Cloudian HyperStore as follows. If you have used s3n or s3a, then simply replace s3n/s3a with hsfs.

```
hsfs://BUCKET_NAME/OBJECT_KEY
```

Please note that hsfs does not support write operations because it is designed to provide data locality, which is not available in writes. For writes, please use s3a instead(no additional config is required).

## Questions
[![join the chat at https://gitter.im/cloudian](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/cloudian)  
If you have any problems, please file an issue.