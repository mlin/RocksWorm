RocksWorm
================

Access RocksDB databases over HTTPS for write-once/read-many use cases. Entails:

1. Command-line utility to consolidate a RocksDB database (a directory with several files) into one read-only file, which may then be uploaded to cloud storage like S3 or Azure Blob, making it accessible via HTTP/HTTPS.
2. Plug-in for RocksDB enabling it to read from a RocksWorm file over HTTP/HTTPS directly, so that the data can be retrieved or queried without first downloading the whole database from cloud storage to the local file system.
