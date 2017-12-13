RocksWorm
================

Access RocksDB databases over HTTPS for write-once/read-many use cases. Entails:

1. Command-line utility to consolidate a RocksDB database (a directory with several files) into a RocksWorm file, which may then be uploaded to cloud storage like S3, making it readable via HTTP/HTTPS.
2. Plug-in for RocksDB enabling it to read from a RocksWorm file over HTTP/HTTPS directly, so that your program can query the database on cloud storage without having to first download it to the local file system.
