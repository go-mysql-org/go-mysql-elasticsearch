go-mysql-elasticsearch is a service to sync your MySQL data into elastissearch automatically. 

It uses mysqldump to fetch data at first, then syncs data incrementally with binlog.

## Use

+ Create your MySQL table.
+ Create the associated Elasticsearch index, document type and mappings if possible, if not, go-mysql-elasticsearch will create index and type automatically using default mapping.
+ Set the MySQL and Elasticsearch mapping in config file.
+ Start `go-mysql-elasticsearch` and enjoy it.

## Notice

+ binlog format must be **row**.
+ binlog row image may be **full** for MySQL. (MariaDB only supports full row image).
+ MySQL table should have a primary key.
+ Can not alter table format at runtime.
+ MySQL table which will be synced must have a PK(primary key), multi columns PK is not allowed. The PK data will be used as id in Elasticsearch.  
+ You should create the associated mappings in Elasticsearch first, I don't think using the default mapping is a wise decision, you must know how to search accurately.