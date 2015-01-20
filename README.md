go-mysql-elasticsearch is a service to sync your MySQL data into Elastissearch automatically. 

It uses mysqldump to fetch data at first, then syncs data incrementally with binlog.

## Use

+ Create your MySQL table.
+ Create the associated Elasticsearch index, document type and mappings if possible, if not, go-mysql-elasticsearch will create index and type automatically with default mapping.
+ Set MySQL source in config file, see [Source](#Source) below.
+ Custom MySQL and Elasticsearch mapping rule in config file, see [Rule](#Rule) below.
+ Start `go-mysql-elasticsearch` and enjoy it.

## Notice

+ binlog format must be **row**.
+ binlog row image may be **full** for MySQL. (MariaDB only supports full row image).
+ Can not alter table format at runtime.
+ MySQL table which will be synced must have a PK(primary key), multi columns PK is not allowed. The PK data will be used as id in Elasticsearch.  
+ You should create the associated mappings in Elasticsearch first, I don't think using the default mapping is a wise decision, you must know how to search accurately.
+ `mysqldump` must exist in the same node with go-mysql-elasticsearch.
+ Don't change too many rows at same time in one SQL.

## Source

In go-mysql-elasticsearch, you must decide which tables you want to sync into elasticsearch in the source config.

The format in config file is below:

```
[[source]]
schema = "test"
tables = ["t1", t2]

[[source]]
schema = "test_1"
tables = ["t3", t4]
```

`schema` is the database name, and `tables` includes the table need to be synced. 

## Rule

By default, go-mysql-elasticsearch will use MySQL table name as the Elasticserach's index and type name, 
use MySQL table field name as the Elasticserach's field name. 
e.g, if a table named blog, the default index and type in Elasticserach are both named blog, if the table field named title, 
the default field name is also named title.

Rule can let you change this name mapping. Rule format in config file is below:

```
[[rule]]
schema = "test"
table = "t1"
index = "t"
type = "t"

    [rule.field]
    title = "my_title"
```

In the example above, we will use a new index and type both named "t" instead of default "t1", and use "my_title" instead of field name "title".

## Todo

+ Wildcard table source support, like "table_%".
+ Filter table field support, only fields in filter config will be synced.