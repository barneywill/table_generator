# table_generator
Generate create table statement, select statement, or DataFrame structure schama, and get rid of the boring work.

| |Generator|Description|
|---|---|---|
|1|[JsonTableGenerator](#json_table)|Generate create table statement for Hive from JSON|
|2|[JsonDataFrameStructureTypeGenerator](#json_df)|Generate StructureType for DataFrame from JSON|
|3|[MysqlTableGenerator](#mysql_table)|Generate create table statement for Hive from Mysql, mapping a table in Mysql to a table in Hive|
|4|[PostgreSQLTableGenerator](#postgres_table)|Generate create table statement for Hive from Postgresql, mapping a table in Postgresql to a table in Hive|
|5|[Hive2DorisTableGenerator](#hive_doris)|Generate create table statement for Doris from Hive, mapping a table in Hive to a table in Doris|
|6|[generate_hbase_table.sh](#hbase_table)|Generate create table statement for Hive from HBase, mapping a table in HBase to an external table in Hive|
|7|[generate_from_table.sh](#select)|Generate select from statement for a table|


```
#package
sbt package
```

## 1 <a id='json_table' href='https://github.com/barneywill/table_generator/blob/main/src/main/scala/generator/JsonTableGenerator.scala'>JsonTableGenerator</a>
Generate create table statement for Hive from JSON
```
usage : generator.JsonTableGenerator <dbName> <tableName> <json> <isNameQuoted>
```

## 2 <a id='json_df' href='https://github.com/barneywill/table_generator/blob/main/src/main/scala/generator/JSONDataFrameStructureTypeGenerator.scala'>JsonDataFrameStructureTypeGenerator</a>
Generate StructureType for DataFrame from JSON
```
usage : generator.JsonDataFrameStructureTypeGenerator <json>
```

## 3 <a id='mysql_table' href='https://github.com/barneywill/table_generator/blob/main/src/main/scala/generator/MysqlTableGenerator.scala'>MysqlTableGenerator</a>
Generate create table statement for Hive from Mysql, mapping a table in Mysql to a table in Hive
```
usage : generator.MysqlTableGenerator <mysqlUrl> <mysqlUser> <mysqlPassword> <hiveDb> <mysqlDb> <mysqlTable> <timeColumn>
```

## 4 <a id='postgres_table' href='https://github.com/barneywill/table_generator/blob/main/src/main/scala/generator/PostgreSQLTableGenerator.scala'>PostgreSQLTableGenerator</a>
Generate create table statement for Hive from Postgresql, mapping a table in Postgresql to a table in Hive
```
usage : generator.PostgreSQLTableGenerator <postgreUrl> <postgreUser> <postgrePassword> <hiveDb> <postgreDb> <postgreTable> <timeColumn>
```

## 5 <a id='hive_doris' href='https://github.com/barneywill/table_generator/blob/main/src/main/scala/generator/Hive2DorisTableGenerator.scala'>Hive2DorisTableGenerator</a>
Generate create table statement for Doris from Hive, mapping a table in Hive to a table in Doris
```
usage : generator.Hive2DorisTableGenerator <hiveUrl> <hiveUser> <hiveDb> <hiveDb> <hiveTable> <dorisDb> <dorisTable>
```

## 6 <a id='hbase_table' href='https://github.com/barneywill/table_generator/blob/main/sh/generate_hbase_table.sh'>generate_hbase_table.sh</a>
Generate create table statement for Hive from HBase, mapping a table in HBase to an external table in Hive
```
usage : sh generate_hbase_table.sh <ns.table>
```

## 7 <a id='select' href='https://github.com/barneywill/table_generator/blob/main/sh/generate_from_table.sh'>generate_from_table.sh</a>
Generate select from statement for a table
```
usage : sh generate_from_table.sh <db.table> [alias] [line_breaker]
```
