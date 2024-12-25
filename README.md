# table_generator
Generate create table statement, select statement, or DataFrame structure schama, and get rid of the boring work.

```
#package
sbt package
```

## 1 JsonTableGenerator
Generate create table statement for Hive from JSON
```
usage : generator.JsonTableGenerator <dbName> <tableName> <json> <isNameQuoted>
```

## 2 JsonDataFrameStructureTypeGenerator
Generate StructureType for DataFrame from JSON
```
usage : generator.JsonDataFrameStructureTypeGenerator <json>
```

## 3 MysqlTableGenerator
Generate create table statement for Hive from Mysql, mapping a table in Mysql to a table in Hive
```
usage : generator.MysqlTableGenerator <mysqlUrl> <mysqlUser> <mysqlPassword> <hiveDb> <mysqlDb> <mysqlTable> <timeColumn>
```

## 4 PostgreSQLTableGenerator
Generate create table statement for Hive from Postgresql, mapping a table in Postgresql to a table in Hive
```
usage : generator.PostgreSQLTableGenerator <postgreUrl> <postgreUser> <postgrePassword> <hiveDb> <postgreDb> <postgreTable> <timeColumn>
```

## 5 Hive2DorisTableGenerator
Generate create table statement for Doris from Hive, mapping a table in Hive to a table in Doris
```
usage : generator.Hive2DorisTableGenerator <hiveUrl> <hiveUser> <hiveDb> <hiveDb> <hiveTable> <dorisDb> <dorisTable>
```

## 6 generate_hbase_table.sh
Generate create table statement for Hive from HBase, mapping a table in HBase to an external table in Hive
```
usage : sh generate_hbase_table.sh <ns.table>
```

## 7 generate_from_table.sh
Generate
```
usage : sh generate_from_table.sh <db.table> [alias] [line_breaker]
```
