#!/bin/sh

echo 'usage : sh generate_hbase_table.sh <ns.table>'

param=$1
params=(${param//:/ })
ns=${params[0]}
table=${params[1]}

echo "$param $ns $table"

hbase_column="`echo "scan '${param}', {LIMIT=>1}"|hbase shell|awk '{print $2}'|grep -E '^column'`"
keys=`echo "$hbase_column"|awk -F '=' '{print $2}'|sed ':a;N;$!ba;s/\n//g'|sed 's/.$//'`
columns=`echo "$hbase_column"|awk -F ':' '{print $2}'|sed 's/,/\` string,/g'`
select_columns=`echo "$hbase_column"|awk -F ':' '{print $2}'`

echo $keys
echo $columns

echo "---------------------"
echo "CREATE EXTERNAL TABLE hbase.src_hbase_${table,,}(
\`key\` string,"
echo '`'$columns|sed 's/.$//'|sed 's/, /,\n\`/g'
echo ")
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES
(\"hbase.columns.mapping\" = \":key,$keys\")
TBLPROPERTIES(\"hbase.table.name\" = \"${param}\");"

echo "---------------------"
echo "CREATE EXTERNAL TABLE hbase.ods_hbase_${table,,}(
\`key\` string,"
echo '`'$columns|sed 's/.$//'|sed 's/, /,\n\`/g'
echo ")
partitioned by (dt string)
stored as orc
TBLPROPERTIES (\"orc.compression\"=\"SNAPPY\");"

echo "---------------------"
echo "insert overwrite table hbase.ods_hbase_${table,,} partition(dt)
select \`key\`, "
echo '`'${select_columns}|sed 's/, /\`, \`/g'|sed 's/,$/\`,/g'
echo "from_unixtime(cast(substr(\`timestamp\`, 1, 10) as bigint), 'yyyyMMdd') dt
from hbase.src_hbase_${table,,};"
