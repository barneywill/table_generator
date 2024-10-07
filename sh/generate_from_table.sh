#!/bin/sh

echo "usage : sh generate_from_table.sh <\$db.\$table> [\$alias] [\$line_breaker]"

tb_name=$1
alias=$2
line_breaker=$3

prefix=""
if [ -n "$alias" ]; then
	prefix="${alias}."
fi
hive -e "desc ${tb_name}" > /tmp/${tb_name} && end=`cat -n /tmp/${tb_name}|grep 'Partition Information'|awk '{print $1}'`
columns=`sed -n '1,'${end}'p' /tmp/${tb_name}|grep -E '^[a-z]'|awk '{print $1}'|sed ':a;N;$!ba;s/\n/,'${line_breaker}' '${prefix}'/g'`
echo "generated sql: "
echo "------------------------"
echo "insert overwrite table * partition(dt)"
echo "select ${prefix}$columns"
echo "from ${tb_name} ${alias}"
echo "where ${prefix}dt = '\${bdp.system.bizdate}';"
echo "------------------------"
