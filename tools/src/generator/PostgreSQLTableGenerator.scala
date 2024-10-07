package generator

import com.ninebot.bigdata.etl.util.PinyinUtils

import java.sql.{Connection, DriverManager, ResultSet, Statement}

class PostgreSQLTableGenerator(postgreUrl : String, postgreUser : String, postgrePassword : String) {
  Class.forName("org.postgresql.Driver")
  def generateTable(hiveDb : String, postgreDb : String, postgreTable : String, timeColumn : String = null, isDorisOutput : Boolean = false) {
    var conn : Connection = null
    var stat : Statement = null
    var rs : ResultSet = null
    try {
      conn = DriverManager.getConnection(postgreUrl, postgreUser, postgrePassword)
      stat = conn.createStatement()
      rs = stat.executeQuery(String.format("SELECT a.attname as name, format_type(a.atttypid,a.atttypmod) as type, col_description(a.attrelid,a.attnum) as comment FROM pg_class as c, pg_attribute as a, pg_namespace b where b.nspname = '%s' and b.oid = c.relnamespace and c.relname = '%s' and a.attrelid = c.oid and a.attnum > 0", postgreDb, postgreTable))

      val db = postgreUrl.substring(postgreUrl.lastIndexOf("/") + 1)
      val hiveTable = String.format("ods_%s_%s_%s", db, postgreDb, postgreTable)
      var createHiveTableSql = String.format("create external table %s.%s \n(\n", hiveDb, hiveTable)
      var createDorisTableSql = String.format("create table if not exists %s.%s \n(\n", hiveDb, hiveTable)
      var columns = ""
      while (rs.next()) {
        createHiveTableSql += String.format("%s %s comment '%s',\n", PinyinUtils.getPinYin(rs.getString(1)), convertHiveType(rs.getString(2)), rs.getString(3))
        createDorisTableSql += String.format("%s %s comment '%s',\n", rs.getString(1), convertDorisType(rs.getString(2)), rs.getString(3))
        columns += "`" + rs.getString(1) + "`" + ","
      }
      createHiveTableSql = createHiveTableSql.substring(0, createHiveTableSql.length - 2)
      createHiveTableSql += String.format("\n)\npartitioned by (dt string)\nstored as orc\nlocation 'hdfs://nameservice1/user/hive/warehouse/%s.db/%s'\ntblproperties(\"orc.compression\"=\"SNAPPY\");", hiveDb, hiveTable)
      println(createHiveTableSql)
      println
      if (isDorisOutput) {
        createDorisTableSql = createDorisTableSql.substring(0, createDorisTableSql.length - 2)
        createDorisTableSql += "\n)\nENGINE=OLAP\nDUPLICATE KEY( )\nCOMMENT \"\"\nPARTITION BY RANGE( )\nDISTRIBUTED BY HASH( ) BUCKETS 4\nPROPERTIES (\n\"replication_num\" = \"2\",\n\"colocate_with\" = \" \",\n\"in_memory\" = \"false\",\n\"storage_format\" = \"DEFAULT\"\n);"
        println(createDorisTableSql)
      }

      columns = columns.substring(0, columns.length - 1)

      println
      println(String.format("CREATE OR REPLACE TEMPORARY VIEW tmp_%s\n" +
        "USING org.apache.spark.sql.jdbc OPTIONS (\n" +
        "url '%s',\n" +
        "dbtable '%s.%s',\n" +
        "user '%s',\n" +
        "password '%s');\n\n", postgreTable, postgreUrl, postgreDb, postgreTable, postgreUser, postgrePassword))

      println(String.format("insert overwrite table %s.%s partition(dt)\n" +
        "select %s,'ACTIVE' dt\n" +
        "from tmp_%s;\n\n", hiveDb, hiveTable, columns, postgreTable))
    } catch {case e : Exception => e.printStackTrace}
    finally {
      if (rs != null) try {rs.close} catch {case e : Exception => e.printStackTrace}
      if (stat != null) try {stat.close} catch {case e : Exception => e.printStackTrace}
      if (conn != null) try {conn.close} catch {case e : Exception => e.printStackTrace}
    }
  }

  val hiveTypeMap = Array(("bigint" -> "bigint"), ("int" -> "int"), ("bigserial" -> "bigint"), ("serial" -> "int"), ("character" -> "string"), ("text" -> "string"), ("real" -> "double"), ("double" -> "double"), ("numeric" -> "double"), ("time" -> "timestamp"), ("date" -> "timestamp"))
  def convertHiveType(postgreType : String) : String = {
    hiveTypeMap.foreach(entry => if (postgreType.contains(entry._1)) return entry._2)
    "string"
  }
  val dorisTypeMap = Array(("bigint" -> "bigint"), ("int" -> "int"), ("bigserial" -> "bigint"), ("serial" -> "int"), ("character" -> "varchar"), ("text" -> "varchar"), ("real" -> "double"), ("double" -> "double"), ("numeric" -> "double"), ("time" -> "datetime"), ("date" -> "datetime"))
  def convertDorisType(postgreType : String) : String = {
    dorisTypeMap.foreach(entry => if (postgreType.contains(entry._1)) return entry._2 + (if (postgreType.startsWith("character")) postgreType.substring(postgreType.indexOf("(")) else if (postgreType.startsWith("text")) "(4096)" else ""))
    "varchar(64)"
  }
}

object PostgreSQLTableGenerator {
  def main(args : Array[String]) : Unit = {
//    println("usage : postgreUrl postgreUser postgrePassword hiveDb postgreDb postgreTable timeColumn")

    val postgreUrl = args.apply(0)
    val postgreUser = args.apply(1)
    val postgrePassword = args.apply(2)

    val hiveDb = args.apply(3)
    val postgreDb = args.apply(4)

    val postgreTable = args.apply(5)
    var timeColumn = ""
    if (args.length > 6) timeColumn = args.apply(6)
    var isDorisOutput = false
    if (args.length > 7) isDorisOutput = args.apply(7).toBoolean

    val generator = new PostgreSQLTableGenerator(postgreUrl, postgreUser, postgrePassword)
    generator.generateTable(hiveDb, postgreDb, postgreTable, timeColumn, isDorisOutput)
  }
}
