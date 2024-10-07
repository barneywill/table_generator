package generator

import java.sql.{Connection, DriverManager, ResultSet, Statement}

class MysqlTableGenerator(mysqlUrl : String, mysqlUser : String, mysqlPassword : String) {
  Class.forName("com.mysql.jdbc.Driver")
  def generateTable(hiveDb : String, mysqlDb : String, mysqlTable : String, timeColumn : String = null) {
    var conn : Connection = null
    var stat : Statement = null
    var rs : ResultSet = null
    try {
      conn = DriverManager.getConnection(mysqlUrl, mysqlUser, mysqlPassword)
      stat = conn.createStatement()
      rs = stat.executeQuery(String.format("show create table %s.%s", mysqlDb, mysqlTable))
      var createTableSql = ""
      while (rs.next()) createTableSql = rs.getString(2)
      //      println(createTableSql)

      val hiveTable = String.format("ods_%s_%s", mysqlDb, mysqlTable)
      val timeCol = if (timeColumn == null) "create_time" else timeColumn

      val lines = createTableSql.replaceAll("`", "")
        .replaceAll("CREATE TABLE ", String.format("create external table %s.ods_%s_", hiveDb, mysqlDb))
        .split("\\n")
        .filter(line => !line.contains("KEY") && !line.contains("ENGINE"))
        .zipWithIndex
      var columns = ""
      lines.map(item => {
        var result = item._1
        if (item._2 > 0) result = {val arr = item._1.split("\\s+"); columns += "," + arr.apply(1); " " + arr.apply(1) + " " + convertType(arr.apply(2)) + (if (item._1.contains("COMMENT")) " " + item._1.substring(item._1.indexOf("COMMENT")) else ",")}
        if (item._2 == lines.length - 1) result = result.substring(0, result.length - 1)
        result
      })
        .foreach(println)
      println(")\npartitioned by (dt string)\nstored as orc\ntblproperties(\"orc.compression\"=\"SNAPPY\");")
      columns = columns.substring(1)
      println
      println(String.format("CREATE OR REPLACE TEMPORARY VIEW tmp_%s\n" +
        "USING org.apache.spark.sql.jdbc OPTIONS (\n" +
        "url '%s',\n" +
        "dbtable '%s',\n" +
        "user '%s',\n" +
        "password '%s');\n\n", mysqlTable, mysqlUrl, mysqlTable, mysqlUser, mysqlPassword))
      println(String.format("insert overwrite table %s.%s partition(dt)\n" +
        "select %s,'ACTIVE' dt\n" +
        "from tmp_%s;\n\n", hiveDb, hiveTable, columns, mysqlTable))

      if (timeColumn != null && !timeColumn.isEmpty) println(String.format("insert overwrite table %s.%s partition(dt)\n" +
        "select %s,from_unixtime(unix_timestamp(%s), 'yyyyMMdd') dt\n" +
        "from %s.%s\n" +
        "where dt = 'ACTIVE';\n\n" +
        "insert overwrite table %s.%s partition(dt)\n" +
        "select %s,from_unixtime(unix_timestamp(%s), 'yyyyMMdd') dt\n" +
        "from tmp_%s\n" +
        "where %s >= date_sub(now(), ${diff}) and %s <= date_sub(now(), ${diff} - ${days});", hiveDb, hiveTable, columns, timeCol,hiveDb, hiveTable, hiveDb, hiveTable, columns, timeCol, mysqlTable, timeCol, timeCol))
    } catch {case e : Exception => e.printStackTrace}
    finally {
      if (rs != null) try {rs.close} catch {case e : Exception => e.printStackTrace}
      if (stat != null) try {stat.close} catch {case e : Exception => e.printStackTrace}
      if (conn != null) try {conn.close} catch {case e : Exception => e.printStackTrace}
    }
  }

  val typeMap = Map("int" -> "int", "varchar" -> "string", "decimal" -> "double", "text" -> "string", "datetime" -> "timestamp")
  def convertType(mysqlType : String) : String = {
    typeMap.foreach(entry => if (mysqlType.contains(entry._1)) return entry._2)
    "string"
  }
}

object MysqlTableGenerator {
  def main(args : Array[String]) : Unit = {
    println("usage : mysqlUrl mysqlUser mysqlPassword hiveDb mysqlDb mysqlTable timeColumn")

    val mysqlUrl = args.apply(0)
    val mysqlUser = args.apply(1)
    val mysqlPassword = args.apply(2)

    val hiveDb = args.apply(3)
    val mysqlDb = args.apply(4)

    val mysqlTable = args.apply(5)
    val timeColumn = args.apply(6)

    val generator = new MysqlTableGenerator(mysqlUrl, mysqlUser, mysqlPassword)
    generator.generateTable(hiveDb, mysqlDb, mysqlTable, timeColumn)
  }
}

