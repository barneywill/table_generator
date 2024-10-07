package generator

import java.sql.{Connection, DriverManager, ResultSet, Statement}

class Hive2DorisTableGenerator(hiveUrl:String, hiveUser:String) {
  Class.forName("org.apache.hive.jdbc.HiveDriver")

  def generateTable(hiveDb: String, hiveTable: String, dorisDb: String, dorisTable:String): Unit = {
    var conn: Connection = null
    var stat: Statement = null
    var rs: ResultSet = null
    try {
      conn = DriverManager.getConnection(hiveUrl, hiveUser, "")
      stat = conn.createStatement()
      rs = stat.executeQuery(String.format("show create table %s.%s", hiveDb, hiveTable))
      var createDorisTableSql = ""
      var body: String = ""
      var colums = ""
      var text = "COMMENT ''"
      var flag = 1
      var key = ""
      while (rs.next()) {
        if (rs.getString(1).contains("COMMENT")) {
          val arr: Array[String] = rs.getString(1).trim.split("\\s+")
          if (flag == 1) {
            body += (arr(0) + " " + convertDorisType(arr(1)) + " NULL " + arr(2) + " " + arr(3) + "\n")
            key = arr(0)
            flag = 0
          }else{
            if (arr.length >= 4)
              body += (arr(0) + " " + convertDorisType(arr(1)) + " REPLACE_IF_NOT_NULL NULL " + arr(2) + " " + arr(3) + "\n")
            else
              text = rs.getString(1)
          }
        }
      }
      val head = String.format("create table if not exists %s.%s (\n", dorisDb, dorisTable)
      val tail = String.format("ENGINE=OLAP\nAGGREGATE KEY(%s)\n%s\nDISTRIBUTED BY HASH(%s) BUCKETS 4\nPROPERTIES (\n\"replication_num\" = \"2\",\n\"colocate_with\" = \" \",\n\"in_memory\" = \"false\",\n\"storage_format\" = \"DEFAULT\"\n);", key, text, key)
      val str: String = head + body + tail
      System.out.println(str)
    } catch {case e : Exception => e.printStackTrace}
    finally {
      if (rs != null) try {rs.close} catch {case e : Exception => e.printStackTrace}
      if (stat != null) try {stat.close} catch {case e : Exception => e.printStackTrace}
      if (conn != null) try {conn.close} catch {case e : Exception => e.printStackTrace}
    }
  }
  val dorisTypeMap = Array(("bigint" -> "bigint"), ("int" -> "int"), ("string" -> "varchar(1024)"), ("double" -> "double"), ("timestamp" -> "datetime"))

  def convertDorisType(postgreType: String): String = {
    dorisTypeMap.foreach(entry => if (postgreType.contains(entry._1)) return entry._2 else "")
    "varchar(64)"
  }
}
object Hive2DorisTableGenerator {
  def main(args: Array[String]): Unit = {
    val hiveUrl = args(0)
    val hiveUser = args(1)
    val hiveDb = args(2)
    val hiveTable = args(3)
    val dorisDb = args(4)
    val dorisTable = args(5)
    val generator = new Hive2DorisTableGenerator(hiveUrl, hiveUser)
    generator.generateTable(hiveDb, hiveTable, dorisDb, dorisTable)
  }
}
