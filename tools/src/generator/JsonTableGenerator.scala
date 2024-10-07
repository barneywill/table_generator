package generator

import scala.util.parsing.json.JSON

/**
  * Created by barney on 2019/7/4.
  */
class JsonTableGenerator {
  def isDouble(value : Double) : Boolean = Math.abs(value) - Math.abs(value.toLong.toDouble) > 0.001

  val isEmptyArrayException = false
  def generateTable(dbName : String, tableName : String, json : String, isNameQuoted : Boolean = false, format : String = "json") : String = {
    var result = String.format("create external table %s.%s (\n", dbName, tableName)
    JSON.parseFull(json) match {
      case Some(map : Map[String, Any]) => {map.keySet.foreach(key => {
        val value = map.get(key).get
        if (value == null) result += getKey(key, isNameQuoted) + " string,\n"
        else {
          //println(key + ", " + value.getClass)
          value match {
            case value if value.getClass == Class.forName("java.lang.Double") => result += getKey(key, isNameQuoted) + " " + (if (isDouble(value.asInstanceOf[java.lang.Double])) "double" else "bigint") + ",\n"
            case value if value.getClass == Class.forName("java.lang.String") => result += getKey(key, isNameQuoted) + " string,\n"
            case value if value.getClass == Class.forName("java.lang.Boolean") => result += getKey(key, isNameQuoted) + " boolean,\n"
            case value if value.isInstanceOf[Map[String, Any]] && !value.asInstanceOf[Map[String, Any]].isEmpty => result += getKey(key, isNameQuoted) + " " + generateRecursive(value.asInstanceOf[Map[String, Any]], isNameQuoted) + ",\n"
            case value if value.isInstanceOf[Seq[Any]] => if (value.asInstanceOf[Seq[Any]].isEmpty) {if (isEmptyArrayException) throw new RuntimeException("array with key is empty : " + key)} else result += getKey(key, isNameQuoted) + " array<" + generateRecursive(value.asInstanceOf[Seq[Any]].head, isNameQuoted) + ">,\n"
          }
        }
      })}
    }
    result = result.substring(0, result.length -2) + "\n)"
    result += "\npartitioned by (dt string) \nROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe' \nstored as textfile\nlocation '';"
    result
  }
  def generateRecursive(value : Any, isNameQuoted : Boolean) : String = {
    var result = ""
    if (value.isInstanceOf[String]) "string"
    else if (value.isInstanceOf[Double]) if (isDouble(value.asInstanceOf[Double])) "double" else "bigint"
    else if (value.isInstanceOf[Boolean]) "boolean"
    else if (value.isInstanceOf[Map[String, Any]]) {
      val map = value.asInstanceOf[Map[String, Any]]
      map.keySet.foreach(key => {
        val valueTmp = map.get(key).get
        if (valueTmp == null) result += ", " + getKey(key, isNameQuoted) + " : string"
        else {
          //println(key + ", " + valueTmp.getClass)
          valueTmp match {
            case valueTmp if valueTmp.getClass == Class.forName("java.lang.Double") => result += ", " + getKey(key, isNameQuoted) + " : " + (if (isDouble(valueTmp.asInstanceOf[java.lang.Double])) "double" else "bigint")
            case valueTmp if valueTmp.getClass == Class.forName("java.lang.String") => result += ", " + getKey(key, isNameQuoted) + " : string"
            case valueTmp if valueTmp.getClass == Class.forName("java.lang.Boolean") => result += ", " + getKey(key, isNameQuoted) + " : boolean"
            case valueTmp if valueTmp.isInstanceOf[Map[String, Any]] && !valueTmp.asInstanceOf[Map[String, Any]].isEmpty => result += ", " + getKey(key, isNameQuoted) + " : " + generateRecursive(valueTmp.asInstanceOf[Map[String, Any]], isNameQuoted)
            case valueTmp if valueTmp.isInstanceOf[Seq[Any]] => if (valueTmp.asInstanceOf[Seq[Any]].isEmpty) {if (isEmptyArrayException) throw new RuntimeException("array with key is empty : " + key)} else result += ", " + getKey(key, isNameQuoted) + " : array<" + generateRecursive(valueTmp.asInstanceOf[Seq[Any]].head, isNameQuoted) + ">"
            case _ => {}
          }
        }
      })
      "struct<" + result.substring(2) + ">"
    }
    else ""
  }
  def getKey(key : String, isNameQuoted : Boolean) : String = (if (isNameQuoted) "`" else "") + key + (if (isNameQuoted) "`" else "")
}

object JsonTableGenerator {
  var dbName = "test_db"
  var tableName = "test_table"
  var json = "{\"a\":\"\",\"b\":0.0,\"x\":5.3,\"y\":1627451695759,\"c\":{\"d\":\"\",\"e\":\"\"},\"f\":[{\"g\":\"\",\"h\":\"\"}]}"
  var isNameQuoted = true
  def main(args : Array[String]): Unit = {
    println("usage : generator.JsonTableGenerator <db_name> <table_name> <json> <isNameQuoted>")
    if (args.length > 0) dbName = args.apply(0)
    if (args.length > 1) tableName = args.apply(1)
    if (args.length > 2) json = args.apply(2)
    if (args.length > 3) isNameQuoted = args.apply(3).toBoolean
    println(s"parameters : $dbName $tableName $json $isNameQuoted")
    println("generated sql : ")
    println(new JsonTableGenerator().generateTable(dbName, tableName, json.replaceAll("\\.0", ".1"), isNameQuoted))
  }
}
