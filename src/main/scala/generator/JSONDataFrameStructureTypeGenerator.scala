package generator

import scala.util.parsing.json.JSON

class JSONDataFrameStructureTypeGenerator {
    def isDouble(value : Double) : Boolean = Math.abs(value) - Math.abs(value.toLong.toDouble) > 0.001
    val isEmptyArrayException = false
    val defaultIndent = "    "
    def generateDataFrameStructureType(json: String): String = {
        var result = "schema = StructType(["
        JSON.parseFull(json) match {
            case Some(map : Map[String, Any]) => {map.keySet.foreach(key => {
                val value = map.get(key).get
                if (value == null) result += f"  StructField('${key}', StringType(), True),\n"
                else {
                    //println(key + ", " + value.getClass)
                    value match {
                        case value if value.getClass == Class.forName("java.lang.Double") => result += f"${defaultIndent}StructField('${key}', " + (if (isDouble(value.asInstanceOf[java.lang.Double])) "DoubleType()" else "LongType()") + ", True),\n"
                        case value if value.getClass == Class.forName("java.lang.String") => result += f"${defaultIndent}StructField('${key}', StringType(), True),\n"
                        case value if value.getClass == Class.forName("java.lang.Boolean") => result += f"${defaultIndent}StructField('${key}', BooleanType(), True),\n"
                        case value if value.isInstanceOf[Map[String, Any]] && !value.asInstanceOf[Map[String, Any]].isEmpty => result += f"${defaultIndent}StructField('${key}', " + generateRecursive(value.asInstanceOf[Map[String, Any]], defaultIndent + defaultIndent) + ",\n"
                        case value if value.isInstanceOf[Seq[Any]] => if (value.asInstanceOf[Seq[Any]].isEmpty) {if (isEmptyArrayException) throw new RuntimeException("array with key is empty : " + key)} else result += f"${defaultIndent}StructField('${key}',  ArrayType(" + generateRecursive(value.asInstanceOf[Seq[Any]].head, defaultIndent + defaultIndent) + "),\n"
                    }
                }
            })}
        }
        result = result.substring(0, result.length -2) + "\n)"
        result += "])"
        result
    }
    def generateRecursive(value : Any, indent : String) : String = {
        var result = ""
        if (value.isInstanceOf[String]) "StringType()"
        else if (value.isInstanceOf[Double]) if (isDouble(value.asInstanceOf[Double])) "DoubleType()" else "LongType()"
        else if (value.isInstanceOf[Boolean]) "BooleanType()"
        else if (value.isInstanceOf[Map[String, Any]]) {
            val map = value.asInstanceOf[Map[String, Any]]
            map.keySet.foreach(key => {
                val valueTmp = map.get(key).get
                if (valueTmp == null) result += f"${defaultIndent}${indent}StructField('${key}', StringType(), True),\n"
                else {
                    //println(key + ", " + valueTmp.getClass)
                    valueTmp match {
                        case valueTmp if valueTmp.getClass == Class.forName("java.lang.Double") => result += f"${defaultIndent}${indent}StructField('${key}', " + (if (isDouble(valueTmp.asInstanceOf[java.lang.Double])) "DoubleType()" else "LongType()") + ", True),\n"
                        case valueTmp if valueTmp.getClass == Class.forName("java.lang.String") => result += f"${defaultIndent}${indent}StructField('${key}', StringType(), True),\n"
                        case valueTmp if valueTmp.getClass == Class.forName("java.lang.Boolean") => result += f"${defaultIndent}${indent}StructField('${key}', BooleanType(), True),\n"
                        case valueTmp if valueTmp.isInstanceOf[Map[String, Any]] && !valueTmp.asInstanceOf[Map[String, Any]].isEmpty => result += f"${defaultIndent}${indent}StructField('${key}', " + generateRecursive(valueTmp.asInstanceOf[Map[String, Any]], indent + defaultIndent) + f"${defaultIndent}${indent}),\n"
                        case valueTmp if valueTmp.isInstanceOf[Seq[Any]] => if (valueTmp.asInstanceOf[Seq[Any]].isEmpty) {if (isEmptyArrayException) throw new RuntimeException("array with key is empty : " + key)} else result += f"${defaultIndent}${indent}StructField('${key}', ArrayType(" + generateRecursive(valueTmp.asInstanceOf[Seq[Any]].head, isNameQuoted) + f"${defaultIndent}${indent}),"
                        case _ => {}
                    }
                }
            })
            f"\n${indent}StructType([\n${result}\n${indent}])"
        }
        else ""
    }
}

object JSONDataFrameStructureTypeGenerator {
    def main(args : Array[String]): Unit = {
        println("usage : generator.JSONDataFrameStructureTypeGenerator <json>")
        var json = "{\"a\":\"\",\"b\":0.0,\"x\":5.3,\"y\":1627451695759,\"c\":{\"d\":\"\",\"e\":\"\"},\"f\":[{\"g\":\"\",\"h\":\"\"}]}"
        if (args.length > 0) json = args.apply(0)
        val generator = new JSONDataFrameStructureTypeGenerator()
        println(generator.generateDataFrameStructureType(json))
    }
}