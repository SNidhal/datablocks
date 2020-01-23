package validation

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{BooleanType, DataType, IntegerType, LongType, StringType}

object NullHandler {

  def dropNull(how: String, cols: Seq[String])(df: DataFrame): DataFrame = {
    (how, cols) match {
      case (s: String, cl: Seq[String]) => df.na.drop(s, cl)
      case (s: String, null) => df.na.drop(s)
      case (null, cl: Seq[String]) => df.na.drop(cl)
      case _ => df.na.drop()
    }
  }

  def fillNull(value: String, valueType: String, cols: Seq[String], valueMap: Map[String, String])(df: DataFrame): DataFrame = {
    (value, cols, valueMap) match {
      case (null, null, vm: Map[String, String]) => df.na.fill(vm.map(x => x._1 -> castTo(x._2, df.schema.apply(x._1).dataType)))
      case (v: String, c: Seq[String], null) => df.na.fill(v, c)
      case (v: String, null, null) => valueType match {
        case "Integer" => df.na.fill(v.toInt)
        case "Long" => df.na.fill(v.toLong)
        case "String" => df.na.fill(v)
        case "Boolean" => df.na.fill(v.equals("true"))
      }
    }
  }


  def replaceNull(cols: Seq[String], replacement: Map[String, String])(df: DataFrame): DataFrame = {
    val dataType = df.schema.apply(cols.head).dataType
    val castedReplacement = replacement.map(x => castTo(x._1, dataType) -> castTo(x._2, dataType))
    df.na.replace(cols, castedReplacement)
  }

  def castTo(value: String, dataType: DataType): Any = {
    dataType match {
      case _: IntegerType => value.toInt
      case _: LongType => value.toLong
      case _: StringType => value
      case _: BooleanType => value.equals("true")
    }
  }


}
