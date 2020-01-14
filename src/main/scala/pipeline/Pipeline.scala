package pipeline

import destinations.Writer
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{BooleanType, DataType, DoubleType, IntegerType, StringType, StructField, StructType}
import org.yaml.snakeyaml.Yaml
import sources.Reader
import transformations.ColumnAppender

import scala.collection.JavaConverters._


object Pipeline {


  def buildEtl(config : String)(implicit sparkSession: SparkSession): Etl = {
    val configPath = scala.io.Source.fromFile(config).mkString
    val yml = new Yaml()
    val configuration = yml.load(configPath).asInstanceOf[java.util.Map[String, Object]]
    val readerPath = configuration.get("reader").asInstanceOf[java.util.Map[String, String]].get("path")
    val readerFormat = configuration.get("reader").asInstanceOf[java.util.Map[String, String]].get("format")
    val readerOptions = configuration.get("reader").asInstanceOf[java.util.Map[String, Object]].get("options")
      .asInstanceOf[java.util.Map[String, String]].asScala
    val schemaFields: List[StructField] = configuration.get("reader").asInstanceOf[java.util.Map[String, Object]].get("schema")
      .asInstanceOf[java.util.ArrayList[java.util.Map[String, String]]].toArray().toList
      .map(x => x.asInstanceOf[java.util.Map[String, String]])
      .map(fieldMap => StructField(fieldMap.get("name"), replaceTypeWithDatatype(fieldMap.get("type")), fieldMap.get("nullable").toBoolean))
    val reader = Reader(readerPath, readerFormat, StructType(schemaFields), readerOptions.toMap)

    val writerPath = configuration.get("writer").asInstanceOf[java.util.Map[String, String]].get("path")
    val mode = configuration.get("writer").asInstanceOf[java.util.Map[String, String]].get("mode")
    val partitionedBy = configuration.get("writer").asInstanceOf[java.util.Map[String, String]].get("partitionedBy")
    val format = configuration.get("writer").asInstanceOf[java.util.Map[String, String]].get("format")
    val compression = configuration.get("writer").asInstanceOf[java.util.Map[String, String]].get("compression")
    val writerOptions = configuration.get("writer").asInstanceOf[java.util.Map[String, Object]]
      .getOrDefault("options",new java.util.HashMap[String,String]())
      .asInstanceOf[java.util.Map[String, String]].asScala
    println(writerPath)
    println( mode)
    println(partitionedBy)
    println(compression)
    println(format)
    println(writerOptions.toMap)
    val writer = Writer(writerPath, mode, partitionedBy, compression, format, writerOptions.toMap)

    val transformer = configuration.get("transformer").asInstanceOf[java.util.ArrayList[java.util.Map[String, String]]]
      .toArray().toList
      .map(x => x.asInstanceOf[java.util.Map[String, String]])
      .map(x => {
        x.get("rule") match {
          case "tagWithName" => {
            val a : DataFrame => DataFrame = ColumnAppender.tagWithFileName(x.get("columnName"))
            a
          }
        }
      })

    Etl(reader, transformer, writer)
  }

  def replaceTypeWithDatatype(typeName: String): DataType = {
    typeName match {
      case "String" => StringType
      case "Integer" => IntegerType
      case "Double" => DoubleType
      case "Boolean" => BooleanType

    }
  }
}

