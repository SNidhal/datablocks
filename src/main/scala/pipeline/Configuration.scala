package pipeline

import constraints.{ColumnValidator, Constraint}
import destinations.Writer
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{BooleanType, DataType, DoubleType, IntegerType, StringType, StructField, StructType}
import org.yaml.snakeyaml.Yaml
import sources.Reader
import transformations.{ColumnAppender, Joiner, QueryHandler}
import validation.NullHandler

import scala.collection.JavaConverters._


object Configuration {


  def buildEtlList(config: String)(implicit sparkSession: SparkSession): List[Etl] = {
    val configSource = scala.io.Source.fromFile(config)
    val configContent = configSource.mkString
    val yml = new Yaml()
    val root = yml.load(configContent).asInstanceOf[java.util.ArrayList[java.util.Map[String, Object]]].toArray().toList
    root.map(config => {
      val configuration = config.asInstanceOf[java.util.Map[String, Object]]

      val readerRoot = configuration.getOrDefault("reader", null)

      val reader = readerRoot match {
        case null => null
        case obj: Object => buildReader(obj)
      }

      val joinerRoot = configuration.getOrDefault("joiner", null)

      val joiner = joinerRoot match {
        case null => null
        case obj: Object => buildJoiner(obj)
      }

      val writerRoot = configuration.getOrDefault("writer", null)

      val writer = writerRoot match {
        case null => null
        case obj: Object => buildWriter(obj)
      }

      val transformerRoot = configuration.getOrDefault("transformer", null)
      val transformer = buildTransformer(transformerRoot)

      val id = configuration.get("id").toString

      val destinationId = configuration.getOrDefault("destinationId", None)
      val dest = destinationId match {
        case None => None
        case _: String => Some(destinationId.toString)
      }
      Etl(reader, transformer, writer, dest, id, joiner)
    })
  }

  def replaceTypeWithDatatype(typeName: String): DataType = {
    typeName match {
      case "String" => StringType
      case "Integer" => IntegerType
      case "Double" => DoubleType
      case "Boolean" => BooleanType

    }
  }


  def buildReader(readerConfig: Object): Reader = {
    val readerPath = readerConfig.asInstanceOf[java.util.Map[String, String]].get("path")
    val readerFormat = readerConfig.asInstanceOf[java.util.Map[String, String]].get("format")
    val readerOptions = readerConfig.asInstanceOf[java.util.Map[String, Object]].get("options")
      .asInstanceOf[java.util.Map[String, String]].asScala
    val schemaFields: List[StructField] = readerConfig.asInstanceOf[java.util.Map[String, Object]].get("schema")
      .asInstanceOf[java.util.ArrayList[java.util.Map[String, String]]].toArray().toList
      .map(x => x.asInstanceOf[java.util.Map[String, String]])
      .map(fieldMap => StructField(fieldMap.get("name"),
        replaceTypeWithDatatype(fieldMap.getOrDefault("type","String")),
        fieldMap.getOrDefault("nullable","true").toBoolean))
    Reader(readerPath, readerFormat, StructType(schemaFields), readerOptions.toMap)
  }

  def buildTransformer(transformerConfig: Object)(implicit sparkSession: SparkSession): List[DataFrame => DataFrame] = {
    transformerConfig.asInstanceOf[java.util.ArrayList[java.util.Map[String, String]]]
      .toArray().toList
      .map(x => x.asInstanceOf[java.util.Map[String, Object]])
      .map(x => {
        x.get("type") match {
          case "ColumnAppender" => x.get("rule") match {
            case "tagWithName" =>
              val a: DataFrame => DataFrame = ColumnAppender.tagWithFileName(x.get("columnName").asInstanceOf[String])
              a
            case "addRankInGroup" =>
              val a: DataFrame => DataFrame = ColumnAppender.addRankInGroup(x.get("columnName").asInstanceOf[String],
                x.get("groupColumn").asInstanceOf[String], x.get("rankingColumn").asInstanceOf[String])
              a
            case "deriveColumn" =>
              val a: DataFrame => DataFrame = ColumnAppender.deriveColumn(x.get("columnName").asInstanceOf[String],
                x.get("expression").asInstanceOf[String])
              a
            case "tagWithSize" =>
              val a: DataFrame => DataFrame = ColumnAppender.tagWithFileSize(x.get("columnName").asInstanceOf[String])
              a
            case "tagWithOwner" =>
              val a: DataFrame => DataFrame = ColumnAppender.tagWithFileOwner(x.get("columnName").asInstanceOf[String])
              a
            case "functionOverWindow" =>
              val columnName =x.get("columnName").asInstanceOf[String]
              val function =x.get("function").asInstanceOf[String]
              val partitionColumn =x.get("partitionColumn").asInstanceOf[String]
              val order =x.getOrDefault("order",null).asInstanceOf[String]
              val orderColumn=x.getOrDefault("orderColumn",null).asInstanceOf[String]
              val rangeStart=x.getOrDefault("rangeStart",null).asInstanceOf[String]
              val rangeEnd=x.getOrDefault("rangeEnd",null).asInstanceOf[String]


              val a: DataFrame => DataFrame = ColumnAppender.runOverWindow(
                columnName,
                partitionColumn,
                orderColumn,
                order,
                function,
                rangeStart,
                rangeEnd
              )
              a
          }
          case "QueryHandler" =>
            val a: DataFrame => DataFrame = QueryHandler.execute(x.get("query").asInstanceOf[String])
            a
          case "NullHandler" =>
            val columns: List[String] = x.getOrDefault("columns", new java.util.ArrayList[String])
              .asInstanceOf[java.util.ArrayList[String]]
              .toArray().toList.map(x => x.asInstanceOf[String])
            val strategy: String = x.get("strategy").toString

            val mapping = x.get("mapping").asInstanceOf[java.util.Map[String, String]].asScala

            strategy match {
              case "drop" => val a: DataFrame => DataFrame = NullHandler.dropNull(x.get("how").asInstanceOf[String], columns)
                a
              case "replace" => val a: DataFrame => DataFrame = NullHandler.replaceNull(columns, mapping.toMap)
                a
            }
          case "Validation" =>
            val constraintList = x.get("constraints").asInstanceOf[java.util.ArrayList[java.util.Map[String, String]]]
              .toArray().toList
              .map(x => x.asInstanceOf[java.util.Map[String, String]])
              .map(x => Constraint(x.get("condition"), x.get("mode")))
            val a: DataFrame => DataFrame = ColumnValidator.check(constraintList)
            a
        }
      }) :+ {
      val a: DataFrame => DataFrame = ColumnAppender.removeUtilityColumn
      a
    }
  }


  def buildWriter(writerConfig: Object)(implicit sparkSession: SparkSession): Writer = {
    val writerPath = writerConfig.asInstanceOf[java.util.Map[String, String]].get("path")
    val mode = writerConfig.asInstanceOf[java.util.Map[String, String]].get("mode")
    val partitionedBy = writerConfig.asInstanceOf[java.util.Map[String, String]].get("partitionedBy")
    val format = writerConfig.asInstanceOf[java.util.Map[String, String]].get("format")
    val compression = writerConfig.asInstanceOf[java.util.Map[String, String]].get("compression")
    val writerOptions = writerConfig.asInstanceOf[java.util.Map[String, Object]]
      .getOrDefault("options", new java.util.HashMap[String, String]())
      .asInstanceOf[java.util.Map[String, String]].asScala
    val tableName = writerConfig.asInstanceOf[java.util.Map[String, String]].get("tableName")
    Writer(writerPath, mode, partitionedBy, compression, format, writerOptions.toMap, tableName)
  }

  def buildJoiner(joinerConfig: Object)(implicit sparkSession: SparkSession): Joiner = {
    val left = joinerConfig.asInstanceOf[java.util.Map[String, String]].get("left")
    val right = joinerConfig.asInstanceOf[java.util.Map[String, String]].get("right")
    val joinType = joinerConfig.asInstanceOf[java.util.Map[String, String]].get("type")
    val mapping = joinerConfig.asInstanceOf[java.util.Map[String, String]].get("mapping")
      .asInstanceOf[java.util.Map[String, String]].asScala

    Joiner(left, right, joinType, mapping.toMap)
  }


}

