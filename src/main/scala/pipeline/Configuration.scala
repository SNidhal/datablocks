package pipeline

import constraints.{ColumnValidator, Constraint}
import destinations.Writer
import org.apache.spark
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{BooleanType, DataType, DoubleType, IntegerType, StringType, StructField, StructType}
import org.yaml.snakeyaml.Yaml
import sources.Reader
import transformations.{ColumnAppender, Identity, Joiner, QueryHandler}
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
      case "Date" => spark.sql.types.DateType
      case "Float" => spark.sql.types.FloatType
    }
  }


  def buildReader(readerConfig: Object): Reader = {
    val readerPath = readerConfig.asInstanceOf[java.util.Map[String, String]].get("path")
    val readerFormat = readerConfig.asInstanceOf[java.util.Map[String, String]].get("format")
    val processMode = readerConfig.asInstanceOf[java.util.Map[String, String]].get("processMode")
    val readerOptions = readerConfig.asInstanceOf[java.util.Map[String, Object]].get("options")
      .asInstanceOf[java.util.Map[String, String]].asScala
    val schemaFields: List[StructField] = readerConfig.asInstanceOf[java.util.Map[String, Object]].get("schema")
      .asInstanceOf[java.util.ArrayList[java.util.Map[String, String]]].toArray().toList
      .map(x => x.asInstanceOf[java.util.Map[String, String]])
      .map(fieldMap => StructField(fieldMap.get("name"),
        replaceTypeWithDatatype(fieldMap.getOrDefault("type", "String")),
        fieldMap.getOrDefault("nullable", "true").toBoolean))
    Reader(readerPath, readerFormat, StructType(schemaFields), readerOptions.toMap,processMode)
  }

  def buildTransformer(transformerConfig: Object)(implicit sparkSession: SparkSession): List[DataFrame => DataFrame] = {
    if(transformerConfig==null){
      return List(Identity.getIdentity)
    }
    transformerConfig.asInstanceOf[java.util.ArrayList[java.util.Map[String, String]]]
      .toArray().toList
      .map(x => x.asInstanceOf[java.util.Map[String, Object]])
      .map(x => {
        x.get("type") match {
          case "ColumnAppender" => x.get("rule") match {
            case "FileName" =>
              val a: DataFrame => DataFrame = ColumnAppender.tagWithFileNameHDFS(x.get("columnName").asInstanceOf[String])
              a
            case "addRankInGroup" =>
              val a: DataFrame => DataFrame = ColumnAppender.addRankInGroup(x.get("columnName").asInstanceOf[String],
                x.get("groupColumn").asInstanceOf[String], x.get("rankingColumn").asInstanceOf[String])
              a
            case "deriveColumn" =>
              val a: DataFrame => DataFrame = ColumnAppender.deriveColumn(x.get("columnName").asInstanceOf[String],
                x.get("expression").asInstanceOf[String])
              a
            case "FileSize" =>
              val a: DataFrame => DataFrame = ColumnAppender.tagWithFileSizeHDFS(x.get("columnName").asInstanceOf[String])
              a
            case "FileOwner" =>
              val a: DataFrame => DataFrame = ColumnAppender.tagWithFileOwnerHDFS(x.get("columnName").asInstanceOf[String])
              a
            case "FileOwnerGroup" =>
              val a: DataFrame => DataFrame = ColumnAppender.tagWithFileOwnerGroup(x.get("columnName").asInstanceOf[String])
              a
            case "FileModificationTime" =>
              val a: DataFrame => DataFrame = ColumnAppender.tagWithFileModificationTime(x.get("columnName").asInstanceOf[String])
              a
            case "FilePermission" =>
              val a: DataFrame => DataFrame = ColumnAppender.tagWithFilePermission(x.get("columnName").asInstanceOf[String])
              a
            case "functionOverWindow" =>
              val columnName = x.get("columnName").asInstanceOf[String]
              val function = x.get("function").asInstanceOf[String]
              val partitionColumn = x.get("partitionColumn").asInstanceOf[String]
              val order = x.getOrDefault("order", null).asInstanceOf[String]
              val orderColumn = x.getOrDefault("orderColumn", null).asInstanceOf[String]
              val rangeStart = x.getOrDefault("rangeStart", null).asInstanceOf[String]
              val rangeEnd = x.getOrDefault("rangeEnd", null).asInstanceOf[String]


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
            val value = x.getOrDefault("value",null)
            val valueType = x.getOrDefault("valueType",null)

            val mapping = x.get("mapping").asInstanceOf[java.util.Map[String, String]].asScala

            strategy match {
              case "Drop" => val a: DataFrame => DataFrame = NullHandler.dropNull(x.get("how").asInstanceOf[String], columns)
                a
              case "Replace" => val a: DataFrame => DataFrame = NullHandler.replaceNull(columns, mapping.toMap)
                a
              case "Fill" => val a: DataFrame => DataFrame = NullHandler.fillNull(value.toString,valueType.toString,columns,null)
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
    val partitionedBySource = writerConfig.asInstanceOf[java.util.Map[String, java.util.ArrayList[String]]]
      .getOrDefault("partitionedBy", null)
    val partitionedBy = partitionedBySource match {
      case null => null
      case partitionColumnList: Object => partitionColumnList.asInstanceOf[java.util.ArrayList[String]].toArray.toList
    }
    val format = writerConfig.asInstanceOf[java.util.Map[String, String]].get("format")
    val processMode = writerConfig.asInstanceOf[java.util.Map[String, String]].get("processMode")
    val triggerType = writerConfig.asInstanceOf[java.util.Map[String, String]].get("triggerType")
    val triggerValue = writerConfig.asInstanceOf[java.util.Map[String, String]].get("triggerValue")
    val compression = writerConfig.asInstanceOf[java.util.Map[String, String]].get("compression")
    val writerOptions = writerConfig.asInstanceOf[java.util.Map[String, Object]]
      .getOrDefault("options", new java.util.HashMap[String, String]())
      .asInstanceOf[java.util.Map[String, String]].asScala
    val tableName = writerConfig.asInstanceOf[java.util.Map[String, String]].get("tableName")
    Writer(writerPath, mode, partitionedBy.asInstanceOf[List[String]], compression, format, writerOptions.toMap,
      tableName, processMode,triggerType,triggerValue)
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

