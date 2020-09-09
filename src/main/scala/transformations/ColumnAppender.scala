package transformations

import java.net.URI
import java.nio.file.{Files, Paths}
import java.nio.file.attribute.FileOwnerAttributeView

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.{Window, WindowSpec}

import scala.reflect.io.File

object ColumnAppender {

  def tagWithFileName(columnName: String)(df: DataFrame)(implicit spark: SparkSession): DataFrame = {
    spark.udf.register("file_name", (path: String) => path.substring(path.lastIndexOf("/") + 1, path.lastIndexOf(".")))

    val schemaFields = df.schema.fields.map(x => x.name)
    if (schemaFields.contains("input_file_name")) {
      df.withColumn(columnName, callUDF("file_name", col("input_file_name")))
    } else {
      val df2 = df.withColumn("input_file_name", input_file_name())
      df2.withColumn(columnName, callUDF("file_name", col("input_file_name")))
    }
  }

  def addRankInGroup(columnName: String, groupColumn: String, rankingColumn: String)(df: DataFrame)(implicit spark: SparkSession): DataFrame = {
    val window = Window.partitionBy(groupColumn).orderBy(col(rankingColumn).desc)
    df.withColumn(columnName, row_number.over(window))
  }

  def deriveColumn(columnName: String, expression: String)(df: DataFrame)(implicit spark: SparkSession): DataFrame = {
    df.withColumn(columnName, expr(expression))
  }


  def runOverWindow(columnName: String, partitionColumn: String, orderColumn: String, order: String, function: String,
                    rangeStart: String, rangeEnd: String)(df: DataFrame)(implicit spark: SparkSession): DataFrame = {
    val window: WindowSpec = (partitionColumn, orderColumn, order, rangeStart, rangeEnd) match {
      case (pc: String, null, null, null, null) => Window.partitionBy(pc)
      case (null, oc: String, null, null, null) => Window.orderBy(oc)
      case (pc: String, oc: String, o: String, null, null) => o match {
        case "asc" => Window.partitionBy(pc).orderBy(col(oc).asc)
        case "desc" => Window.partitionBy(pc).orderBy(col(oc).desc)
      }
      case (null, null, null, rmin: String, rmax: String) => Window.rangeBetween(rmin.asInstanceOf[Long], rmax.asInstanceOf[Long])
    }
    function match {
      case "row_number" => df.withColumn(columnName, row_number.over(window))
      case "rank" => df.withColumn(columnName, rank.over(window))
      case "percent_rank" => df.withColumn(columnName, percent_rank.over(window))
      case "dense_rank" => df.withColumn(columnName, dense_rank.over(window))
      case "ntile" => df.withColumn(columnName, row_number.over(window))
      case "cume_dist" => df.withColumn(columnName, row_number.over(window))
      case "lag" => df.withColumn(columnName, row_number.over(window))
    }
  }


  def tagWithFileSize(columnName: String)(df: DataFrame)(implicit spark: SparkSession): DataFrame = {
    spark.udf.register("file_size", (path: String) => File(path.substring(8)).length)
    val schemaFields = df.schema.fields.map(x => x.name)
    if (schemaFields.contains("input_file_name")) {

      val df2 = df.select("input_file_name").withColumnRenamed("input_file_name", "input_file_name_ref").distinct()
        .withColumn(columnName, callUDF("file_size", col("input_file_name_ref")))

      df.join(df2, df("input_file_name") === df2("input_file_name_ref")).drop("input_file_name_ref")
    } else {
      val df1 = df.withColumn("input_file_name", input_file_name())
      val df2 = df1.select("input_file_name").as("input_file_name_ref").distinct()
        .withColumn(columnName, callUDF("file_size", col("input_file_name")))

      df1.join(df2, df("input_file_name") === df2("input_file_name_ref")).drop("input_file_name_ref")
    }

  }

  def tagWithFileOwner(columnName: String)(df: DataFrame)(implicit spark: SparkSession): DataFrame = {

    spark.udf.register("file_owner", (path: String) => Files.getFileAttributeView(Paths.get(path.substring(8)), classOf[FileOwnerAttributeView]).getOwner.getName)
    val schemaFields = df.schema.fields.map(x => x.name)
    if (schemaFields.contains("input_file_name")) {
      val df2 = df.select("input_file_name").withColumnRenamed("input_file_name", "input_file_name_ref").distinct()
        .withColumn(columnName, callUDF("file_owner", col("input_file_name_ref")))


      df.join(df2, df("input_file_name") === df2("input_file_name_ref")).drop("input_file_name_ref")
    } else {
      val df1 = df.withColumn("input_file_name", input_file_name())
      val df2 = df1.select("input_file_name").as("input_file_name_ref").distinct()
        .withColumn(columnName, callUDF("file_owner", col("_y")))

      df1.join(df2, df1("input_file_name") === df2("input_file_name_ref")).drop("input_file_name_ref")
    }


  }

  ///

  def tagWithFileNameHDFS(columnName: String)(df: DataFrame)(implicit spark: SparkSession): DataFrame = {
    spark.udf.register("file_name", (path: String) => path.substring(path.lastIndexOf("/") + 1, path.lastIndexOf(".")))

    val schemaFields = df.schema.fields.map(x => x.name)
    if (schemaFields.contains("input_file_name")) {
      df.withColumn(columnName, callUDF("file_name", col("input_file_name")))
    } else {
      val df2 = df.withColumn("input_file_name", input_file_name())
      df2.withColumn(columnName, callUDF("file_name", col("input_file_name")))
    }
  }




  def tagWithFileSizeHDFS(columnName: String)(df: DataFrame)(implicit spark: SparkSession): DataFrame = {
    spark.udf.register("file_size", (path: String) => {
      val fs = FileSystem.get(new URI(path.substring(0,path.lastIndexOf("/"))), new Configuration())
      val status = fs.listStatus(new Path(path.substring(0,path.lastIndexOf("/"))))
      status.filter(x=>x.getPath.toString.equals(path)).head.getLen
    })
    val schemaFields = df.schema.fields.map(x => x.name)
    if (schemaFields.contains("input_file_name")) {

      val df2 = df.select("input_file_name").withColumnRenamed("input_file_name", "input_file_name_ref").distinct()
        .withColumn(columnName, callUDF("file_size", col("input_file_name_ref")))

      df.join(df2, df("input_file_name") === df2("input_file_name_ref")).drop("input_file_name_ref")
    } else {
      val df1 = df.withColumn("input_file_name", input_file_name())
      val df2 = df1.select("input_file_name").withColumnRenamed("input_file_name", "input_file_name_ref").distinct()
        .withColumn(columnName, callUDF("file_size", col("input_file_name_ref")))

      df1.join(df2, df1("input_file_name") === df2("input_file_name_ref")).drop("input_file_name_ref")
    }

  }

  def tagWithFileOwnerHDFS(columnName: String)(df: DataFrame)(implicit spark: SparkSession): DataFrame = {

    spark.udf.register("file_owner", (path: String) => {
      val fs = FileSystem.get(new URI(path.substring(0,path.lastIndexOf("/"))), new Configuration())
      val status = fs.listStatus(new Path(path.substring(0,path.lastIndexOf("/"))))
      status.filter(x=>x.getPath.toString.equals(path)).head.getOwner
    })
    val schemaFields = df.schema.fields.map(x => x.name)
    if (schemaFields.contains("input_file_name")) {
      val df2 = df.select("input_file_name").withColumnRenamed("input_file_name", "input_file_name_ref").distinct()
        .withColumn(columnName, callUDF("file_owner", col("input_file_name_ref")))


      df.join(df2, df("input_file_name") === df2("input_file_name_ref")).drop("input_file_name_ref")
    } else {
      val df1 = df.withColumn("input_file_name", input_file_name())
      val df2 = df1.select("input_file_name").withColumnRenamed("input_file_name", "input_file_name_ref").distinct()
        .withColumn(columnName, callUDF("file_owner", col("input_file_name_ref")))

      df1.join(df2, df1("input_file_name") === df2("input_file_name_ref")).drop("input_file_name_ref")
    }


  }


  def tagWithFileOwnerGroup(columnName: String)(df: DataFrame)(implicit spark: SparkSession): DataFrame = {

    spark.udf.register("file_owner_group", (path: String) => {
      val fs = FileSystem.get(new URI(path.substring(0,path.lastIndexOf("/"))), new Configuration())
      val status = fs.listStatus(new Path(path.substring(0,path.lastIndexOf("/"))))
      status.filter(x=>x.getPath.toString.equals(path)).head.getGroup
    })
    val schemaFields = df.schema.fields.map(x => x.name)
    if (schemaFields.contains("input_file_name")) {
      val df2 = df.select("input_file_name").withColumnRenamed("input_file_name", "input_file_name_ref").distinct()
        .withColumn(columnName, callUDF("file_owner_group", col("input_file_name_ref")))


      df.join(df2, df("input_file_name") === df2("input_file_name_ref")).drop("input_file_name_ref")
    } else {
      val df1 = df.withColumn("input_file_name", input_file_name())
      val df2 = df1.select("input_file_name").withColumnRenamed("input_file_name", "input_file_name_ref").distinct()
        .withColumn(columnName, callUDF("file_owner_group", col("input_file_name_ref")))

      df1.join(df2, df1("input_file_name") === df2("input_file_name_ref")).drop("input_file_name_ref")
    }


  }

  def tagWithFileModificationTime(columnName: String)(df: DataFrame)(implicit spark: SparkSession): DataFrame = {

    spark.udf.register("file_modification_time", (path: String) => {
      val fs = FileSystem.get(new URI(path.substring(0,path.lastIndexOf("/"))), new Configuration())
      val status = fs.listStatus(new Path(path.substring(0,path.lastIndexOf("/"))))
      status.filter(x=>x.getPath.toString.equals(path)).head.getModificationTime
    })
    val schemaFields = df.schema.fields.map(x => x.name)
    if (schemaFields.contains("input_file_name")) {
      val df2 = df.select("input_file_name").withColumnRenamed("input_file_name", "input_file_name_ref").distinct()
        .withColumn(columnName, callUDF("file_modification_time", col("input_file_name_ref")))


      df.join(df2, df("input_file_name") === df2("input_file_name_ref")).drop("input_file_name_ref")
    } else {
      val df1 = df.withColumn("input_file_name", input_file_name())
      val df2 = df1.select("input_file_name").withColumnRenamed("input_file_name", "input_file_name_ref").distinct()
        .withColumn(columnName, callUDF("file_modification_time", col("input_file_name_ref")))

      df1.join(df2, df1("input_file_name") === df2("input_file_name_ref")).drop("input_file_name_ref")
    }


  }

  def tagWithFilePermission(columnName: String)(df: DataFrame)(implicit spark: SparkSession): DataFrame = {

    spark.udf.register("file_permission", (path: String) => {
      val fs = FileSystem.get(new URI(path.substring(0,path.lastIndexOf("/"))), new Configuration())
      val status = fs.listStatus(new Path(path.substring(0,path.lastIndexOf("/"))))
      status.filter(x=>x.getPath.toString.equals(path)).head.getPermission.toString
    })
    val schemaFields = df.schema.fields.map(x => x.name)
    if (schemaFields.contains("input_file_name")) {
      val df2 = df.select("input_file_name").withColumnRenamed("input_file_name", "input_file_name_ref").distinct()
        .withColumn(columnName, callUDF("file_permission", col("input_file_name_ref")))


      df.join(df2, df("input_file_name") === df2("input_file_name_ref")).drop("input_file_name_ref")
    } else {
      val df1 = df.withColumn("input_file_name", input_file_name())
      val df2 = df1.select("input_file_name").withColumnRenamed("input_file_name", "input_file_name_ref").distinct()
        .withColumn(columnName, callUDF("file_permission", col("input_file_name_ref")))

      df1.join(df2, df1("input_file_name") === df2("input_file_name_ref")).drop("input_file_name_ref")
    }


  }


  def removeUtilityColumn(df: DataFrame)(implicit spark: SparkSession): DataFrame = {
    val schemaFields = df.schema.fields.map(x => x.name)
    if (schemaFields.contains("input_file_name")) {
      df.drop("input_file_name")
    } else {
      df
    }
  }

}
