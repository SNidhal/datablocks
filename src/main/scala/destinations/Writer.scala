package destinations

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import transformations.ColumnAppender


case class Writer(path: String, mode: String, partitionedBy: List[String], compression: String
                  , format: String, options: Map[String, String], tableName: String, processMode: String,
                  triggerType: String = "", triggerValue: String = "") {

  override def toString: String = path + "-" + mode + "-" + partitionedBy + "-" + compression + "-" + format + "-" + options + "-" + tableName + "-" + processMode


  def write(dataFrame: DataFrame, user: String, application: String)(implicit sparkSession: SparkSession): Unit = {

    val now = Calendar.getInstance().getTime
    val minuteFormat = new SimpleDateFormat("YYYYMMddHH")
    val nowFormat = new SimpleDateFormat("YYYY-MM-dd HH:mm:ss")
    val date = minuteFormat.format(now)
    val timeStamp = nowFormat.format(now)

    val dataFrameWithId = dataFrame.withColumn("applicationId", lit(sparkSession.sparkContext.applicationId))
      .withColumn("date", lit(date))
    val partitionColumns = "date" :: partitionedBy

    processMode match {
      case "batch" =>
        (path
          , mode
          , partitionColumns
          , format
          , options
          , tableName) match {
          case (path: String, mode: String, partitionColumns: List[String], format: String, options: Map[String, String], null) =>
            dataFrameWithId
              .write
              .format(format)
              .partitionBy(partitionColumns: _*)
              .options(options)
              .mode(mode)
              .save(path)
          case (path: String, mode: String, null, format: String, options: Map[String, String], null) =>
            dataFrameWithId
              .write
              .format(format)
              .options(options)
              .mode(mode)
              .save(path)
          case (path: String, mode: String, partitionColumns: List[String], format: String, null, null) =>
            dataFrameWithId
              .write
              .format(format)
              .partitionBy(partitionColumns: _*)
              .mode(mode)
              .save(path)
          case (path: String, null, partitionColumns: List[String], format: String, null, null) =>
            dataFrameWithId
              .write
              .format(format)
              .partitionBy(partitionColumns: _*)
              .save(path)
          case (path: String, mode: String, null, format: String, null, null) =>
            dataFrameWithId
              .write
              .format(format)
              .mode(mode)
              .save(path)
          case (path: String, null, null, format: String, null, null) =>
            dataFrameWithId
              .write
              .format(format)
              .save(path)
          case (null, mode: String, partitionColumns: List[String], null, options: Map[String, String], tableName: String) =>
            dataFrameWithId
              .write
              .partitionBy(partitionColumns: _*) // partitionBy take varargs as parameter
              .options(options)
              .mode(mode)
              .saveAsTable(tableName)
          case (null, mode: String, null, null, options: Map[String, String], tableName: String) =>
            dataFrameWithId
              .write
              .options(options)
              .mode(mode)
              .saveAsTable(tableName)
          case (null, mode: String, partitionColumns: List[String], null, null, tableName: String) =>
            dataFrameWithId
              .write
              .partitionBy(partitionColumns: _*)
              .mode(mode)
              .saveAsTable(tableName)
          case (null, null, partitionColumns: List[String], null, null, tableName: String) =>
            dataFrameWithId
              .write
              .partitionBy(partitionColumns: _*)
              .saveAsTable(tableName)
          case (null, mode: String, null, null, null, tableName: String) =>
            dataFrameWithId
              .write
              .mode(mode)
              .saveAsTable(tableName)
          case (null, null, null, null, null, tableName: String) =>
            dataFrameWithId
              .write
              .saveAsTable(tableName)


        }


        sparkSession.sql("INSERT INTO TABLE writeAudit  VALUES ('" + sparkSession.sparkContext.applicationId + "', " +
          "'" + format + "' , " + "'" + partitionColumns.mkString(",") + "' , '" + timeStamp + "' , '" + path + "' , '" + user + "' ," +
          " '" + application + "' )")

        sparkSession.sql("select * from writeAudit").show()
      case "stream" =>
        (path
          , mode
          , partitionedBy
          , format
          , options
          , triggerType
          , triggerValue) match {

          case (path: String, mode: String, partitionColumns: List[String], format: String, options: Map[String, String],
          null, null) =>
            if (format.equals("hive")) {
              val query = dataFrame.writeStream
                .foreachBatch((df, batchId) => {
                  df.write.
                    format("parquet").
                    mode(mode).
                    partitionBy(partitionColumns: _*).
                    saveAsTable(path)
                }).start()
              query.awaitTermination()
            } else {
              val query = dataFrameWithId.writeStream
                .outputMode(mode)
                .format(format)
                .options(options)
                .partitionBy(partitionColumns: _*)
                .start(path)
              query.awaitTermination()
            }

          case (path: String, mode: String, partitionColumns: List[String], format: String, options: Map[String, String],
          triggerType: String, triggerValue: String) =>
            val trigger: Trigger = triggerType match {
              case "ContinuousTrigger" => Trigger.Continuous(triggerValue)
              case "OneTimeTrigger" => Trigger.Once()
              case "ProcessingTime" => Trigger.ProcessingTime(triggerValue)

            }
            if (format.equals("hive")) {
              val query = dataFrame.writeStream
                .foreachBatch((df, batchId) => {
                  df.write.
                    format("parquet").
                    mode(mode).
                    partitionBy(partitionColumns: _*).
                    saveAsTable(path)
                }).trigger(trigger).start()
              query.awaitTermination()
            } else {
              val query = dataFrame.writeStream
                .outputMode(mode)
                .format(format)
                .options(options)
                .trigger(trigger)
                .partitionBy(partitionColumns: _*)
                .start(path)
              query.awaitTermination()
            }
        }


    }
  }
}
