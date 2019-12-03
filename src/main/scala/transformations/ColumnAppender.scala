package transformations

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{callUDF, input_file_name}

object ColumnAppender {
  def tagWithFileName(df: DataFrame, columnName: String)(implicit spark: SparkSession): DataFrame = {
    spark.udf.register("file_name", (path: String) => path.substring(path.lastIndexOf("/") + 1, path.lastIndexOf(".")))
    df.withColumn(columnName, callUDF("file_name", input_file_name()))
  }
}
