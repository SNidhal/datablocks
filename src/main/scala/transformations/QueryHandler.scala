package transformations

import org.apache.spark.sql.{DataFrame, SparkSession}

object QueryHandler {
  def execute(query: String)(df: DataFrame)(implicit spark: SparkSession): DataFrame = {
    df.createOrReplaceTempView("TMP")
    spark.sql(query)
  }
}
