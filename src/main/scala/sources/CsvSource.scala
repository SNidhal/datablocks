package sources

import configuration.csv.CsvSourceConfig
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

object CsvSource {
  def read(config: CsvSourceConfig, schema: StructType = null)(implicit spark: SparkSession): DataFrame = {
    config.toMap.foreach(x=>println(x._1+"--"+x._2))
    spark.read.format("csv")
      .options(config.toMap)
      .schema(schema)
      .load(config.path)
  }
}
