package sources

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}


case class Reader(path: String, format: String, schema: StructType, options: Map[String, String]) {
  def read()(implicit spark: SparkSession): DataFrame = {
    spark.read.format(format)
      .options(options)
      .schema(schema)
      .load(path)
  }
}

