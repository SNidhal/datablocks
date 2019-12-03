package destinations

import org.apache.spark.sql.DataFrame

object ParquetDestination {
  def write(dataFrame: DataFrame, partitionColumn: String, destinationDirectory: String) {
    dataFrame
      .write
      .partitionBy(partitionColumn)
      .parquet(destinationDirectory)
  }
}
