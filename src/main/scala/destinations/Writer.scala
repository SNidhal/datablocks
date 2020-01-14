package destinations

import org.apache.spark.sql.DataFrame

case class Writer(path: String, mode: String, partitionedBy: String, compression: String
                  , format: String, options: Map[String, String]) {
  def write(dataFrame: DataFrame) {
    dataFrame
      .write
      .format(format)
      .partitionBy(partitionedBy)
      .options(options)
      .mode(mode)
      .save(path)
  }
}
