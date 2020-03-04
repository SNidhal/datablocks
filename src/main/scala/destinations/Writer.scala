package destinations

import org.apache.spark.sql.DataFrame

case class Writer(path: String, mode: String, partitionedBy: String, compression: String
                  , format: String, options: Map[String, String],tableName:String) {

  override def toString: String = path+"-"+mode+"-"+partitionedBy+"-"+compression+"-"+format+"-"+options+"-"+tableName

  def write(dataFrame: DataFrame) {
    format match {
      case "table" => dataFrame
        .write
        .partitionBy(partitionedBy)
        .options(options)
        .mode(mode)
        .saveAsTable(tableName)
      case _ => dataFrame
        .write
        .format(format)
        .partitionBy(partitionedBy)
        .options(options)
        .mode(mode)
        .save(path)
    }

  }
}
