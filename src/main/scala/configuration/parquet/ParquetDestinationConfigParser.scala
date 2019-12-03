package configuration.parquet

import scala.xml.XML

object ParquetDestinationConfigParser {
  def loadConfig(configPath: String): ParquetDestinationConfig = {

    val argFile = XML.loadString(configPath)

    val partitionColumn = ((argFile \\ "ParquetDestination") \ "partitionColumn").text
    val destinationDirectory = ((argFile \\ "ParquetDestination") \ "destinationDirectory").text


    ParquetDestinationConfig(
      partitionColumn,
      destinationDirectory
    )
  }

}
