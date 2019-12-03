package configuration.ColumnAppender

import scala.xml.XML

object ColumnAppenderConfigParser {
  def loadConfig(configPath: String): ColumnAppenderConfig = {

    val argFile = XML.loadString(configPath)

    val ColumnName = ((argFile \\ "ColumnAppender") \ "ColumnName").text
    val rule = ((argFile \\ "ColumnAppender") \ "rule").text


    ColumnAppenderConfig(
      ColumnName,
      rule
    )
  }

}
