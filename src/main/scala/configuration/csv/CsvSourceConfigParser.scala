package configuration.csv

import scala.xml.XML

object CsvSourceConfigParser {
  def loadConfig(configPath: String): CsvSourceConfig = {

    val argFile = XML.loadString(configPath)

    val path = ((argFile \\ "CsvSource") \ "@path").text
    val header = ((argFile \\ "CsvSource") \ "header").text
    val delimiter = ((argFile \\ "CsvSource") \ "delimiter").text
    val quote = ((argFile \\ "CsvSource") \ "quote").text
    val escape = ((argFile \\ "CsvSource") \ "escape").text
    val mode = ((argFile \\ "CsvSource") \ "mode").text
    val charset = ((argFile \\ "CsvSource") \ "charset").text
    val inferSchema = ((argFile \\ "CsvSource") \ "inferSchema").text
    val comment = ((argFile \\ "CsvSource") \ "comment").text
    val nullValue = ((argFile \\ "CsvSource") \ "nullValue").text
    val dateFormat = ((argFile \\ "CsvSource") \ "dateFormat").text

println(header)


    CsvSourceConfig(
      path,
      header,
      delimiter,
      quote,
      escape,
      mode,
      charset,
      inferSchema,
      comment,
      nullValue,
      dateFormat
    )
  }

  }
