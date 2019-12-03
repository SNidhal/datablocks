package configuration.csv

case class CsvSourceConfig(path: String,
                           header: String,
                           delimiter: String,
                           quote: String,
                           escape: String,
                           mode: String,
                           charset: String,
                           inferSchema: String,
                           comment: String,
                           nullValue: String,
                           dateFormat: String
                          ) {

  def equal(config: CsvSourceConfig): Boolean = {
    if (path == config.path &&
      header == config.header &&
      delimiter == config.delimiter &&
      quote == config.quote &&
      escape == config.escape &&
      mode == config.mode &&
      charset == config.charset &&
      inferSchema == config.inferSchema &&
      comment == config.comment &&
      nullValue == config.nullValue &&
      dateFormat == config.dateFormat
    ) true
    else false
  }

  def toMap : Map[String, String] = {
    val parametersList: List[(String, String)] = List(
      "path" -> path,
      "header" -> header,
      "delimiter" -> delimiter,
      "quote" -> quote,
      "escape" -> escape,
      "mode" -> mode,
      "charset" -> charset,
      "inferSchema" -> inferSchema,
      "comment" -> comment,
      "nullValue" -> nullValue,
      "dateFormat" -> dateFormat
    )
    parametersList.filter((x => x._2 != "")).map(x => x._1 -> x._2) toMap
  }

}
