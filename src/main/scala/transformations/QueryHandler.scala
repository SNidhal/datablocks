package transformations

import org.apache.spark.sql.{DataFrame, SparkSession}

object QueryHandler {
  def execute(query: String)(df: DataFrame)(implicit spark: SparkSession): DataFrame = {
    val lowCaseQuery = query.toLowerCase
    val fromIndex = lowCaseQuery.indexOf("from")
    val tableName = query.substring(fromIndex + 4).trim.split(" ").head
    df.createOrReplaceTempView(tableName)
    spark.sql(query)
  }

  def executeScript(query: String)(implicit spark: SparkSession): DataFrame = {
    val parsedQuery = query.split(" ").filter(x => x.contains(".json") ^ x.contains(".csv")).map(x => {
      val df = spark.read.format("csv")
        .option("header", "true")
        .option("sep", ";")
        .option("inferSchema", "true")
        .load(x)
      val implicitTableName = "TBL" + x.hashCode.abs
      df.createOrReplaceTempView(implicitTableName)
      x -> implicitTableName
    }).toMap.foldLeft(query) {
      (s, r) => s.replaceAll(r._1, r._2)
    }
    spark.sql(parsedQuery)

  }

}
