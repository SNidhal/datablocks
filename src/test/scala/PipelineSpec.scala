import configuration.ColumnAppender.ColumnAppenderConfigParser
import configuration.csv.CsvSourceConfigParser
import configuration.parquet.ParquetDestinationConfigParser
import destinations.ParquetDestination
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.FlatSpec
import pipeline.Pipeline
import sources.CsvSource
import transformations.ColumnAppender

class PipelineSpec extends FlatSpec {
  val configPath = scala.io.Source.fromFile("src/test/resources/config.xml").mkString

  implicit val _sparkSession: SparkSession = SparkSession
    .builder()
    .appName("test_Session")
    .master("local[*]")
    .getOrCreate()

  var df: DataFrame = _
  Pipeline.getSteps(configPath).foreach(x => x match {
    case "ParquetDestination" => ParquetDestination.write(df, ParquetDestinationConfigParser.loadConfig(configPath).partitionColumn,
      ParquetDestinationConfigParser.loadConfig(configPath).destinationDirectory)
    case "ColumnAppender" => df = ColumnAppender.tagWithFileName(df, ColumnAppenderConfigParser.loadConfig(configPath).ColumnName)
    case "TracedCopy" => println("trace")
    case "CsvSource" => df = CsvSource.read(CsvSourceConfigParser.loadConfig(configPath))
  })

  df.show()

}
