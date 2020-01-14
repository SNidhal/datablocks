package pipeline

import destinations.Writer
import org.apache.spark.sql.{DataFrame, SparkSession}
import sources.Reader

case class Etl(reader: Reader,
               transformer: List[DataFrame=> DataFrame],
               writer: Writer,
              ) (implicit sparkSession: SparkSession){

  def runPipeline(df: DataFrame): DataFrame = {
    transformer.foldLeft(df) {
      (finalDf, transformation) => finalDf.transform(transformation)
    }
  }

  def run():Unit = {
    writer.write(runPipeline(reader.read()))
  }

}
