package pipeline

import destinations.Writer
import org.apache.spark.sql.{DataFrame, SparkSession}
import sources.Reader
import transformations.Joiner

case class Etl(reader: Reader,
               transformer: List[DataFrame=> DataFrame],
               writer: Writer,
               destinationId:Option[String],
               id :String,
               joiner: Joiner
              )(implicit sparkSession: SparkSession){

  def runPipeline(df: DataFrame): DataFrame = {
    transformer.foldLeft(df) {
      (finalDf, transformation) => finalDf.transform(transformation)
    }
  }

  def run():Unit = {
    writer.write(runPipeline(reader.read()),"","")
  }

  def runRoot(df: DataFrame,user:String,application:String):Unit = {
    writer.write(df,user,application)
  }

}
