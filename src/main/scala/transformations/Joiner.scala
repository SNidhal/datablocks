package transformations

import org.apache.spark.sql.DataFrame

case class Joiner(left: String, right: String, joinType: String, mapping: Map[String, String]) {

  def join(leftDf: DataFrame, rightDf: DataFrame): DataFrame = {
    leftDf.show()
    leftDf.join(rightDf, leftDf(mapping.head._1) === rightDf(mapping.head._1), joinType)
  }

}
