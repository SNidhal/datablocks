package transformations

import org.apache.spark.sql.DataFrame

case class Joiner(left: String, right: String, joinType: String, mapping: Map[String, String]) {

  def join(leftDf: DataFrame, rightDf: DataFrame): DataFrame = {
    //leftDf.show()
    leftDf.join(rightDf, leftDf(mapping.head._1) === rightDf(mapping.head._2), joinType).drop(mapping.head._1)
  }
 // TODO joiner with left right insted of list
 def join2(leftDf: DataFrame, rightDf: DataFrame): DataFrame = {
   leftDf.show()
   leftDf.join(rightDf, Seq(mapping.head._1), joinType).drop(mapping.head._1)
 }

}
