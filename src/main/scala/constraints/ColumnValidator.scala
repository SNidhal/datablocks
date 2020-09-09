package constraints

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._


object ColumnValidator {
  def check(constraintList: List[Constraint])(df: DataFrame)(implicit spark: SparkSession): DataFrame = {

    val dfWithConstraint = df.withColumn("broken_constraint", lit(""))
    constraintList.foldLeft(dfWithConstraint) {
      (mediumDf, constraint) =>
        constraint.mode match {
          case "Permissive" => mediumDf
            .withColumn("broken_constraint", when(expr(constraint.condition) === true, mediumDf("broken_constraint"))
            .otherwise(expr("concat(broken_constraint,'" + constraint.condition + "',';')")))
          case "DROPMALFORMED" => mediumDf
            .withColumn("broken_constraint", when(expr(constraint.condition) === true, mediumDf("broken_constraint"))
            .otherwise(expr("concat(broken_constraint,'" + constraint.condition + "',';')")))
            .filter(col("broken_constraint") === "")
        }
    }.drop("broken_constraint")
  }


}
