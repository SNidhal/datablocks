package validation

import org.apache.spark.sql.DataFrame

object Validator {
  def dropNull(df: DataFrame): DataFrame = {
    df.na.drop()
  }

  def dropNull(df: DataFrame, minNonNulls: Int): DataFrame = {
    df.na.drop()
  }

  def dropNull(df: DataFrame, minNonNulls: Int, cols: Seq[String]): DataFrame = {
    df.na.drop(minNonNulls, cols)
  }

  def dropNull(df: DataFrame, cols: Seq[String]): DataFrame = {
    df.na.drop(cols)
  }

  def dropNull(df: DataFrame, how: String): DataFrame = {
    df.na.drop(how)
  }

  def dropNull(df: DataFrame, how: String, cols: Seq[String]): DataFrame = {
    df.na.drop(how, cols)
  }

}
