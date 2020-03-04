package transformations

import TestTraits.SharedSparkSession
import org.scalatest.{FlatSpec, GivenWhenThen, Matchers}

class ColumnAppenderSpec extends FlatSpec with GivenWhenThen with Matchers with SharedSparkSession {

  behavior of "ColumnAppenderSpec"

  it should "deriveColumn" in {
    val spark = _sparkSession
    import spark.implicits._
    val expectedDF = Seq(
      ("John", "Stones", 22, 32),
      ("David", "Smith", 64, 74),
      ("Maria", "Gonzalez", 32, 42)
    ).toDF("firstName", "lastName", "age", "agePlusTen")

    Given("a column name,a data frame and an expression")
    val baseDF = Seq(
      ("John", "Stones", 22),
      ("David", "Smith", 64),
      ("Maria", "Gonzalez", 32)
    ).toDF("firstName", "lastName", "age")

    val expression = "age + 10"
    val columnName = "agePlusTen"

    When("deriveColumn is invoked")
    val resultDf = ColumnAppender.deriveColumn(columnName, expression)(baseDF)
    Then("a data frame containing the derived column should be returned")
    resultDf.collect() should contain theSameElementsAs expectedDF.collect()
  }

  it should "run a function Over a Window" in {
    val spark = _sparkSession
    import spark.implicits._
    val expectedDF = Seq(
      ("John", "Stones", 22, "AXA", 2),
      ("David", "Smith", 64, "AXA", 1),
      ("Maria", "Gonzalez", 32, "Databricks", 1)
    ).toDF("firstName", "lastName", "age", "company", "ageRankByCompany")

    Given("a column name, partition column, order column, an order and a function")

    val columnName = "ageRankByCompany"
    val partitionColumn = "company"
    val orderColumn = "age"
    val order = "desc"
    val function = "rank"

    val baseDF = Seq(
      ("John", "Stones", 22, "AXA"),
      ("David", "Smith", 64, "AXA"),
      ("Maria", "Gonzalez", 32, "Databricks")
    ).toDF("firstName", "lastName", "age", "company")

    When("executeScript is invoked")
    val resultDf = ColumnAppender.runOverWindow(columnName, partitionColumn, orderColumn,
      order, function, null, null)(baseDF)
    Then("a data frame containing the function result column should be returned")
    resultDf.collect() should contain theSameElementsAs expectedDF.collect()

  }

}
