package validation

import TestTraits.SharedSparkSession2
import org.apache.spark.sql.DataFrame
import org.scalatest.{BeforeAndAfterAll, FlatSpec, GivenWhenThen, Matchers}

class NullHandlerTest extends FlatSpec with GivenWhenThen with Matchers with SharedSparkSession2 with BeforeAndAfterAll {

  behavior of "NullHandlerTest"

  import _sparkSession.implicits._

  val dropNullReferenceDataFrame: DataFrame = Seq(
    ("John", "Stones", 22),
    ("Maria", "Gonzalez", 32)).toDF()

  val fillNullReferenceDataFrame: DataFrame = Seq(
    ("John", "Stones", 22),
    ("David", "N/A", 64),
    ("Maria", "Gonzalez", 32)).toDF()

  val replaceNullReferenceDataFrame: DataFrame = Seq(
    ("John", "Stones", 22),
    ("David", "Smith", 100),
    ("Maria", "Gonzalez", 32)).toDF()

  it should "drop columns that contain null with no specifications" in {

    Given("a DataFrame")

    val sourceDataFrame = Seq(
      ("John", "Stones", 22),
      ("David", null, 64),
      ("Maria", "Gonzalez", 32)).toDF()

    When("dropNull is invoked")

    val resultDf = NullHandler.dropNull(null, null)(sourceDataFrame)

    Then("a dataFrame not containing null is returned ")

    resultDf.collect() should equal(dropNullReferenceDataFrame.collect())
  }


  it should "drop columns that contain null given how to drop" in {

    Given("a DataFrame and how to drop")

    val sourceDataFrame = Seq(
      ("John", "Stones", 22),
      ("David", null, 64),
      ("Maria", "Gonzalez", 32)).toDF()

    val how = "any"

    When("dropNull is invoked")

    val resultDf = NullHandler.dropNull(how, null)(sourceDataFrame)

    Then("a dataFrame not containing null is returned")

    resultDf.collect() should equal(dropNullReferenceDataFrame.collect())
  }

  it should "drop columns that contain null in given columns" in {

    Given("a DataFrame and how to drop")

    val sourceDataFrame = Seq(
      ("John", "Stones", 22),
      ("David", null, 64),
      ("Maria", "Gonzalez", 32)).toDF("firstName", "lastName", "age")

    val cols = Seq("lastName")

    When("dropNull is invoked")

    val resultDf = NullHandler.dropNull(null, cols)(sourceDataFrame)

    Then("a dataFrame not containing null in the given columns is returned")

    resultDf.collect() should equal(dropNullReferenceDataFrame.collect())
  }

  it should "drop columns that contain null given how to drop and the columns" in {

    Given("a DataFrame and how to drop")

    val sourceDataFrame = Seq(
      ("John", "Stones", 22),
      ("David", null, 64),
      ("Maria", "Gonzalez", 32)).toDF("firstName", "lastName", "age")

    val how = "any"

    val cols = Seq("lastName")

    When("dropNull is invoked")

    val resultDf = NullHandler.dropNull(how, cols)(sourceDataFrame)

    Then("a dataFrame not containing null is returned ")

    resultDf.collect() should equal(dropNullReferenceDataFrame.collect())
  }

  it should "fill columns that contain null with a given value" in {

    Given("a dataFrame, a value and the type of the value")


    val sourceDataFrame = Seq(
      ("John", "Stones", 22),
      ("David", null, 64),
      ("Maria", "Gonzalez", 32)).toDF("firstName", "lastName", "age")

    val value = "N/A"
    val valueType = "String"

    When("csvToDf is invoked")

    val resultDf = NullHandler.fillNull(value, valueType, null, null)(sourceDataFrame)
    Then("a dataFrame is returned ")
    resultDf.collect() should equal(fillNullReferenceDataFrame.collect())
  }

  it should "fill columns that contain null with a given filling mapping" in {
    Given("a dataFrame, a value and the type of the value")


    val sourceDataFrame = Seq(
      ("John", "Stones", 22),
      ("David", null, 64),
      ("Maria", "Gonzalez", 32)).toDF("firstName", "lastName", "age")

    val valueMap = Map("lastName" -> "N/A")

    When("csvToDf is invoked")

    val resultDf = NullHandler.fillNull(null, null, null, valueMap)(sourceDataFrame)
    Then("a dataFrame is returned ")
    resultDf.collect() should equal(fillNullReferenceDataFrame.collect())
  }

  it should "fill columns that contain null with a value and a given list of columns" in {
    Given("a dataFrame, a value and a list of columns")


    val sourceDataFrame = Seq(
      ("John", "Stones", 22),
      ("David", null, 64),
      ("Maria", "Gonzalez", 32)).toDF("firstName", "lastName", "age")

    val valueMap = Map("lastName" -> "N/A")

    When("csvToDf is invoked")

    val resultDf = NullHandler.fillNull(null, null, null, valueMap)(sourceDataFrame)
    Then("a dataFrame is returned ")
    resultDf.collect() should equal(fillNullReferenceDataFrame.collect())
  }

  it should "replaceNull" in {

    Given("a dataFrame and a list of columns and replacement mapping")

    val sourceDataFrame = Seq(
      ("John", "Stones", 22),
      ("David", "Smith", 64),
      ("Maria", "Gonzalez", 32)).toDF("firstName", "lastName", "age")

    val cols = Seq("age")

    val replacement = Map("64" -> "100")

    When("csvToDf is invoked")
    val resultDf = NullHandler.replaceNull(cols, replacement)(sourceDataFrame)
    Then("a dataFrame is returned ")

    resultDf.collect() should equal(replaceNullReferenceDataFrame.collect())
  }

}
