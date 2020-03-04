package transformations

import TestTraits.SharedSparkSession
import org.scalatest.{FlatSpec, GivenWhenThen, Matchers}

class QueryHandlerSpec extends FlatSpec with GivenWhenThen with Matchers with SharedSparkSession{

  behavior of "QueryHandlerSpec"

  it should "executeScript" in {

    val spark = _sparkSession
    import spark.implicits._
    val expectedDF = Seq(
      ("John", "Stones", 22),
      ("David", "Smith", 64),
      ("Maria", "Gonzalez", 32)
    ).toDF("firstName", "lastName", "age")

    Given("an sql script")

    val sqlScript = "select * from src/test/resources/TestFile.csv"

    val baseDF = Seq(
      ("John", "Stones", 22),
      ("David", "Smith", 64),
      ("Maria", "Gonzalez", -32)
    ).toDF("firstName", "lastName", "age")

    When("executeScript is invoked")
    val resultDf = QueryHandler.executeScript(sqlScript)
    Then("a data frame containing the result of the script should be returned")
    resultDf.collect() should equal(expectedDF.collect())

    ///
   // val s =QueryHandler.executeScript("select * from src/test/resources/TestFile.csv")
  //  s.show()
  }

  it should "execute" in {
    val spark = _sparkSession
    import spark.implicits._
    val expectedDF = Seq(
      "John",
      "David",
      "Maria"
    ).toDF("firstName")

    Given("an sql query and a data frame")

    val sqlQuery = "SELECT firstName FROM tmp2"

    val baseDF = Seq(
      ("John", "Stones", 22),
      ("David", "Smith", 64),
      ("Maria", "Gonzalez", -32)
    ).toDF("firstName", "lastName", "age")

    When("execute is invoked")
    val resultDf = QueryHandler.execute(sqlQuery)(baseDF)
    Then("a data frame containing the result of the query should be returned")
    resultDf.collect() should equal(expectedDF.collect())


  }

}
