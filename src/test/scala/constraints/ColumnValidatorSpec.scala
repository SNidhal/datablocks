package constraints

import TestTraits.{SharedSparkSession, SharedSparkSession2}
import org.scalatest.{FlatSpec, GivenWhenThen, Matchers}

class ColumnValidatorSpec extends FlatSpec with GivenWhenThen with Matchers with SharedSparkSession {

  behavior of "ColumnValidatorSpec"

  it should "check Constraints" in {

    val spark = _sparkSession
    import spark.implicits._
    val expectedDF = Seq(
      ("John", "Stones", 22),
      ("David", "Smith", 64)
    ).toDF("firstName", "lastName", "age")

    Given("a list of constraints and a data frame")
    val baseDF = Seq(
      ("John", "Stones", 22),
      ("David", "Smith", 64),
      ("Maria", "Gonzalez", -32)
    ).toDF("firstName", "lastName", "age")
    val constraintList = List(Constraint("age > 0", "DROPMALFORMED"), Constraint("age < 60", "Permissive"))
    When("check is invoked")
    val resultDf = ColumnValidator.check(constraintList)(baseDF)
    Then("a valid data frame based on constraints and according modes should be returned")
    resultDf.collect() should equal(expectedDF.collect())

  }

}
