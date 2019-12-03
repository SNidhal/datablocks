package sources

import TestTraits.SharedSparkSession
import configuration.csv.CsvSourceConfig
import org.scalatest.{FlatSpec, GivenWhenThen, Matchers}

class CsvSourceTest extends FlatSpec with SharedSparkSession with GivenWhenThen with Matchers {

  behavior of "CsvSourceTest"

  it should "read record from a CSV file" in {

    val spark = _sparkSession
    import spark.implicits._
    val referenceDF = Seq(
      ("John", "Stones", "22"),
      ("David", "Smith", "64"),
      ("Maria", "Gonzalez", "32")
    ).toDF

    Given("CsvSourceConfig object")
    val config = CsvSourceConfig("src/test/resources/TestFile.csv", "true", ";", "", "", "", "", "", "", "", "")
    When("loadConfig is invoked")
    val resultDF = CsvSource.read(config)
    Then("a DataFrame of the records should be returned")
    resultDF.collect() should contain theSameElementsAs referenceDF.collect()

  }

}
