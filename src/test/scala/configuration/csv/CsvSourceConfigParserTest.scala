package configuration.csv

import org.scalatest.{FlatSpec, GivenWhenThen}

class CsvSourceConfigParserTest extends FlatSpec with GivenWhenThen {

  behavior of "CsvSourceConfigParserTest"

  val resultCsvConfig = CsvSourceConfig("test path ","false","","","","","","","","","")

  it should "loadConfig" in {
    Given("the path of the configuration file")
    val configPath = scala.io.Source.fromFile("src/test/resources/config.xml").mkString
    When("loadConfig is invoked")
    val CsvSourceConfig = CsvSourceConfigParser.loadConfig(configPath)
    Then("a CsvSourceConfig should be returned")
    assert(CsvSourceConfig.equal(resultCsvConfig))

  }

}
