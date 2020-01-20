package pipeline

import TestTraits.SharedSparkSession
import destinations.Writer
import org.apache.spark.sql.DataFrame
import org.scalatest.{FlatSpec, GivenWhenThen, Matchers}
import sources.Reader

class PipelineTest extends FlatSpec with GivenWhenThen with Matchers with SharedSparkSession {

  behavior of "PipelineTest"

  it should "build a Reader" in {
    Given("the path of the configuration file")
    val configPath = "src/test/resources/ConfigTest/ReaderConfig.yaml"
    When("buildReader is invoked")
    val reader: Reader = Pipeline.buildReader(configPath)
    Then("a Reader object should be returned ")

    val spark = _sparkSession
    import spark.implicits._

    val expectedDF = Seq(
      ("John","Stones",22),
      ("David","Smith",64),
      ("Maria","Gonzalez",32)
    ).toDF("firstName", "lastName","age")

    reader.read().collect() should contain theSameElementsAs expectedDF.collect()
  }

  it should "build a Transformer" in {
    Given("the path of the configuration file")
    val configPath = "src/test/resources/ConfigTest/TransformerConfig.yaml"
    When("buildTransformer is invoked")
    val transformer : List[DataFrame=>DataFrame] = Pipeline.buildTransformer(configPath)
    Then("a list of transformation  should be returned ")

  }

  it should "build a Writer" in {
    Given("the path of the configuration file")
    val configPath = "src/test/resources/ConfigTest/WriterConfig.yaml"
    When("buildWriter is invoked")
    val writer : Writer = Pipeline.buildWriter(configPath)
    Then("a list of transformation  should be returned ")

  }

}
