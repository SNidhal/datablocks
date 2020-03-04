package pipeline

import TestTraits.SharedSparkSession
import destinations.Writer
import org.apache.spark.sql.DataFrame
import org.scalatest.{FlatSpec, GivenWhenThen, Matchers}
import org.yaml.snakeyaml.Yaml
import sources.Reader

class ConfigurationSpec extends FlatSpec with GivenWhenThen with Matchers with SharedSparkSession {

  behavior of "PipelineTest"

  it should "build a CSV Reader" in {
    Given("a map of the reader configuration")
    val configSource = scala.io.Source.fromFile("src/test/resources/ConfigTest/CsvReaderConfig.yaml")
    val configContent = configSource.mkString
    val yml = new Yaml()
    val readerConfigMap = yml.load(configContent).asInstanceOf[java.util.Map[String, Object]].get("reader")
    When("buildReader is invoked")
    val reader: Reader = Configuration.buildReader(readerConfigMap)
    Then("a Reader object should be returned ")

    val spark = _sparkSession
    import spark.implicits._

    val expectedDF = Seq(
      ("John", "Stones", 22),
      ("David", "Smith", 64),
      ("Maria", "Gonzalez", 32)
    ).toDF("firstName", "lastName", "age")

    reader.read().collect() should contain theSameElementsAs expectedDF.collect()
  }


  it should "build a Json Reader" in {
    Given("the path of the configuration file")
    val configSource = scala.io.Source.fromFile("src/test/resources/ConfigTest/JsonReaderConfig.yaml")
    val configContent = configSource.mkString
    val yml = new Yaml()
    val readerConfigMap = yml.load(configContent).asInstanceOf[java.util.Map[String, Object]].get("reader")
    When("buildReader is invoked")
    val reader: Reader = Configuration.buildReader(readerConfigMap)
    Then("a Reader object should be returned ")

    val spark = _sparkSession
    import spark.implicits._

    val expectedDF = Seq(
      ("John", "Stones", 22),
      ("David", "Smith", 64),
      ("Maria", "Gonzalez", 32)
    ).toDF("firstName", "lastName", "age")
    reader.read().collect() should contain theSameElementsAs expectedDF.collect()
  }

  it should "build a Transformer" in {

    val spark = _sparkSession
    import spark.implicits._

    val expectedDF = Seq(
      ("John", "Stones", 100),
      ("David", "Smith", 64),
      ("Maria", "Gonzalez", 32)
    ).toDF("firstName", "lastName", "age")

    Given("the path of the configuration file")
    val configSource = scala.io.Source.fromFile("src/test/resources/ConfigTest/TransformerConfig.yaml")
    val configContent = configSource.mkString
    val yml = new Yaml()
    val transformerConfig = yml.load(configContent).asInstanceOf[java.util.Map[String, Object]].get("transformer")
    When("buildTransformer is invoked")
    val transformer: List[DataFrame => DataFrame] = Configuration.buildTransformer(transformerConfig)
    Then("a list of transformation  should be returned ")

    val baseDF = Seq(
      ("John", "Stones", 22),
      ("David", "Smith", 64),
      ("Maria", "Gonzalez", 32)
    ).toDF("firstName", "lastName", "age")
    val resultDf = transformer.foldLeft(baseDF) {
      (finalDf, transformation) => finalDf.transform(transformation)
    }

    resultDf.collect() should contain theSameElementsAs expectedDF.collect()

  }

  it should "build a Writer" in {

    val expectedWriter= Writer("C:/parquet/test","append","fileName","snappy","parquet",Map(),null)

    Given("the path of the configuration file")
    val configSource = scala.io.Source.fromFile("src/test/resources/ConfigTest/WriterConfig.yaml")
    val configContent = configSource.mkString
    val yml = new Yaml()
    val writerConfig = yml.load(configContent).asInstanceOf[java.util.Map[String, Object]].get("writer")
    When("buildWriter is invoked")
    val writer: Writer = Configuration.buildWriter(writerConfig)
    Then("a list of transformation  should be returned ")
     writer should equal(expectedWriter)
  }

}
