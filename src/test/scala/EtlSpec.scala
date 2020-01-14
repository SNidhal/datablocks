import TestTraits.SharedSparkSession
import org.scalatest.FunSuite
import pipeline.Pipeline

class EtlSpec extends FunSuite with SharedSparkSession {


  test("building and running ETL pipeline using configuration file") {
    val configPath = "src/test/resources/config.yaml"
    Pipeline.buildEtl(configPath).run()
  }


}
