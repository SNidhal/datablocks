import TestTraits.SharedSparkSession
import org.scalatest.FunSuite
import pipeline.{EtlTreeConstructor, Configuration}

class EtlSpec extends FunSuite with SharedSparkSession {


  test("building and running ETL pipeline using configuration file") {
    val configPath = "src/test/resources/config2.yaml"
    val EtlList = Configuration.buildEtlList(configPath)
    val EtlTree = EtlTreeConstructor.construct(EtlList)
    EtlTreeConstructor.fold(EtlTree).show()

    Thread.sleep(10000000)
  }


}
