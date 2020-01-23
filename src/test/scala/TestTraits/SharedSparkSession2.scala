package TestTraits

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, Suite}

trait SharedSparkSession2 extends BeforeAndAfterAll {
  this: Suite =>

  lazy val _sparkSession: SparkSession = SparkSession
    .builder()
    .appName("test_Session")
    .master("local[*]")
    .getOrCreate()


  override def afterAll(): Unit = {
    if (_sparkSession != null) {
      _sparkSession.stop()
    }
    super.afterAll()
  }

  def sparkSession: SparkSession = _sparkSession

}
