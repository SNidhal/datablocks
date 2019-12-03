import TestTraits.HDFSCluster
import configuration.tracer.{TracerConfig, TracerConfigParser}
import hadoopIO.HDFSHelper
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FlatSpec, Suite}
import services.tracer.CopyHiveTableService

class HiveTracedCopySpec extends FlatSpec with Suite with HDFSCluster with BeforeAndAfterEach with BeforeAndAfterAll {

  behavior of "TracedCopy"

  var clusterURI: String = _
  var rootDirectory: String = _
  var hdfsHelper: HDFSHelper = _
  implicit var spark: SparkSession = _
  var config: TracerConfig = _


  override def beforeAll(): Unit = {
    startHDFS()
    clusterURI = getNameNodeURI
    rootDirectory = getNameNodeURI + "/test"
    hdfsHelper = HDFSHelper(clusterURI)

    hdfsHelper.hdfs.mkdirs(new Path(clusterURI + "/test/source"))
    hdfsHelper.hdfs.mkdirs(new Path(clusterURI + "/test/destination"))
    hdfsHelper.hdfs.mkdirs(new Path(clusterURI + "/test/trace"))


    hdfsHelper.hdfs.create(new Path(rootDirectory + "/trace/trace.csv"))
    hdfsHelper.hdfs.create(new Path(rootDirectory + "/config.xml"))

    spark = SparkSession
      .builder()
      .appName("SparkApp")
      .config("spark.master", "local[*]")
      .enableHiveSupport()
      .getOrCreate()

    val configPath = rootDirectory + "/config.xml"
    val configSource = scala.io.Source.fromFile("src/test/resources/fichierArguments.xml")
    val lines = configSource.mkString
    hdfsHelper.writeInto(lines, configPath, hdfsHelper.hdfs)


    val stream = hdfsHelper.hdfs.open(new Path(configPath))

    val configFileContent: String = scala.io.Source.fromInputStream(stream).takeWhile(_ != null).mkString
    config = TracerConfigParser.loadConfig(configFileContent)

   // spark.sql("CREATE TABLE IF NOT EXISTS TRACETABLE(File  STRING, Source STRING , Destination STRING , State STRING , Cheksum STRING , Message STRING , Size STRING ,LastModifiedDate STRING)")

    super.beforeAll()
  }

  println("out")

  override def afterAll(): Unit = {
    shutdownHDFS()
    spark.close()
  }


  override def beforeEach() {
    spark.sql("TRUNCATE TABLE TRACETABLE")
    super.beforeEach()
  }


  it should "make traced copy with  an empty destination and trace " in {

    hdfsHelper.hdfs.create(new Path(rootDirectory + "/source/HDFSTestFile2.txt"))
    hdfsHelper.hdfs.create(new Path(rootDirectory + "/source/HDFSTestFile3.txt"))

    hdfsHelper.writeInto("HDFSTestFile2 content", rootDirectory + "/source/HDFSTestFile2.txt", hdfsHelper.hdfs)
    hdfsHelper.writeInto("HDFSTestFile3 content", rootDirectory + "/source/HDFSTestFile3.txt", hdfsHelper.hdfs)

    CopyHiveTableService(clusterURI).tabletracedCopy(config.sourceDirectory, config.destinationDirectory)
    spark.sql("SELECT * FROM TRACETABLE").show()


  }
  it should "make traced copy when file checksum already exist in destination " in {

    hdfsHelper.hdfs.create(new Path(rootDirectory + "/source/HDFSTestFile4.txt"))
    hdfsHelper.hdfs.create(new Path(rootDirectory + "/destination/HDFSTestFile44.txt"))

    hdfsHelper.writeInto("HDFSTestFile4 content", rootDirectory + "/source/HDFSTestFile4.txt", hdfsHelper.hdfs)
    hdfsHelper.writeInto("HDFSTestFile4 content", rootDirectory + "/destination/HDFSTestFile44.txt", hdfsHelper.hdfs)

    CopyHiveTableService(clusterURI).tabletracedCopy(config.sourceDirectory, config.destinationDirectory)
    spark.sql("SELECT * FROM TRACETABLE").show()

  }

  it should "make traced copy when file name already exist in destination " in {

    hdfsHelper.hdfs.create(new Path(rootDirectory + "/source/HDFSTestFile5.txt"))
    hdfsHelper.hdfs.create(new Path(rootDirectory + "/destination/HDFSTestFile5.txt"))

    hdfsHelper.writeInto("HDFSTestFile5 content", rootDirectory + "/source/HDFSTestFile5.txt", hdfsHelper.hdfs)
    hdfsHelper.writeInto("HDFSTestFile55 content", rootDirectory + "/destination/HDFSTestFile5.txt", hdfsHelper.hdfs)

    CopyHiveTableService(clusterURI).tabletracedCopy(config.sourceDirectory, config.destinationDirectory)
    spark.sql("SELECT * FROM TRACETABLE").show()

  }

}
