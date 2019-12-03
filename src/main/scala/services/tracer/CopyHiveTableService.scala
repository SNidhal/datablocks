package services.tracer

import java.io.File
import java.text.SimpleDateFormat

import hadoopIO.HDFSHelper
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession

case class CopyHiveTableService(uri: String) {

  val hdfsHelper = HDFSHelper(uri)

  def tabletracedCopy(sourceDirectory: String, destinationDirectory: String)(implicit spark: SparkSession) = {

    initHiveTable()

    val fileList = hdfsHelper.listFilesFrom(sourceDirectory)
    // TODO: useless rdd
    val fileRdd = spark.sparkContext.parallelize(fileList)
    val tracedFilesChecksum = getTracedFilesChecksum()

    import spark.implicits._

    val dataFramey = fileRdd.map(file => {
      tracedMove(file, sourceDirectory, destinationDirectory, tracedFilesChecksum)
    }).toDF()

    dataFramey.write.mode("append").insertInto("TRACETABLE")
  }


  def listFilesFrom(directory: String): List[File] = {
    val directoryFile: File = new File(directory)
    if (directoryFile.exists && directoryFile.isDirectory) {
      directoryFile.listFiles.filter(_.isFile).toList
    } else {
      List[File]()
    }
  }


  def initHiveTable()(implicit spark: SparkSession) = {
//    spark.sql("CREATE TABLE IF NOT EXISTS TRACETABLE(File  STRING, Source STRING , Destination STRING , State STRING , Cheksum STRING , Message STRING , Size STRING ,LastModifiedDate STRING)")
  }


  private def computeHash(path: String): String = {
    val hdfsHelper = new HDFSHelper(uri)
    println("hash " + path + " hdfs " + hdfsHelper)
    val stream = hdfsHelper.hdfs.open(new Path(path))
    org.apache.hadoop.io.MD5Hash.digest(stream).toString
  }


  def getTracedFilesChecksum()(implicit spark: SparkSession): Array[String] = {
    spark.sqlContext.sql("SELECT Cheksum FROM TRACETABLE").collect()
      .map(x => x.getString(0))
  }


  def tracedMove(file: (String, String, Long, Long), sourceDirectory: String, destinationDirectory: String, tracedFilesChecksum: Array[String]) = {
    val hdfsHelper = new HDFSHelper(uri)
    val hash = computeHash(sourceDirectory + file._2) //check du nveau fichier  // APPEL
    val exists = hdfsHelper.hdfs.exists(new Path(destinationDirectory + file._2)) // true ou false existe dans le rep destination

    val length = file._4
    val sdateformat = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss")
    val lastmodifieddate = sdateformat.format(file._3)
    var traceValue: trace = null
    (exists, tracedFilesChecksum.contains(hash)) match {
      case (false, false) => {
        hdfsHelper.move(
          sourceDirectory + file._2,
          destinationDirectory + file._2)
        traceValue = trace(file._2, file._1, destinationDirectory + file._2, "MOVE SUCCESS: File's Name  dosen't exist yet !", hash, "Cheksum dosen't exist yet !", length.toString, lastmodifieddate)
      }

      case (true, false) => {
        traceValue = trace(file._2, file._1, destinationDirectory + file._2, "MOVE FAILED: File's Name Already Exists", hash, "", length.toString, lastmodifieddate)
      }

      case (false, true) => {
        traceValue = trace(file._2, file._1, destinationDirectory + file._2, "MOVE FAILED", hash, "Cheksum Exists Already !", length.toString, lastmodifieddate)
      }

      case (true, true) => {
        traceValue = trace(file._2, file._1, destinationDirectory + file._2, "MOVE FAILED", hash, "Cheksum AND Name Exists Already !", length.toString, lastmodifieddate)
      }

    }
    traceValue
  }


}
