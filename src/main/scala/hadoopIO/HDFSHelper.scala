package hadoopIO

import java.net.URI
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

case class HDFSHelper(uri: String) extends Serializable {
  @transient val conf = new Configuration()
  conf.set("fs.defaultFSi", uri)
  @transient val hdfs: FileSystem = FileSystem.get(new URI(uri), conf)


  def writeInto(data: String, filePath: String, fs: FileSystem): Unit = {
    val stream = fs.open(new Path(filePath))

    def readLines = scala.io.Source.fromInputStream(stream)

    val content: String = readLines.takeWhile(_ != null).mkString
    readLines.close()
    val os = fs.create(new Path(filePath))
    os.write(content.getBytes)
    os.write(data.getBytes)
    os.close()
  }

  def listFilesFrom(path: String): Array[(String, String, Long, Long)] = {
    val conf = new Configuration()
    conf.set("fs.defaultFSi", uri)
    val fs: FileSystem = FileSystem.get(new URI(uri), conf)
    val files = fs.listStatus(new Path(path))
    files.filterNot(x => x.isDirectory).map(x => (x.getPath.toString, x.getPath.getName, x.getModificationTime, x.getLen))
  }

  def ls(path: String): List[String] = {
    val status = hdfs.listStatus(new Path(path))
    status.map(x => x.getPath.toString).toList
  }

  def isFileEmpty(file: String): Boolean = {
    val stream = hdfs.open(new Path(file))
    if (stream.read().equals(-1)) true
    else false
  }

  def move(srcPath: String, dstPath: String): Unit = {
    org.apache.hadoop.fs.FileUtil.copy(
      hdfs,
      new Path(srcPath),
      hdfs,
      new Path(dstPath),
      true,
      conf
    )
  }

}
