package TestTraits

import java.io.File

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hdfs.MiniDFSCluster

trait HDFSCluster {

  @transient private var hdfsCluster: MiniDFSCluster = _

  def startHDFS(): Unit = {
    println("Starting HDFS Cluster...")
    val baseDir = new File("target\\test\\data", "miniHDFS")
    import org.apache.hadoop.fs.FileUtil
    FileUtil.fullyDelete(baseDir)
    val conf = new Configuration()
    conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath)
    conf.setBoolean("dfs.webhdfs.enabled", true)
    conf.setBoolean("dfs.support.append", true)
    conf.set("dfs.replication", "1")
    val builder = new MiniDFSCluster.Builder(conf)
    hdfsCluster = builder
      .nameNodePort(9000)
      .manageNameDfsDirs(true)
      .manageDataDfsDirs(true)
      .format(true)
      .build()
    hdfsCluster.waitClusterUp()
  }

  def getNameNodeURI: String = "hdfs://localhost:" + hdfsCluster.getNameNodePort

  def shutdownHDFS(): Unit = {
    println("Shutting down HDFS Cluster...")
    hdfsCluster.shutdown()
  }
}
