package configuration.tracer

import scala.xml.XML

object TracerConfigParser {


  def loadConfig(configPath: String ): TracerConfig = {

    val argFile = XML.loadString(configPath)

    val sourceDirectory=  (argFile \ "sourceDirectory").text
    val destinationDirectory = (argFile \ "destinationDirectory").text
    val tracePath = (argFile \ "tracePath").text
    val traceFileName = (argFile \ "traceFileName").text
    val traceMethod = (argFile \ "traceMethod").text



    TracerConfig(
      sourceDirectory,
      destinationDirectory,
      tracePath,
      traceFileName,
      traceMethod)
  }
}
