package configuration.validator.nullHandler

import scala.xml.XML

object NullValidatorConfigParser {
  def loadConfig(configPath: String) = {
   //TODO complete the methods
    val argFile = XML.loadString(configPath)

    val mode = ((argFile \\ "NullValidator") \ "@mode").text
    val columns = ((argFile \\ "NullValidator") \ "columns" \ "column" ).map(x=>x.text).toList
    val valueMap = ((argFile \\ "NullValidator") \ "map" \ "value" ).map(x=>(x\"@from").text->x.text).toMap
    valueMap.foreach(x=>println("---"+x+"---"))
    println(mode)

  }

}
