package pipeline


import scala.xml.{Elem, NodeSeq, XML}

object Pipeline {

  def getChildren(ns :NodeSeq) :NodeSeq = ns.flatMap( { _ match {
    case e: Elem => e.child
    case _ => NodeSeq.Empty
  } })

  def getSteps(configPath: String): List[String] = {
    val argFile = XML.loadString(configPath)

    val path = argFile \ "nodes"
    getChildren(path).map(x=>x.label).filterNot(x=>x.equals("#PCDATA")).toList
  }
}

