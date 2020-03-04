package pipeline

import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.annotation.tailrec


object EtlTreeConstructor {

  sealed trait Tree[Etl]

  private case class Leaf(value: Etl) extends Tree[Etl]

  private case class Branch(data: Etl, children: Seq[Tree[Etl]]) extends Tree[Etl]

  private case class Root(data: Etl, children: Seq[Tree[Etl]]) extends Tree[Etl]

  private case class EntryWithDepth(etl: Etl, depth: Int)

  @tailrec
  private def sortNodesByDepth(depth: Int,
                               nodesInDepth: Seq[Etl],
                               nodesByParent: Map[Option[String], Seq[Etl]],
                               acc: Seq[EntryWithDepth]): Seq[EntryWithDepth] = {
    val withDepth = nodesInDepth.map(n => EntryWithDepth(n, depth))
    val calculated = withDepth ++ acc // accumulating in reverse order
    val children = nodesInDepth.flatMap(n => nodesByParent.getOrElse(Some(n.id), Seq.empty))
    if (children.isEmpty)
      calculated
    else
      sortNodesByDepth(depth + 1, children, nodesByParent, calculated)
  }

  private def sortNodesByDepth(nodesInDepth: Seq[Etl],
                               nodesByParent: Map[Option[String], Seq[Etl]]): Seq[EntryWithDepth] =
    sortNodesByDepth(0, nodesInDepth, nodesByParent, Seq.empty)

  @tailrec
  private def buildFromBottom(depth: Int,
                              remaining: Seq[EntryWithDepth],
                              nodesByParent: Map[Option[String], Seq[Etl]],
                              processedNodesById: Map[String, Tree[Etl]],
                              maxDepth: Int): Tree[Etl] = {
    val (nodesOnCurrentDepth, rest) = remaining.span(_.depth == depth)
    val newProcessedNodes = nodesOnCurrentDepth.map { n =>
      val nodeId = n.etl.id
      val children = nodesByParent.getOrElse(Some(nodeId), Seq.empty).flatMap(c => processedNodesById.get(c.id))
      if (depth == maxDepth)
        nodeId -> Leaf(n.etl)
      else if (depth == 0)
        nodeId -> Root(n.etl, children)
      else
        nodeId -> Branch(n.etl, children)
    }.toMap
    if (depth > 0) {
      buildFromBottom(depth - 1, rest, nodesByParent, processedNodesById ++ newProcessedNodes, maxDepth)
    } else {
      newProcessedNodes.values.head.asInstanceOf[Tree[Etl]]
    }
  }

  def construct(nodes: Seq[Etl]): Tree[Etl] = {
    val nodesByParent = nodes.groupBy(_.destinationId)
    val topNodes = nodesByParent.getOrElse(None, Seq.empty)
    val bottomToTop = sortNodesByDepth(topNodes, nodesByParent)
    val maxDepth = bottomToTop.headOption.map(_.depth).getOrElse(0)
    buildFromBottom(maxDepth, bottomToTop, nodesByParent, Map.empty, maxDepth)
  }

  def fold(t: Tree[Etl])(implicit _sparkSession: SparkSession): DataFrame = t match {
    case Leaf(a) => Leaf(a).value.runPipeline(Leaf(a).value.reader.read())
    case Branch(data, seq) =>
      val left = seq.filter(x=>x.asInstanceOf[Leaf].value.id==data.joiner.left).head
      val right = seq.filter(x=>x.asInstanceOf[Leaf].value.id==data.joiner.right).head
      data.runPipeline(data.joiner.join(fold(left), fold(right)))
    case Root(data, seq) =>
      val left = seq.filter(x=>x.asInstanceOf[Leaf].value.id==data.joiner.left).head
      val right = seq.filter(x=>x.asInstanceOf[Leaf].value.id==data.joiner.right).head
      data.runPipeline(data.joiner.join(fold(left), fold(right)))
  }
}



