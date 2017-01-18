package io.treev.eventuate.crdt.tree

import com.rbmhtechnology.eventuate.VectorTime
import io.treev.eventuate.crdt.tree.model.Tree
import io.treev.eventuate.crdt.tree.model.internal._

object TestHelpers {

  type Id = String
  type Payload = String

  implicit class TreeExtensions(tree: Tree[Payload, Id]) {

    def normalize(implicit idOrdering: Ordering[Id]): Tree[Payload, Id] =
      sortById(tree)

    private implicit def treeOrdering(implicit idOrdering: Ordering[Id]): Ordering[Tree[Payload, Id]] =
      Ordering.by(_.nodeId)

    private def sortById(tree: Tree[Payload, Id])
                        (implicit treeOrdering: Ordering[Tree[Payload, Id]]): Tree[Payload, Id] =
      tree.copy(children = tree.children.sorted.map(sortById))

  }

  def mkVectorTimestamp(processId: String = "P1", logicalTime: Long = 1): VectorTime =
    VectorTime(Map(processId -> logicalTime))

  def mkEdgeMetainfo(vectorTimestamp: VectorTime = mkVectorTimestamp(),
                             emitterId: String = "L1",
                             systemTimestamp: Long = System.currentTimeMillis()): EdgeMetainfo =
    EdgeMetainfo(vectorTimestamp, emitterId, systemTimestamp)

  def node(number: Int): (Id, Payload) =
    (s"child$number", s"child$number's payload")

}
