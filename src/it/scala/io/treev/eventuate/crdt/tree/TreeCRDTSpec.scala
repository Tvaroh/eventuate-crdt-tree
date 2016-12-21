package io.treev.eventuate.crdt.tree

import com.rbmhtechnology.eventuate.VectorTime
import com.rbmhtechnology.eventuate.crdt.ORSet
import io.treev.eventuate.crdt.tree.model.internal.{Edge, ServiceInfo}
import io.treev.eventuate.crdt.tree.model.{Tree, TreeConfig}
import org.scalatest.OptionValues._
import org.scalatest.{Matchers, WordSpec}

class TreeCRDTSpec extends WordSpec with Matchers {

  "TreeCRDT" must {

    "value" must {

      "return single root element tree if no nodes were added" in {
        treeCRDT.value should be (Tree(treeConfig.rootNodeId, treeConfig.rootPayload))
      }

      "return tree with all elements" in {
        val (node1Id, payload1) = node(1)
        val (node2Id, payload2) = node(2)
        val (node3Id, payload3) = node(3)

        treeCRDT(
          edge(treeConfig.rootNodeId, node1Id, payload1),
          edge(treeConfig.rootNodeId, node2Id, payload2),
          edge(node2Id, node3Id, payload3)
        ).value should be {
          Tree(
            treeConfig.rootNodeId, treeConfig.rootPayload,
            Set(
              Tree(node1Id, payload1),
              Tree(
                node2Id, payload2,
                Set(
                  Tree(node3Id, payload3)
                )
              )
            )
          )
        }
      }

    }

    "value(nodeId)" must {

      "return sub-tree starting from specified node id" in {
        val (node1Id, payload1) = node(1)
        val (node2Id, payload2) = node(2)
        val (node3Id, payload3) = node(3)

        treeCRDT(
          edge(treeConfig.rootNodeId, node1Id, payload1),
          edge(treeConfig.rootNodeId, node2Id, payload2),
          edge(node2Id, node3Id, payload3)
        ).value(node2Id).value should be (Tree(node2Id, payload2, Set(Tree(node3Id, payload3))))
      }

      "return root tree with all elements if node id is root node id" in {
        val (node1Id, payload1) = node(1)
        val (node2Id, payload2) = node(2)
        val (node3Id, payload3) = node(3)

        treeCRDT(
          edge(treeConfig.rootNodeId, node1Id, payload1),
          edge(treeConfig.rootNodeId, node2Id, payload2),
          edge(node2Id, node3Id, payload3)
        ).value(treeConfig.rootNodeId).value should be {
          Tree(
            treeConfig.rootNodeId, treeConfig.rootPayload,
            Set(
              Tree(node1Id, payload1),
              Tree(
                node2Id, payload2,
                Set(
                  Tree(node3Id, payload3)
                )
              )
            )
          )
        }
      }

      "return None on missing node id" in {
        val (node1Id, payload1) = node(1)
        val (node2Id, payload2) = node(2)
        val (node3Id, payload3) = node(3)

        treeCRDT(
          edge(treeConfig.rootNodeId, node1Id, payload1),
          edge(treeConfig.rootNodeId, node2Id, payload2),
          edge(node2Id, node3Id, payload3)
        ).value("wrong") should be (None)
      }

    }

  }

  private type Id = String
  private type Payload = String

  private implicit val treeConfig: TreeConfig[String, String] =
    TreeConfig[String, String]("root", "root's payload")

  private def treeCRDT: TreeCRDT[Payload, Id] = TreeCRDT[Payload, Id]

  private def treeCRDT(edges: Edge[Payload, Id]*): TreeCRDT[Payload, Id] = {
    val edgesORSet =
      edges.foldLeft(ORSet[Edge[Payload, Id]])((orSet, edge) => orSet.add(edge, edge.serviceInfo.vectorTimestamp))
    val edgesByNodeId =
      edges.map(edge => (edge.nodeId, edge)).toMap
    val edgesByParentId =
      edges.groupBy(_.parentId).mapValues(_.toSet)

    TreeCRDT[Payload, Id](edgesORSet, edgesByNodeId, edgesByParentId)
  }

  private def edge(parentId: Id,
                   nodeId: Id,
                   payload: Payload,
                   serviceInfo: ServiceInfo = mkServiceInfo()): Edge[Payload, Id] = {
    Edge(nodeId, parentId, payload, serviceInfo)
  }

  private def node(number: Int): (String, String) =
    (s"child$number", s"child$number's payload")

  private def mkVectorTimestamp(): VectorTime = VectorTime(Map.empty[String, Long])

  private def mkServiceInfo(): ServiceInfo =
    ServiceInfo(mkVectorTimestamp(), "L1", System.currentTimeMillis())

}
