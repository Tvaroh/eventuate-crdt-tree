package io.treev.eventuate.crdt.tree

import com.rbmhtechnology.eventuate.VectorTime
import com.rbmhtechnology.eventuate.crdt.ORSet
import io.treev.eventuate.crdt.tree.model.exception.{NodeAlreadyExistsException, ParentNodeNotExistsException}
import io.treev.eventuate.crdt.tree.model.internal.{Edge, ServiceInfo}
import io.treev.eventuate.crdt.tree.model.op.CreateChildNodeOpPrepared
import io.treev.eventuate.crdt.tree.model.{ConflictResolver, MappingPolicy, Tree, TreeConfig}
import org.scalatest.OptionValues._
import org.scalatest.{Assertion, Matchers, WordSpec}

import scala.util.{Failure, Success}

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

    "prepareCreateChildNode" must {

      "return valid CreateChildNodeOpPrepared instance on success" in {
        val (nodeId, payload) = node(1)
        treeCRDT.prepareCreateChildNode(treeConfig.rootNodeId, nodeId, payload) should be {
          Success(Some(CreateChildNodeOpPrepared(treeConfig.rootNodeId, nodeId, payload)))
        }
      }

      "fail with ParentNodeNotExistsException if parent node doesn't exist" in {
        val parentId = "wrong"
        val (nodeId, payload) = node(1)
        treeCRDT.prepareCreateChildNode(parentId, nodeId, payload) should be {
          Failure(ParentNodeNotExistsException(parentId))
        }
      }

      "fail with NodeAlreadyExistsException if node with same id already exists" in {
        val (nodeId, payload) = node(1)
        treeCRDT(edge(treeConfig.rootNodeId, nodeId, payload))
          .prepareCreateChildNode(treeConfig.rootNodeId, nodeId, payload) should be {
          Failure(NodeAlreadyExistsException(nodeId))
        }
      }

    }

    "createChildNode" must {

      "add child node to an empty tree" in {
        val (nodeId, payload) = node(1)
        treeCRDT
          .createChildNode(CreateChildNodeOpPrepared(treeConfig.rootNodeId, nodeId, payload), mkServiceInfo())
          .value should be (Tree(treeConfig.rootNodeId, treeConfig.rootPayload, Set(Tree(nodeId, payload))))
      }

      "add child node to a single child tree" in {
        val (node1Id, payload1) = node(1)
        val (node2Id, payload2) = node(2)

        treeCRDT(
          edge(treeConfig.rootNodeId, node1Id, payload1)
        ).createChildNode(CreateChildNodeOpPrepared(node1Id, node2Id, payload2), mkServiceInfo())
          .value should be {
            Tree(
              treeConfig.rootNodeId, treeConfig.rootPayload,
              Set(
                Tree(
                  node1Id, payload1,
                  Set(
                    Tree(node2Id, payload2)
                  )
                )
              )
            )
          }
      }

      "add child node to a root node that already has a child" in {
        val (node1Id, payload1) = node(1)
        val (node2Id, payload2) = node(2)

        treeCRDT(
          edge(treeConfig.rootNodeId, node1Id, payload1)
        ).createChildNode(CreateChildNodeOpPrepared(treeConfig.rootNodeId, node2Id, payload2), mkServiceInfo())
          .value should be {
          Tree(
            treeConfig.rootNodeId, treeConfig.rootPayload,
            Set(
              Tree(node1Id, payload1),
              Tree(node2Id, payload2)
            )
          )
        }
      }

      "add child node to a root's child node that already has a child" in {
        val (node1Id, payload1) = node(1)
        val (node2Id, payload2) = node(2)
        val (node3Id, payload3) = node(3)

        treeCRDT(
          edge(treeConfig.rootNodeId, node1Id, payload1),
          edge(node1Id, node2Id, payload2)
        ).createChildNode(CreateChildNodeOpPrepared(node2Id, node3Id, payload3), mkServiceInfo())
          .value should be {
          Tree(
            treeConfig.rootNodeId, treeConfig.rootPayload,
            Set(
              Tree(
                node1Id, payload1,
                Set(
                  Tree(
                    node2Id, payload2,
                    Set(
                      Tree(node3Id, payload3)
                    )
                  )
                )
              )
            )
          )
        }
      }

      "return original tree if same node under same parent and equal payload is added concurrently" in {
        val (node1Id, payload1) = node(1)
        val (node2Id, payload2) = node(2)

        treeCRDT(
          edge(treeConfig.rootNodeId, node1Id, payload1),
          edge(node1Id, node2Id, payload2)
        ).createChildNode(CreateChildNodeOpPrepared(node2Id, node2Id, payload2), mkServiceInfo())
          .value should be {
          Tree(
            treeConfig.rootNodeId, treeConfig.rootPayload,
            Set(
              Tree(
                node1Id, payload1,
                Set(
                  Tree(node2Id, payload2)
                )
              )
            )
          )
        }
      }

      "remove node if same node is added concurrently under same parent when Zero mapping policy is used" in {
        val (node1Id, payload1) = node(1)
        val (node2Id, payload2) = node(2)
        val (_, payload3) = node(3)

        val config: TreeConfig[String, String] =
          treeConfig.copy(policies = treeConfig.policies.copy(sameParentMappingPolicy = MappingPolicy.Zero))

        treeCRDT(
          edge(treeConfig.rootNodeId, node1Id, payload1),
          edge(node1Id, node2Id, payload2)
        )(config)
          .createChildNode(CreateChildNodeOpPrepared(node2Id, node2Id, payload3), mkServiceInfo())
          .value should be {
          Tree(treeConfig.rootNodeId, treeConfig.rootPayload, Set(Tree(node1Id, payload1)))
        }
      }

      "resolve conflict if same node is added concurrently under same parent when Custom mapping policy is used" in {
        val (node1Id, payload1) = node(1)
        val (node2Id, payload2) = node(2)
        val (_, payload3) = node(3)

        def test(expectedPayload: Payload): Assertion = {
          implicit val resolver = ConflictResolver.instance[Payload]((p1, _) => p1 == expectedPayload)

          val config: TreeConfig[String, String] =
            treeConfig.copy(policies = treeConfig.policies.copy(sameParentMappingPolicy = MappingPolicy.Custom()))

          treeCRDT(
            edge(treeConfig.rootNodeId, node1Id, payload1),
            edge(node1Id, node2Id, payload2)
          )(config)
            .createChildNode(CreateChildNodeOpPrepared(node2Id, node2Id, payload3), mkServiceInfo())
            .value should be {
            Tree(
              treeConfig.rootNodeId, treeConfig.rootPayload,
              Set(
                Tree(
                  node1Id, payload1,
                  Set(Tree(node2Id, expectedPayload))
                )
              )
            )
          }
        }

        test(expectedPayload = payload2) // keep
        test(expectedPayload = payload3) // replace
      }

    }

  }

  private type Id = String
  private type Payload = String

  private implicit val treeConfig: TreeConfig[String, String] =
    TreeConfig[String, String]("root", "root's payload")

  private def treeCRDT: TreeCRDT[Payload, Id] = TreeCRDT[Payload, Id]

  private def treeCRDT(edges: Edge[Payload, Id]*)
                      (implicit treeConfig: TreeConfig[Payload, Id]): TreeCRDT[Payload, Id] = {
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
