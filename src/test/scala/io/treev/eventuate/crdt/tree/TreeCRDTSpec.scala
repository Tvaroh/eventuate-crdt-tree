package io.treev.eventuate.crdt.tree

import com.rbmhtechnology.eventuate.VectorTime
import com.rbmhtechnology.eventuate.crdt.ORSet
import io.treev.eventuate.crdt.tree.model.exception.{NodeAlreadyExistsException, NodeNotExistsException, ParentNodeNotExistsException}
import io.treev.eventuate.crdt.tree.model.internal.{Edge, ServiceInfo}
import io.treev.eventuate.crdt.tree.model.op.{CreateChildNodeOpPrepared, DeleteSubTreeOpPrepared}
import io.treev.eventuate.crdt.tree.model._
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

      "fail with NodeAlreadyExistsException when adding node with same id as root" in {
        val (nodeId, payload) = node(1)
        treeCRDT(edge(treeConfig.rootNodeId, nodeId, payload))
          .prepareCreateChildNode(treeConfig.rootNodeId, treeConfig.rootNodeId, payload) should be {
          Failure(NodeAlreadyExistsException(treeConfig.rootNodeId))
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

        treeCRDT(edge(treeConfig.rootNodeId, node1Id, payload1))
          .createChildNode(CreateChildNodeOpPrepared(node1Id, node2Id, payload2), mkServiceInfo())
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

        treeCRDT(edge(treeConfig.rootNodeId, node1Id, payload1))
          .createChildNode(CreateChildNodeOpPrepared(treeConfig.rootNodeId, node2Id, payload2), mkServiceInfo())
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

        val config: TreeConfig[Payload, Id] =
          treeConfig.copy(policies = treeConfig.policies.copy(mappingPolicy = MappingPolicy.Zero))

        treeCRDT(
          edge(treeConfig.rootNodeId, node1Id, payload1),
          edge(node1Id, node2Id, payload2)
        )(config)
          .createChildNode(CreateChildNodeOpPrepared(node2Id, node2Id, payload3), mkServiceInfo())
          .value should be {
          Tree(treeConfig.rootNodeId, treeConfig.rootPayload, Set(Tree(node1Id, payload1)))
        }
      }

      "resolve conflict by comparing vector timestamps when LastWriteWins mapping policy is used" in {
        val (node1Id, payload1) = node(1)
        val (node2Id, payload2) = node(2)
        val (_, payload3) = node(3)

        val config: TreeConfig[Payload, Id] =
          treeConfig.copy(policies = treeConfig.policies.copy(mappingPolicy = MappingPolicy.LastWriteWins))

        def test(existingLogicalTime: Long, logicalTime: Long, expectedPayload: Payload): Assertion = {
          treeCRDT(
            edge(treeConfig.rootNodeId, node1Id, payload1),
            edge(node1Id, node2Id, payload2, mkServiceInfo(mkVectorTimestamp(logicalTime = existingLogicalTime)))
          )(config)
            .createChildNode(
              CreateChildNodeOpPrepared(node2Id, node2Id, payload3),
              mkServiceInfo(mkVectorTimestamp(logicalTime = logicalTime))
            )
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

        test(existingLogicalTime = 2, logicalTime = 1, expectedPayload = payload2) // keep
        test(existingLogicalTime = 1, logicalTime = 2, expectedPayload = payload3) // replace
      }

      "resolve conflict by comparing emitter ids if vector timestamps are equal when LastWriteWins mapping policy is used" in {
        val (node1Id, payload1) = node(1)
        val (node2Id, payload2) = node(2)
        val (_, payload3) = node(3)

        val config: TreeConfig[Payload, Id] =
          treeConfig.copy(policies = treeConfig.policies.copy(mappingPolicy = MappingPolicy.LastWriteWins))

        def test(existingEmitterId: String, emitterId: String, expectedPayload: Payload): Assertion = {
          treeCRDT(
            edge(treeConfig.rootNodeId, node1Id, payload1),
            edge(node1Id, node2Id, payload2, mkServiceInfo(emitterId = existingEmitterId))
          )(config)
            .createChildNode(
              CreateChildNodeOpPrepared(node2Id, node2Id, payload3), mkServiceInfo(emitterId = emitterId)
            )
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

        test(existingEmitterId = "A", emitterId = "B", expectedPayload = payload2) // keep
        test(existingEmitterId = "B", emitterId = "A", expectedPayload = payload3) // replace
      }

      "resolve conflict by comparing non-equal system timestamps when LastWriteWins mapping policy is used" in {
        val (node1Id, payload1) = node(1)
        val (node2Id, payload2) = node(2)
        val (_, payload3) = node(3)

        val config: TreeConfig[Payload, Id] =
          treeConfig.copy(policies = treeConfig.policies.copy(mappingPolicy = MappingPolicy.LastWriteWins))

        def test(existingSystemTimestamp: Long, systemTimestamp: Long, expectedPayload: Payload): Assertion = {
          treeCRDT(
            edge(treeConfig.rootNodeId, node1Id, payload1),
            edge(
              node1Id, node2Id, payload2,
              mkServiceInfo(vectorTimestamp = mkVectorTimestamp("P1", 2L), systemTimestamp = existingSystemTimestamp)
            )
          )(config)
            .createChildNode(
              CreateChildNodeOpPrepared(node2Id, node2Id, payload3),
              mkServiceInfo(vectorTimestamp = mkVectorTimestamp("P2", 2L), systemTimestamp = systemTimestamp)
            )
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

        test(existingSystemTimestamp = 1000L, systemTimestamp = 999L, expectedPayload = payload2) // keep
        test(existingSystemTimestamp = 999L, systemTimestamp = 1000L, expectedPayload = payload3) // replace
      }

      "resolve conflict by comparing emitter ids if system timestamps are equal when LastWriteWins mapping policy is used" in {
        val (node1Id, payload1) = node(1)
        val (node2Id, payload2) = node(2)
        val (_, payload3) = node(3)

        val config: TreeConfig[Payload, Id] =
          treeConfig.copy(policies = treeConfig.policies.copy(mappingPolicy = MappingPolicy.LastWriteWins))

        val systemTimestamp = System.currentTimeMillis()

        def test(existingEmitterId: String, emitterId: String, expectedPayload: Payload): Assertion = {
          treeCRDT(
            edge(treeConfig.rootNodeId, node1Id, payload1),
            edge(
              node1Id, node2Id, payload2,
              mkServiceInfo(mkVectorTimestamp("P1", 2L), existingEmitterId, systemTimestamp)
            )
          )(config)
            .createChildNode(
              CreateChildNodeOpPrepared(node2Id, node2Id, payload3),
              mkServiceInfo(mkVectorTimestamp("P2", 2L), emitterId, systemTimestamp)
            )
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

        test(existingEmitterId = "A", emitterId = "B", expectedPayload = payload2) // keep
        test(existingEmitterId = "B", emitterId = "A", expectedPayload = payload3) // replace
      }

      "resolve conflict if same node is added concurrently under same parent when Custom mapping policy is used" in {
        val (node1Id, payload1) = node(1)
        val (node2Id, payload2) = node(2)
        val (_, payload3) = node(3)

        def test(expectedPayload: Payload): Assertion = {
          implicit val resolver = ConflictResolver.instance[Payload]((p1, _) => p1 == expectedPayload)

          val config: TreeConfig[Payload, Id] =
            treeConfig.copy(policies = treeConfig.policies.copy(mappingPolicy = MappingPolicy.Custom()))

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

      "skip node if parent node is deleted concurrently when Skip connection policy is used" in {
        val (node1Id, payload1) = node(1)
        val (node2Id, _) = node(2)
        val (node3Id, payload3) = node(3)

        val config: TreeConfig[Payload, Id] =
          treeConfig.copy(policies = treeConfig.policies.copy(connectionPolicy = ConnectionPolicy.Skip))

        treeCRDT(edge(treeConfig.rootNodeId, node1Id, payload1))(config)
          .createChildNode(CreateChildNodeOpPrepared(node2Id, node3Id, payload3), mkServiceInfo())
          .value should be {
          Tree(treeConfig.rootNodeId, treeConfig.rootPayload, Set(Tree(node1Id, payload1)))
        }
      }

      "put node under root if parent node is deleted concurrently when Root connection policy is used" in {
        val (node1Id, payload1) = node(1)
        val (node2Id, _) = node(2)
        val (node3Id, payload3) = node(3)

        val config: TreeConfig[Payload, Id] =
          treeConfig.copy(policies = treeConfig.policies.copy(connectionPolicy = ConnectionPolicy.Root))

        treeCRDT(edge(treeConfig.rootNodeId, node1Id, payload1))(config)
          .createChildNode(CreateChildNodeOpPrepared(node2Id, node3Id, payload3), mkServiceInfo())
          .value should be {
          Tree(treeConfig.rootNodeId, treeConfig.rootPayload, Set(Tree(node1Id, payload1), Tree(node3Id, payload3)))
        }
      }

    }

    "prepareDeleteSubTree" must {

      "gather latest timestamps and node ids of all affected nodes" in {
        val (node1Id, payload1) = node(1)
        val (node2Id, payload2) = node(2)
        val ((node3Id, payload3), logicalTime3) = (node(3), mkVectorTimestamp(logicalTime = 30L))
        val ((node4Id, payload4), logicalTime4) = (node(4), mkVectorTimestamp(logicalTime = 40L))
        val ((node5Id, payload5), logicalTime5) = (node(5), mkVectorTimestamp(logicalTime = 50L))

        treeCRDT(
          edge(treeConfig.rootNodeId, node1Id, payload1),
          edge(node1Id, node2Id, payload2),
          edge(node1Id, node3Id, payload3, mkServiceInfo(logicalTime3)),
          edge(node3Id, node4Id, payload4, mkServiceInfo(logicalTime4)),
          edge(node3Id, node5Id, payload5, mkServiceInfo(logicalTime5))
        ).prepareDeleteSubTree(node3Id) should be {
          Success(Some(
            DeleteSubTreeOpPrepared(
              node3Id, node1Id,
              Set(node3Id, node4Id, node5Id),
              Set(logicalTime3, logicalTime4, logicalTime5)
            )
          ))
        }
      }

      "fail with NodeNotExistsException if node id doesn't exist" in {
        val (node1Id, payload1) = node(1)
        val (node2Id, _) = node(2)

        treeCRDT(edge(treeConfig.rootNodeId, node1Id, payload1))
          .prepareDeleteSubTree(node2Id) should be (Failure(NodeNotExistsException(node2Id)))
      }

      "fail with NodeNotExistsException when deleting root node" in {
        val (node1Id, payload1) = node(1)
        val (node2Id, payload2) = node(2)

        treeCRDT(
          edge(treeConfig.rootNodeId, node1Id, payload1),
          edge(node1Id, node2Id, payload2)
        ).prepareDeleteSubTree(treeConfig.rootNodeId) should be (Failure(NodeNotExistsException(treeConfig.rootNodeId)))
      }

    }

    "deleteSubTree" must {

      "remove affected nodes from internal ORSet of edges and lookup maps" in {
        val (node1Id, payload1) = node(1)
        val (node2Id, payload2) = node(2)
        val ((node3Id, payload3), logicalTime3) = (node(3), mkVectorTimestamp(logicalTime = 30L))
        val ((node4Id, payload4), logicalTime4) = (node(4), mkVectorTimestamp(logicalTime = 40L))
        val ((node5Id, payload5), logicalTime5) = (node(5), mkVectorTimestamp(logicalTime = 50L))

        val result = treeCRDT(
          edge(treeConfig.rootNodeId, node1Id, payload1),
          edge(node1Id, node2Id, payload2),
          edge(node1Id, node3Id, payload3, mkServiceInfo(logicalTime3)),
          edge(node3Id, node4Id, payload4, mkServiceInfo(logicalTime4)),
          edge(node3Id, node5Id, payload5, mkServiceInfo(logicalTime5))
        ).deleteSubTree(
          DeleteSubTreeOpPrepared(
            node3Id, node1Id,
            Set(node3Id, node4Id, node5Id),
            Set(logicalTime3, logicalTime4, logicalTime5)
          )
        )

        val deletedNodeIds = Set(node3Id, node4Id, node5Id)

        result.edgesServiceInfo.keys should not contain allElementsOf (deletedNodeIds)
        result.edgesByNodeId.keys should not contain allElementsOf (deletedNodeIds)
        result.edgesByParentId.keys should not contain allElementsOf (deletedNodeIds)

        result.value should be {
          Tree(
            treeConfig.rootNodeId, treeConfig.rootPayload,
            Set(Tree(node1Id, payload1, Set(Tree(node2Id, payload2))))
          )
        }
      }

    }

  }

  private type Id = String
  private type Payload = String

  private implicit val treeConfig: TreeConfig[Payload, Id] =
    TreeConfig[Payload, Id]("root", "root's payload")

  private def treeCRDT: TreeCRDT[Payload, Id] = TreeCRDT[Payload, Id]

  private def treeCRDT(edges: (Edge[Payload, Id], ServiceInfo)*)
                      (implicit treeConfig: TreeConfig[Payload, Id]): TreeCRDT[Payload, Id] = {
    val edgesORSet =
      edges.foldLeft(ORSet[Edge[Payload, Id]]) {
        case (orSet, (edge, serviceInfo)) => orSet.add(edge, serviceInfo.vectorTimestamp)
      }
    val edgesServiceInfo =
      edges.map({ case (edge, serviceInfo) => (edge.nodeId, serviceInfo) }).toMap
    val edgesByNodeId =
      edges.map(_._1).map(edge => (edge.nodeId, edge)).toMap
    val edgesByParentId =
      edges.map(_._1).groupBy(_.parentId).mapValues(_.map(edge => (edge.nodeId, edge)).toMap)

    TreeCRDT[Payload, Id](edgesORSet, edgesServiceInfo, edgesByNodeId, edgesByParentId)
  }

  private def edge(parentId: Id,
                   nodeId: Id,
                   payload: Payload,
                   serviceInfo: ServiceInfo = mkServiceInfo()): (Edge[Payload, Id], ServiceInfo) =
    (Edge(nodeId, parentId, payload), serviceInfo)

  private def node(number: Int): (Id, Payload) =
    (s"child$number", s"child$number's payload")

  private def mkVectorTimestamp(processId: String = "P1", logicalTime: Long = 1): VectorTime =
    VectorTime(Map(processId -> logicalTime))

  private def mkServiceInfo(vectorTimestamp: VectorTime = mkVectorTimestamp(),
                            emitterId: String = "L1",
                            systemTimestamp: Long = System.currentTimeMillis()): ServiceInfo =
    ServiceInfo(vectorTimestamp, emitterId, systemTimestamp)

}
