package io.treev.eventuate.crdt.tree

import com.rbmhtechnology.eventuate.VectorTime
import com.rbmhtechnology.eventuate.crdt.ORSet
import io.treev.eventuate.crdt.tree.TestHelpers._
import io.treev.eventuate.crdt.tree.model._
import io.treev.eventuate.crdt.tree.model.exception._
import io.treev.eventuate.crdt.tree.model.internal._
import io.treev.eventuate.crdt.tree.model.op._
import org.scalatest.{Assertion, Matchers, WordSpec}

import scala.util.{Failure, Success}

class UnorderedTreeSpec extends WordSpec with Matchers {

  "UnorderedTree" must {

    "value" must {

      "return single root element tree if no nodes were added" in {
        treeCRDT.value.normalize should be (Tree(treeConfig.rootNodeId, treeConfig.rootPayload).normalize)
      }

      "return tree with all elements" in {
        val (node1Id, payload1) = node(1)
        val (node2Id, payload2) = node(2)
        val (node3Id, payload3) = node(3)

        treeCRDT(
          edge(treeConfig.rootNodeId, node1Id, payload1),
          edge(treeConfig.rootNodeId, node2Id, payload2),
          edge(node2Id, node3Id, payload3)
        ).value.normalize should be {
          Tree(
            treeConfig.rootNodeId, treeConfig.rootPayload,
            Seq(
              Tree(node1Id, payload1),
              Tree(
                node2Id, payload2,
                Seq(
                  Tree(node3Id, payload3)
                )
              )
            )
          ).normalize
        }
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
        val (node1Id, payload1) = node(1)
        val (node2Id, payload2) = node(2)
        treeCRDT(
          edge(treeConfig.rootNodeId, node1Id, payload1),
          edge(node1Id, node2Id, payload2)
        ).prepareCreateChildNode(node1Id, node2Id, payload2) should be {
          Failure(NodeAlreadyExistsException(node2Id))
        }
      }

      "fail with NodeIsParentOfItselfException when adding node with equal id and parent id" in {
        val (nodeId, payload) = node(1)
        treeCRDT(edge(treeConfig.rootNodeId, nodeId, payload))
          .prepareCreateChildNode(treeConfig.rootNodeId, treeConfig.rootNodeId, payload) should be {
          Failure(NodeIsParentOfItselfException(treeConfig.rootNodeId))
        }
      }

    }

    "createChildNode" must {

      "add child node to an empty tree" in {
        val (nodeId, payload) = node(1)
        treeCRDT
          .createChildNode(
            CreateChildNodeOpPrepared(treeConfig.rootNodeId, nodeId, payload),
            mkVectorTimestamp(),
            mkSystemTimestamp(),
            mkEmitterId()
          )
          .value.normalize should be (Tree(treeConfig.rootNodeId, treeConfig.rootPayload, Seq(Tree(nodeId, payload))).normalize)
      }

      "add child node to a single child tree" in {
        val (node1Id, payload1) = node(1)
        val (node2Id, payload2) = node(2)

        treeCRDT(edge(treeConfig.rootNodeId, node1Id, payload1))
          .createChildNode(
            CreateChildNodeOpPrepared(node1Id, node2Id, payload2),
            mkVectorTimestamp(),
            mkSystemTimestamp(),
            mkEmitterId()
          )
          .value should be {
            Tree(
              treeConfig.rootNodeId, treeConfig.rootPayload,
              Seq(
                Tree(
                  node1Id, payload1,
                  Seq(
                    Tree(node2Id, payload2)
                  )
                )
              )
            ).normalize
          }
      }

      "add child node to a root node that already has a child" in {
        val (node1Id, payload1) = node(1)
        val (node2Id, payload2) = node(2)

        treeCRDT(edge(treeConfig.rootNodeId, node1Id, payload1))
          .createChildNode(
            CreateChildNodeOpPrepared(treeConfig.rootNodeId, node2Id, payload2),
            mkVectorTimestamp(),
            mkSystemTimestamp(),
            mkEmitterId())
          .value.normalize should be {
            Tree(
              treeConfig.rootNodeId, treeConfig.rootPayload,
              Seq(
                Tree(node1Id, payload1),
                Tree(node2Id, payload2)
              )
            ).normalize
          }
      }

      "add child node to a root's child node that already has a child" in {
        val (node1Id, payload1) = node(1)
        val (node2Id, payload2) = node(2)
        val (node3Id, payload3) = node(3)

        treeCRDT(
          edge(treeConfig.rootNodeId, node1Id, payload1),
          edge(node1Id, node2Id, payload2)
        ).createChildNode(
          CreateChildNodeOpPrepared(node2Id, node3Id, payload3), mkVectorTimestamp(), mkSystemTimestamp(), mkEmitterId()
        ).value.normalize should be {
          Tree(
            treeConfig.rootNodeId, treeConfig.rootPayload,
            Seq(
              Tree(
                node1Id, payload1,
                Seq(
                  Tree(
                    node2Id, payload2,
                    Seq(
                      Tree(node3Id, payload3)
                    )
                  )
                )
              )
            )
          ).normalize
        }
      }

      "return original tree if same node under same parent and equal payload is added concurrently" in {
        val (node1Id, payload1) = node(1)
        val (node2Id, payload2) = node(2)

        treeCRDT(
          edge(treeConfig.rootNodeId, node1Id, payload1),
          edge(node1Id, node2Id, payload2)
        ).createChildNode(
          CreateChildNodeOpPrepared(node1Id, node2Id, payload2), mkVectorTimestamp(), mkSystemTimestamp(), mkEmitterId()
        ).value.normalize should be {
          Tree(
            treeConfig.rootNodeId, treeConfig.rootPayload,
            Seq(
              Tree(
                node1Id, payload1,
                Seq(
                  Tree(node2Id, payload2)
                )
              )
            )
          ).normalize
        }
      }

      "remove node if same node is added concurrently when Zero mapping policy is used (same parent)" in {
        val (node1Id, payload1) = node(1)
        val (node2Id, payload2) = node(2)
        val (_, payload3) = node(3)

        val config: TreeConfig[Payload, Id] =
          treeConfig.copy(policies = treeConfig.policies.copy(mappingPolicy = MappingPolicy.Zero[Payload, Id]()))

        treeCRDT(
          edge(treeConfig.rootNodeId, node1Id, payload1),
          edge(node1Id, node2Id, payload2)
        )(config)
          .createChildNode(
            CreateChildNodeOpPrepared(node2Id, node2Id, payload3), mkVectorTimestamp(), mkSystemTimestamp(), mkEmitterId()
          )
          .value.normalize should be {
            Tree(treeConfig.rootNodeId, treeConfig.rootPayload, Seq(Tree(node1Id, payload1))).normalize
          }
      }

      "remove node if same node is added concurrently when Zero mapping policy is used (different parent)" in {
        val (node1Id, payload1) = node(1)
        val (node2Id, payload2) = node(2)
        val (_, payload3) = node(3)

        val config: TreeConfig[Payload, Id] =
          treeConfig.copy(policies = treeConfig.policies.copy(mappingPolicy = MappingPolicy.Zero[Payload, Id]()))

        treeCRDT(
          edge(treeConfig.rootNodeId, node1Id, payload1),
          edge(node1Id, node2Id, payload2)
        )(config)
          .createChildNode(
            CreateChildNodeOpPrepared(treeConfig.rootNodeId, node2Id, payload3),
            mkVectorTimestamp(),
            mkSystemTimestamp(),
            mkEmitterId()
          )
          .value.normalize should be {
            Tree(treeConfig.rootNodeId, treeConfig.rootPayload, Seq(Tree(node1Id, payload1))).normalize
          }
      }

      "resolve conflict by comparing vector timestamps when LastWriteWins mapping policy is used (same parent)" in {
        val (node1Id, payload1) = node(1)
        val (node2Id, payload2) = node(2)
        val (_, payload3) = node(3)

        val config: TreeConfig[Payload, Id] =
          treeConfig.copy(policies = treeConfig.policies.copy(mappingPolicy = MappingPolicy.LastWriteWins()))

        def test(existingLogicalTime: Long, logicalTime: Long, expectedPayload: Payload): Assertion = {
          treeCRDT(
            edge(treeConfig.rootNodeId, node1Id, payload1),
            edge(node1Id, node2Id, payload2, mkVectorTimestamp(logicalTime = existingLogicalTime))
          )(config)
            .createChildNode(
              CreateChildNodeOpPrepared(node1Id, node2Id, payload3),
              mkVectorTimestamp(logicalTime = logicalTime),
              mkSystemTimestamp(),
              mkEmitterId()
            )
            .value.normalize should be {
              Tree(
                treeConfig.rootNodeId, treeConfig.rootPayload,
                Seq(
                  Tree(
                    node1Id, payload1,
                    Seq(Tree(node2Id, expectedPayload))
                  )
                )
              ).normalize
            }
        }

        test(existingLogicalTime = 2, logicalTime = 1, expectedPayload = payload2) // keep
        test(existingLogicalTime = 1, logicalTime = 2, expectedPayload = payload3) // replace
      }

      "resolve conflict by comparing vector timestamps when LastWriteWins mapping policy is used (different parent)" in {
        val (node1Id, payload1) = node(1)
        val (node2Id, payload2) = node(2)
        val (_, payload3) = node(3)

        val config: TreeConfig[Payload, Id] =
          treeConfig.copy(policies = treeConfig.policies.copy(mappingPolicy = MappingPolicy.LastWriteWins()))

        def test(existingLogicalTime: Long,
                 logicalTime: Long,
                 expectedPayload: Payload,
                 expectedTree: Tree[Payload, Id]): Assertion = {
          treeCRDT(
            edge(treeConfig.rootNodeId, node1Id, payload1),
            edge(node1Id, node2Id, payload2, mkVectorTimestamp(logicalTime = existingLogicalTime))
          )(config)
            .createChildNode(
              CreateChildNodeOpPrepared(treeConfig.rootNodeId, node2Id, payload3),
              mkVectorTimestamp(logicalTime = logicalTime),
              mkSystemTimestamp(),
              mkEmitterId()
            )
            .value.normalize should be (expectedTree.normalize)
        }

        test( // keep
          existingLogicalTime = 2, logicalTime = 1, expectedPayload = payload2,
          Tree(
            treeConfig.rootNodeId, treeConfig.rootPayload,
            Seq(
              Tree(
                node1Id, payload1,
                Seq(Tree(node2Id, payload2))
              )
            )
          )
        )
        test( // move
          existingLogicalTime = 1, logicalTime = 2, expectedPayload = payload3,
          Tree(
            treeConfig.rootNodeId, treeConfig.rootPayload,
            Seq(
              Tree(node1Id, payload1), Tree(node2Id, payload3)
            )
          )
        )
      }

      "resolve conflict by comparing emitter ids if vector timestamps are concurrent when LastWriteWins mapping policy is used (same parent)" in {
        val (node1Id, payload1) = node(1)
        val (node2Id, payload2) = node(2)
        val (_, payload3) = node(3)

        val config: TreeConfig[Payload, Id] =
          treeConfig.copy(policies = treeConfig.policies.copy(mappingPolicy = MappingPolicy.LastWriteWins()))

        val systemTimestamp = System.currentTimeMillis()

        def test(existingEmitterId: String, emitterId: String, expectedPayload: Payload): Assertion = {
          treeCRDT(
            edge(treeConfig.rootNodeId, node1Id, payload1),
            edge(node1Id, node2Id, payload2, mkVectorTimestamp("P1"), systemTimestamp, existingEmitterId)
          )(config)
            .createChildNode(
              CreateChildNodeOpPrepared(node1Id, node2Id, payload3), mkVectorTimestamp("P2"), systemTimestamp, emitterId
            )
            .value.normalize should be {
              Tree(
                treeConfig.rootNodeId, treeConfig.rootPayload,
                Seq(
                  Tree(
                    node1Id, payload1,
                    Seq(Tree(node2Id, expectedPayload))
                  )
                )
              ).normalize
            }
        }

        test(existingEmitterId = "A", emitterId = "B", expectedPayload = payload2) // keep
        test(existingEmitterId = "B", emitterId = "A", expectedPayload = payload3) // replace
      }

      "resolve conflict by comparing emitter ids if vector timestamps are concurrent when LastWriteWins mapping policy is used (different parent)" in {
        val (node1Id, payload1) = node(1)
        val (node2Id, payload2) = node(2)
        val (_, payload3) = node(3)

        val config: TreeConfig[Payload, Id] =
          treeConfig.copy(policies = treeConfig.policies.copy(mappingPolicy = MappingPolicy.LastWriteWins()))

        val systemTimestamp = System.currentTimeMillis()

        def test(existingEmitterId: String,
                 emitterId: String,
                 expectedPayload: Payload,
                 expectedTree: Tree[Payload, Id]): Assertion = {
          treeCRDT(
            edge(treeConfig.rootNodeId, node1Id, payload1),
            edge(node1Id, node2Id, payload2, mkVectorTimestamp("P1"), systemTimestamp, existingEmitterId)
          )(config)
            .createChildNode(
              CreateChildNodeOpPrepared(treeConfig.rootNodeId, node2Id, payload3), mkVectorTimestamp("P2"), systemTimestamp, emitterId
            )
            .value.normalize should be (expectedTree.normalize)
        }

        test( // keep
          existingEmitterId = "A", emitterId = "B", expectedPayload = payload2,
          Tree(
            treeConfig.rootNodeId, treeConfig.rootPayload,
            Seq(
              Tree(
                node1Id, payload1,
                Seq(Tree(node2Id, payload2))
              )
            )
          )
        )
        test( // move
          existingEmitterId = "B", emitterId = "A", expectedPayload = payload3,
          Tree(
            treeConfig.rootNodeId, treeConfig.rootPayload,
            Seq(
              Tree(node1Id, payload1), Tree(node2Id, payload3)
            )
          )
        )
      }

      "resolve conflict by comparing non-equal system timestamps when LastWriteWins mapping policy is used (same parent)" in {
        val (node1Id, payload1) = node(1)
        val (node2Id, payload2) = node(2)
        val (_, payload3) = node(3)

        val config: TreeConfig[Payload, Id] =
          treeConfig.copy(policies = treeConfig.policies.copy(mappingPolicy = MappingPolicy.LastWriteWins()))

        def test(existingSystemTimestamp: Long, systemTimestamp: Long, expectedPayload: Payload): Assertion = {
          val emitterId = mkEmitterId()

          treeCRDT(
            edge(treeConfig.rootNodeId, node1Id, payload1),
            edge(
              node1Id, node2Id, payload2,
              mkVectorTimestamp("P1", 2L),
              existingSystemTimestamp,
              emitterId
            )
          )(config)
            .createChildNode(
              CreateChildNodeOpPrepared(node1Id, node2Id, payload3),
              mkVectorTimestamp("P2", 2L),
              systemTimestamp,
              emitterId
            )
            .value.normalize should be {
              Tree(
                treeConfig.rootNodeId, treeConfig.rootPayload,
                Seq(
                  Tree(
                    node1Id, payload1,
                    Seq(Tree(node2Id, expectedPayload))
                  )
                )
              ).normalize
            }
        }

        test(existingSystemTimestamp = 1000L, systemTimestamp = 999L, expectedPayload = payload2) // keep
        test(existingSystemTimestamp = 999L, systemTimestamp = 1000L, expectedPayload = payload3) // replace
      }

      "resolve conflict by comparing non-equal system timestamps when LastWriteWins mapping policy is used (different parent)" in {
        val (node1Id, payload1) = node(1)
        val (node2Id, payload2) = node(2)
        val (_, payload3) = node(3)

        val config: TreeConfig[Payload, Id] =
          treeConfig.copy(policies = treeConfig.policies.copy(mappingPolicy = MappingPolicy.LastWriteWins()))

        def test(existingSystemTimestamp: Long,
                 systemTimestamp: Long,
                 expectedPayload: Payload,
                 expectedTree: Tree[Payload, Id]): Assertion = {
          val emitterId = mkEmitterId()

          treeCRDT(
            edge(treeConfig.rootNodeId, node1Id, payload1),
            edge(
              node1Id, node2Id, payload2,
              mkVectorTimestamp("P1", 2L),
              existingSystemTimestamp,
              emitterId
            )
          )(config)
            .createChildNode(
              CreateChildNodeOpPrepared(treeConfig.rootNodeId, node2Id, payload3),
              mkVectorTimestamp("P2", 2L),
              systemTimestamp,
              emitterId
            )
            .value.normalize should be (expectedTree.normalize)
        }

        test( // keep
          existingSystemTimestamp = 1000L, systemTimestamp = 999L, expectedPayload = payload2,
          Tree(
            treeConfig.rootNodeId, treeConfig.rootPayload,
            Seq(
              Tree(
                node1Id, payload1,
                Seq(Tree(node2Id, payload2))
              )
            )
          )
        )
        test( // move
          existingSystemTimestamp = 999L, systemTimestamp = 1000L, expectedPayload = payload3,
          Tree(
            treeConfig.rootNodeId, treeConfig.rootPayload,
            Seq(
              Tree(node1Id, payload1), Tree(node2Id, payload3)
            )
          )
        )
      }

      "resolve conflict by comparing emitter ids if system timestamps are equal when LastWriteWins mapping policy is used (same parent)" in {
        val (node1Id, payload1) = node(1)
        val (node2Id, payload2) = node(2)
        val (_, payload3) = node(3)

        val config: TreeConfig[Payload, Id] =
          treeConfig.copy(policies = treeConfig.policies.copy(mappingPolicy = MappingPolicy.LastWriteWins()))

        val systemTimestamp = System.currentTimeMillis()

        def test(existingEmitterId: String, emitterId: String, expectedPayload: Payload): Assertion = {
          treeCRDT(
            edge(treeConfig.rootNodeId, node1Id, payload1),
            edge(
              node1Id, node2Id, payload2,
              mkVectorTimestamp("P1", 2L),
              systemTimestamp,
              existingEmitterId
            )
          )(config)
            .createChildNode(
              CreateChildNodeOpPrepared(node1Id, node2Id, payload3),
              mkVectorTimestamp("P2", 2L),
              systemTimestamp,
              emitterId
            )
            .value.normalize should be {
              Tree(
                treeConfig.rootNodeId, treeConfig.rootPayload,
                Seq(
                  Tree(
                    node1Id, payload1,
                    Seq(Tree(node2Id, expectedPayload))
                  )
                )
              ).normalize
            }
        }

        test(existingEmitterId = "A", emitterId = "B", expectedPayload = payload2) // keep
        test(existingEmitterId = "B", emitterId = "A", expectedPayload = payload3) // replace
      }

      "resolve conflict by comparing emitter ids if system timestamps are equal when LastWriteWins mapping policy is used (different parent)" in {
        val (node1Id, payload1) = node(1)
        val (node2Id, payload2) = node(2)
        val (_, payload3) = node(3)

        val config: TreeConfig[Payload, Id] =
          treeConfig.copy(policies = treeConfig.policies.copy(mappingPolicy = MappingPolicy.LastWriteWins()))

        val systemTimestamp = System.currentTimeMillis()

        def test(existingEmitterId: String,
                 emitterId: String,
                 expectedPayload: Payload,
                 expectedTree: Tree[Payload, Id]): Assertion = {
          treeCRDT(
            edge(treeConfig.rootNodeId, node1Id, payload1),
            edge(
              node1Id, node2Id, payload2,
              mkVectorTimestamp("P1", 2L),
              systemTimestamp,
              existingEmitterId
            )
          )(config)
            .createChildNode(
              CreateChildNodeOpPrepared(treeConfig.rootNodeId, node2Id, payload3),
              mkVectorTimestamp("P2", 2L),
              systemTimestamp,
              emitterId
            )
            .value.normalize should be (expectedTree.normalize)
        }

        test( // keep
          existingEmitterId = "A", emitterId = "B", expectedPayload = payload2,
          Tree(
            treeConfig.rootNodeId, treeConfig.rootPayload,
            Seq(
              Tree(
                node1Id, payload1,
                Seq(Tree(node2Id, payload2))
              )
            )
          )
        )
        test( // move
          existingEmitterId = "B", emitterId = "A", expectedPayload = payload3,
          Tree(
            treeConfig.rootNodeId, treeConfig.rootPayload,
            Seq(
              Tree(node1Id, payload1), Tree(node2Id, payload3)
            )
          )
        )
      }

      "resolve conflict if same node is added concurrently under same parent when Custom mapping policy is used" in {
        val (node1Id, payload1) = node(1)
        val (node2Id, payload2) = node(2)
        val (_, payload3) = node(3)

        def test(expectedPayload: Payload): Assertion = {
          val config: TreeConfig[Payload, Id] =
            treeConfig.copy(policies = treeConfig.policies.copy(mappingPolicy = MappingPolicy.Custom {
              (payload1, _, _, _) => payload1 == expectedPayload
            }))

          treeCRDT(
            edge(treeConfig.rootNodeId, node1Id, payload1),
            edge(node1Id, node2Id, payload2)
          )(config)
            .createChildNode(CreateChildNodeOpPrepared(node1Id, node2Id, payload3), mkVectorTimestamp(), mkSystemTimestamp(), mkEmitterId())
            .value.normalize should be {
              Tree(
                treeConfig.rootNodeId, treeConfig.rootPayload,
                Seq(
                  Tree(
                    node1Id, payload1,
                    Seq(Tree(node2Id, expectedPayload))
                  )
                )
              ).normalize
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
          .createChildNode(CreateChildNodeOpPrepared(node2Id, node3Id, payload3), mkVectorTimestamp(), mkSystemTimestamp(), mkEmitterId())
          .value.normalize should be {
            Tree(treeConfig.rootNodeId, treeConfig.rootPayload, Seq(Tree(node1Id, payload1))).normalize
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
          edge(node1Id, node3Id, payload3, logicalTime3),
          edge(node3Id, node4Id, payload4, logicalTime4),
          edge(node3Id, node5Id, payload5, logicalTime5)
        ).prepareDeleteSubTree(node3Id) should be {
          Success(Some(
            DeleteSubTreeOpPrepared(
              node3Id,
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

      "fail with CannotDeleteRootNodeException when deleting root node" in {
        val (node1Id, payload1) = node(1)

        treeCRDT(
          edge(treeConfig.rootNodeId, node1Id, payload1)
        ).prepareDeleteSubTree(treeConfig.rootNodeId) should be (Failure(CannotDeleteRootNodeException(treeConfig.rootNodeId)))
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
          edge(node1Id, node3Id, payload3, logicalTime3),
          edge(node3Id, node4Id, payload4, logicalTime4),
          edge(node3Id, node5Id, payload5, logicalTime5)
        ).deleteSubTree(
          DeleteSubTreeOpPrepared(
            node3Id,
            Set(node3Id, node4Id, node5Id),
            Set(logicalTime3, logicalTime4, logicalTime5)
          )
        )

        result.value.normalize should be {
          Tree(
            treeConfig.rootNodeId, treeConfig.rootPayload,
            Seq(Tree(node1Id, payload1, Seq(Tree(node2Id, payload2))))
          ).normalize
        }
      }

    }

  }

  private implicit val treeConfig: TreeConfig[Payload, Id] =
    TreeConfig[Payload, Id]("root", "root's payload")

  private def treeCRDT: UnorderedTree[Payload, Id] = UnorderedTree[Payload, Id]

  private def treeCRDT(edges: (Edge[Payload, Id], VectorTime)*)
                      (implicit treeConfig: TreeConfig[Payload, Id]): UnorderedTree[Payload, Id] = {
    val edgesORSet =
      edges.foldLeft(ORSet[Edge[Payload, Id]]) {
        case (orSet, (edge, vectorTimestamp)) => orSet.add(edge, vectorTimestamp)
      }

    UnorderedTree[Payload, Id](edgesORSet)
  }

  private def edge(parentId: Id,
                   nodeId: Id,
                   payload: Payload,
                   vectorTimestamp: VectorTime = mkVectorTimestamp(),
                   systemTimestamp: Long = mkSystemTimestamp(),
                   emitterId: String = mkEmitterId()): (Edge[Payload, Id], VectorTime) =
    (Edge(nodeId, parentId, payload, systemTimestamp, emitterId), vectorTimestamp)

}
