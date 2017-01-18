package io.treev.eventuate.crdt.tree

import akka.actor.ActorSystem
import com.rbmhtechnology.eventuate.SingleLocationSpecLeveldb
import io.treev.eventuate.crdt.tree.TestHelpers._
import io.treev.eventuate.crdt.tree.model._
import io.treev.eventuate.crdt.tree.model.exception._
import org.scalatest.{Assertion, AsyncWordSpec, Matchers}

import scala.concurrent.Future

class UnorderedTreeServiceSpecLeveldb extends AsyncWordSpec with Matchers with SingleLocationSpecLeveldb {

  "UnorderedTreeService" must {

    "value" must {

      "return single root element tree on empty tree" in withService { service =>
        service.value(crdtId).map {
          _.normalize should be (Tree(treeConfig.rootNodeId, treeConfig.rootPayload).normalize)
        }
      }

    }

    "createChildNode" must {

      "add child node to an empty tree" in withService { service =>
        val (nodeId, payload) = node(1)
        service.createChildNode(crdtId, treeConfig.rootNodeId, nodeId, payload).map {
          _.normalize should be (Tree(treeConfig.rootNodeId, treeConfig.rootPayload, Seq(Tree(nodeId, payload))).normalize)
        }
      }

      "add child node to a single child tree" in withService { service =>
        val (node1Id, payload1) = node(1)
        val (node2Id, payload2) = node(2)

        for {
          _ <- service.createChildNode(crdtId, treeConfig.rootNodeId, node1Id, payload1)
          tree <- service.createChildNode(crdtId, node1Id, node2Id, payload2)
        } yield {
          tree.normalize should be {
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
      }

      "add child node to a root node that already has a child" in withService { service =>
        val (node1Id, payload1) = node(1)
        val (node2Id, payload2) = node(2)

        for {
          _ <- service.createChildNode(crdtId, treeConfig.rootNodeId, node1Id, payload1)
          tree <- service.createChildNode(crdtId, treeConfig.rootNodeId, node2Id, payload2)
        } yield {
          tree.normalize should be {
            Tree(
              treeConfig.rootNodeId, treeConfig.rootPayload,
              Seq(
                Tree(node1Id, payload1),
                Tree(node2Id, payload2)
              )
            ).normalize
          }
        }
      }

      "add child node to a root's child node that already has a child" in withService { service =>
        val (node1Id, payload1) = node(1)
        val (node2Id, payload2) = node(2)
        val (node3Id, payload3) = node(3)

        for {
          _ <- service.createChildNode(crdtId, treeConfig.rootNodeId, node1Id, payload1)
          _ <- service.createChildNode(crdtId, node1Id, node2Id, payload2)
          tree <- service.createChildNode(crdtId, node1Id, node3Id, payload3)
        } yield {
          tree.normalize should be {
            Tree(
              treeConfig.rootNodeId, treeConfig.rootPayload,
              Seq(
                Tree(
                  node1Id, payload1,
                  Seq(
                    Tree(node2Id, payload2),
                    Tree(node3Id, payload3)
                  )
                )
              )
            ).normalize
          }
        }
      }

      "fail with ParentNodeNotExistsException if parent node doesn't exist" in withService { service =>
        val (node1Id, payload1) = node(1)

        recoverToSucceededIf[ParentNodeNotExistsException] {
          service.createChildNode(crdtId, "wrong", node1Id, payload1)
        }
      }

      "fail with NodeAlreadyExistsException if node with same id already exists" in withService { service =>
        val (node1Id, payload1) = node(1)
        val (node2Id, payload2) = node(1)

        recoverToSucceededIf[NodeAlreadyExistsException] {
          for {
            _ <- service.createChildNode(crdtId, treeConfig.rootNodeId, node1Id, payload1)
            _ <- service.createChildNode(crdtId, treeConfig.rootNodeId, node2Id, payload2)
          } yield ()
        }
      }

    }

    "deleteSubTree" must {

      "delete previously created leaf node" in withService { service =>
        val (node1Id, payload1) = node(1)
        val (node2Id, payload2) = node(2)
        val (node3Id, payload3) = node(3) // to delete
        val (node4Id, payload4) = node(4)

        for {
          _ <- service.createChildNode(crdtId, treeConfig.rootNodeId, node1Id, payload1)
          _ <- service.createChildNode(crdtId, node1Id, node2Id, payload2)
          _ <- service.createChildNode(crdtId, node2Id, node3Id, payload3)
          _ <- service.createChildNode(crdtId, node2Id, node4Id, payload4)
          tree <- service.deleteSubTree(crdtId, node3Id)
        } yield {
          tree.normalize should be {
            Tree(
              treeConfig.rootNodeId, treeConfig.rootPayload,
              Seq(
                Tree(
                  node1Id, payload1, Seq(
                    Tree(
                      node2Id, payload2,
                      Seq(Tree(node4Id, payload4))
                    )
                  )
                )
              )
            ).normalize
          }
        }
      }

      "delete previously created node with its children" in withService { service =>
        val (node1Id, payload1) = node(1)
        val (node2Id, payload2) = node(2) // to delete
        val (node3Id, payload3) = node(3)
        val (node4Id, payload4) = node(4)
        val (node5Id, payload5) = node(5)

        for {
          _ <- service.createChildNode(crdtId, treeConfig.rootNodeId, node1Id, payload1)
          _ <- service.createChildNode(crdtId, node1Id, node2Id, payload2)
          _ <- service.createChildNode(crdtId, node2Id, node3Id, payload3)
          _ <- service.createChildNode(crdtId, node2Id, node4Id, payload4)
          _ <- service.createChildNode(crdtId, node4Id, node5Id, payload5)
          tree <- service.deleteSubTree(crdtId, node2Id)
        } yield {
          tree.normalize should be {
            Tree(
              treeConfig.rootNodeId, treeConfig.rootPayload,
              Seq(
                Tree(
                  node1Id, payload1
                )
              )
            ).normalize
          }
        }
      }

      "fail with NodeNotExistsException if node id doesn't exist" in withService { service =>
        val (node1Id, payload1) = node(1)

        recoverToSucceededIf[NodeNotExistsException] {
          for {
            _ <- service.createChildNode(crdtId, treeConfig.rootNodeId, node1Id, payload1)
            _ <- service.deleteSubTree(crdtId, "wrong")
          } yield ()
        }
      }

      "fail with CannotDeleteRootNodeException when deleting root node" in withService { service =>
        val (node1Id, payload1) = node(1)

        recoverToSucceededIf[CannotDeleteRootNodeException] {
          for {
            _ <- service.createChildNode(crdtId, treeConfig.rootNodeId, node1Id, payload1)
            _ <- service.deleteSubTree(crdtId, treeConfig.rootNodeId)
          } yield ()
        }
      }

    }

  }

  override implicit val system: ActorSystem = ActorSystem("test")

  private val crdtId = "1"

  private implicit val treeConfig: TreeConfig[Payload, Id] =
    TreeConfig[String, String]("root", "root's payload")

  private def withService(f: UnorderedTreeService[Payload, Id] => Future[Assertion]): Future[Assertion] = {
    val service = new UnorderedTreeService[Payload, Id]("a", log)
    f(service)
  }

}
