package io.treev.eventuate.crdt.tree

import akka.actor.ActorSystem
import com.rbmhtechnology.eventuate.SingleLocationSpecLeveldb
import io.treev.eventuate.crdt.tree.model.{Tree, TreeConfig}
import io.treev.eventuate.crdt.tree.model.exception.{NodeAlreadyExistsException, ParentNodeNotExistsException}
import org.scalatest.{Assertion, AsyncWordSpec, Matchers}

import scala.concurrent.Future

class TreeCRDTServiceSpecLeveldb extends AsyncWordSpec with Matchers with SingleLocationSpecLeveldb {

  "TreeCRDTService" must {

    "return single root element tree on empty tree" in withService { service =>
      service.value(crdtId).map {
        _ should be (Tree(treeConfig.rootNodeId, treeConfig.rootPayload))
      }
    }

    "add child node to an empty tree" in withService { service =>
      val (nodeId, payload) = node(1)
      service.createChildNode(crdtId, treeConfig.rootNodeId, nodeId, payload).map {
        _ should be (Tree(treeConfig.rootNodeId, treeConfig.rootPayload, Set(Tree(nodeId, payload))))
      }
    }

    "add child node to a single child tree" in withService { service =>
      val (node1Id, payload1) = node(1)
      val (node2Id, payload2) = node(2)

      for {
        _ <- service.createChildNode(crdtId, treeConfig.rootNodeId, node1Id, payload1)
        tree <- service.createChildNode(crdtId, node1Id, node2Id, payload2)
      } yield {
        tree should be {
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
    }

    "add child node to a root node that already has a child" in withService { service =>
      val (node1Id, payload1) = node(1)
      val (node2Id, payload2) = node(2)

      for {
        _ <- service.createChildNode(crdtId, treeConfig.rootNodeId, node1Id, payload1)
        tree <- service.createChildNode(crdtId, treeConfig.rootNodeId, node2Id, payload2)
      } yield {
        tree should be {
          Tree(
            treeConfig.rootNodeId, treeConfig.rootPayload,
            Set(
              Tree(node1Id, payload1),
              Tree(node2Id, payload2)
            )
          )
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
        tree should be {
          Tree(
            treeConfig.rootNodeId, treeConfig.rootPayload,
            Set(
              Tree(
                node1Id, payload1,
                Set(
                  Tree(node2Id, payload2),
                  Tree(node3Id, payload3)
                )
              )
            )
          )
        }
      }
    }

    "fail with NodeAlreadyExistsException if parent node doesn't exist" in withService { service =>
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

  override implicit val system: ActorSystem = ActorSystem("test")

  private val crdtId = "1"

  private implicit val treeConfig: TreeConfig[String, String] =
    TreeConfig[String, String]("root", "root's payload")

  private def withService(f: TreeCRDTService[String, String] => Future[Assertion]): Future[Assertion] = {
    val service = new TreeCRDTService[String, String]("a", log)
    f(service)
  }

  private def node(number: Int): (String, String) =
    (s"child$number", s"child$number's payload")

}
