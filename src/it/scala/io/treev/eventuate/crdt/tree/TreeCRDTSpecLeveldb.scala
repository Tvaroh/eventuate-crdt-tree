package io.treev.eventuate.crdt.tree

import io.treev.eventuate.crdt.tree.model.Tree
import io.treev.eventuate.crdt.tree.model.exception.{NodeAlreadyExistsException, ParentNodeNotExistsException}
import org.scalatest.{Assertion, AsyncWordSpec}

import scala.concurrent.Future

class TreeCRDTSpecLeveldb extends AsyncWordSpec with TreeCRDTSpecBaseLeveldb {

  "TreeCRDT" must {

    "return single root element tree on empty tree" in withService { service =>
      service.value(crdtId).map {
        _ should be (Tree(treeConfig.rootNodeId, treeConfig.rootPayload, noChildren))
      }
    }

    "add child node to an empty tree" in withService { service =>
      val (nodeId, payload) = node(1)
      service.createChildNode(crdtId, treeConfig.rootNodeId, nodeId, payload).map {
        _ should be (Tree(treeConfig.rootNodeId, treeConfig.rootPayload, Set(Tree(nodeId, payload, noChildren))))
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
                  Tree(node2Id, payload2, noChildren)
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
              Tree(node1Id, payload1, noChildren),
              Tree(node2Id, payload2, noChildren)
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
                  Tree(node2Id, payload2, noChildren),
                  Tree(node3Id, payload3, noChildren)
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
        service.createChildNode(crdtId, "oops", node1Id, payload1)
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

  private def withService(f: TreeCRDTService[String, String] => Future[Assertion]): Future[Assertion] = {
    val loc = location("A", customConfig = customConfig)
    val endpoint = loc.endpoint(Set("L1"), Set.empty)
    f(service(endpoint))
  }

  private def node(number: Int): (String, String) =
    (s"child$number", s"child$number's payload")

}
