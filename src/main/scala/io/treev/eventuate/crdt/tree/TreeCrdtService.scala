package io.treev.eventuate.crdt.tree

import akka.actor.{ActorRef, ActorSystem}
import com.rbmhtechnology.eventuate.crdt.{CRDTService, CRDTServiceOps}
import io.treev.eventuate.crdt.tree.model.Tree
import io.treev.eventuate.crdt.tree.model.op.{CreateChildNodeOp, DeleteSubTreeOp}

import scala.concurrent.Future

class TreeCrdtService[A, Id](override val serviceId: String,
                             override val log: ActorRef)
                            (implicit override val system: ActorSystem,
                                      override val ops: CRDTServiceOps[TreeCRDT[A, Id], Tree[A, Id]])
  extends CRDTService[TreeCRDT[A, Id], Tree[A, Id]] {

  start()

  def createChildNode(treeId: String, parentId: Id, nodeId: Id, payload: A): Future[Tree[A, Id]] =
    op(treeId, CreateChildNodeOp(parentId, nodeId, payload))

  def deleteSubTree(treeId: String, nodeId: Id): Future[Tree[A, Id]] =
    op(treeId, DeleteSubTreeOp(nodeId))

}
