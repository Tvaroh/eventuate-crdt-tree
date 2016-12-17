package io.treev.eventuate.crdt.tree

import com.rbmhtechnology.eventuate.{DurableEvent, VectorTime}
import com.rbmhtechnology.eventuate.crdt.{CRDTFormat, CRDTServiceOps, ORSet}
import io.treev.eventuate.crdt.tree.model.{Tree, TreeConfig}
import io.treev.eventuate.crdt.tree.model.internal.Edge
import io.treev.eventuate.crdt.tree.model.op.{CreateChildNodeOp, CreateChildNodeOpPrepared, DeleteSubTreeOp, DeleteSubTreeOpPrepared}

case class TreeCrdt[A, Id](edges: ORSet[Edge[A, Id]] = ORSet[Edge[A, Id]])
  extends CRDTFormat {

  def value: Tree[A, Id] = ???

  def value(nodeId: Id): Option[Tree[A, Id]] = ???

  private[tree]
  def prepareCreateChildNode(parentId: Id, nodeId: Id, payload: A): Option[CreateChildNodeOpPrepared[Id, A]] = ???

  private[tree]
  def createChildNode(prepared: CreateChildNodeOpPrepared[Id, A], timestamp: VectorTime): TreeCrdt[A, Id] = ???

  private[tree]
  def prepareDeleteSubTree(nodeId: Id): Option[DeleteSubTreeOpPrepared[Id]] = ???

  private[tree]
  def deleteSubTree(prepared: DeleteSubTreeOpPrepared[Id]): TreeCrdt[A, Id] = ???

}

object TreeCrdt {

  def apply[A, Id](implicit config: TreeConfig[A, Id]): TreeCrdt[A, Id] = TreeCrdt[A, Id]()

  implicit def TreeCrdtServiceOps[A, Id](
    implicit config: TreeConfig[A, Id]
  ): CRDTServiceOps[TreeCrdt[A, Id], Tree[A, Id]] =
    new CRDTServiceOps[TreeCrdt[A, Id], Tree[A, Id]] {

      override def zero: TreeCrdt[A, Id] = TreeCrdt[A, Id]

      override def value(crdt: TreeCrdt[A, Id]): Tree[A, Id] = crdt.value

      override def prepare(crdt: TreeCrdt[A, Id], operation: Any): Option[Any] =
        operation match {
          case CreateChildNodeOp(parentId, nodeId, payload) =>
            crdt.prepareCreateChildNode(coerseId(parentId), coerseId(nodeId), coersePayload(payload))
          case DeleteSubTreeOp(nodeId) =>
            crdt.prepareDeleteSubTree(coerseId(nodeId))
          case _ =>
            super.prepare(crdt, operation)
        }

      override def effect(crdt: TreeCrdt[A, Id], operation: Any, event: DurableEvent): TreeCrdt[A, Id] =
        operation match {
          case CreateChildNodeOpPrepared(_, _, _) =>
            crdt.createChildNode(operation.asInstanceOf[CreateChildNodeOpPrepared[Id, A]], event.vectorTimestamp)
          case DeleteSubTreeOpPrepared(_, _, _) =>
            crdt.deleteSubTree(operation.asInstanceOf[DeleteSubTreeOpPrepared[Id]])
        }

      @inline private def coerseId(id: Any): Id = id.asInstanceOf[Id]
      @inline private def coersePayload(payload: Any): A = payload.asInstanceOf[A]

    }

}
