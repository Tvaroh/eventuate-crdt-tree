package io.treev.eventuate.crdt.tree

import com.rbmhtechnology.eventuate.DurableEvent
import com.rbmhtechnology.eventuate.crdt.{CRDTServiceOps, ORSet}
import io.treev.eventuate.crdt.tree.model._
import io.treev.eventuate.crdt.tree.model.exception._
import io.treev.eventuate.crdt.tree.model.internal._
import io.treev.eventuate.crdt.tree.model.op._

import scala.util.{Failure, Success, Try}

/** Unordered tree CRDT. */
case class TreeCRDT[A, Id](edges: ORSet[Edge[A, Id]] = ORSet[Edge[A, Id]],
                           edgesServiceInfo: Map[Id, ServiceInfo] = Map.empty[Id, ServiceInfo],
                           edgesByNodeId: Map[Id, Edge[A, Id]] = Map.empty[Id, Edge[A, Id]],
                           edgesByParentId: Map[Id, Map[Id, Edge[A, Id]]] = Map.empty[Id, Map[Id, Edge[A, Id]]])
                          (implicit treeConfig: TreeConfig[A, Id]) {

  /** Get whole tree value from underlying CRDT. */
  def value: Tree[A, Id] = {
    val topEdges = edgesByParentId.getOrElse(treeConfig.rootNodeId, Map.empty)
    Tree(treeConfig.rootNodeId, treeConfig.rootPayload, topEdges.keySet.flatMap(nodeId => value(nodeId)))
  }

  /** Get tree value starting from supplied node id from underlying CRDT. */
  def value(nodeId: Id): Option[Tree[A, Id]] =
    if (isRoot(nodeId)) Some(value)
    else {
      def toTree(edge: Edge[A, Id], parentId: Id): Tree[A, Id] = {
        val childEdges = edgesByParentId.getOrElse(edge.nodeId, Map.empty)
        Tree(edge.nodeId, edge.payload, childEdges.values.map(toTree(_, edge.nodeId)).toSet)
      }

      for {
        edge <- edgesByNodeId.get(nodeId)
        children = edgesByParentId.getOrElse(nodeId, Map.empty)
      } yield Tree(nodeId, edge.payload, children.values.map(toTree(_, nodeId)).toSet)
    }

  private[tree]
  def prepareCreateChildNode(parentId: Id, nodeId: Id, payload: A): Try[Option[CreateChildNodeOpPrepared[Id, A]]] =
    if (nodeExists(parentId))
      if (!nodeExists(nodeId)) Success(Some(CreateChildNodeOpPrepared(parentId, nodeId, payload)))
      else Failure(NodeAlreadyExistsException(nodeId))
    else Failure(ParentNodeNotExistsException(parentId))

  private[tree]
  def createChildNode(prepared: CreateChildNodeOpPrepared[Id, A], serviceInfo: ServiceInfo): TreeCRDT[A, Id] = {
    val edge = Edge(prepared.nodeId, prepared.parentId, prepared.payload)

    if (nodeExists(prepared.parentId))
      edgesByNodeId
        .get(prepared.nodeId)
        .fold(addEdge(edge, serviceInfo))(resolveConcurrentAddition(_, edge, serviceInfo))
    else
      resolveConcurrentAdditionDeletion(edge, serviceInfo)
  }

  private[tree]
  def prepareDeleteSubTree(nodeId: Id): Try[Option[DeleteSubTreeOpPrepared[Id]]] =
    edgesByNodeId
      .get(nodeId)
      .map { edge =>
        val children = getChildEdges(edge)
        val timestamps = children.flatMap(edges.prepareRemove).toSet ++ edges.prepareRemove(edge)
        val nodeIds = children.view.map(_.nodeId).toSet + nodeId
        Success(Some(DeleteSubTreeOpPrepared(nodeId, edge.parentId, nodeIds, timestamps)))
      }
      .getOrElse(Failure(NodeNotExistsException(nodeId)))

  private[tree]
  def deleteSubTree(prepared: DeleteSubTreeOpPrepared[Id]): TreeCRDT[A, Id] =
    copy(
      edges = edges.remove(prepared.timestamps),
      edgesServiceInfo = edgesServiceInfo -- prepared.nodeIds,
      edgesByNodeId = edgesByNodeId -- prepared.nodeIds,
      edgesByParentId = {
        val edgesByParentIdLeft = edgesByParentId -- prepared.nodeIds
        edgesByParentIdLeft.updated(
          prepared.parentId,
          edgesByParentIdLeft.get(prepared.parentId).map(_ - prepared.nodeId).getOrElse(Map.empty)
        )
      }
    )

  private def isRoot(nodeId: Id): Boolean =
    nodeId == treeConfig.rootNodeId

  private def nodeExists(nodeId: Id): Boolean =
    isRoot(nodeId) || edgesByNodeId.contains(nodeId)

  private def addEdge(edge: Edge[A, Id], serviceInfo: ServiceInfo): TreeCRDT[A, Id] =
    copy(
      edges = edges.add(edge, serviceInfo.vectorTimestamp),
      edgesServiceInfo = edgesServiceInfo.updated(edge.nodeId, serviceInfo),
      edgesByNodeId = edgesByNodeId.updated(edge.nodeId, edge),
      edgesByParentId = edgesByParentId
        .updated(edge.parentId, edgesByParentId.getOrElse(edge.parentId, Map.empty).updated(edge.nodeId, edge))
    )

  private def replaceEdge(existingEdge: Edge[A, Id], edge: Edge[A, Id], serviceInfo: ServiceInfo): TreeCRDT[A, Id] =
    copy(
      edges = // TODO ensure this does commute
        edges.remove(edges.prepareRemove(existingEdge)).add(edge, serviceInfo.vectorTimestamp),
      edgesServiceInfo = edgesServiceInfo.updated(existingEdge.nodeId, serviceInfo),
      edgesByNodeId = edgesByNodeId.updated(existingEdge.nodeId, edge),
      edgesByParentId = edgesByParentId
        .updated(
          existingEdge.parentId,
          edgesByParentId.getOrElse(existingEdge.parentId, Map.empty).updated(edge.nodeId, edge)
        )
    )

  private def removeEdge(edge: Edge[A, Id]): TreeCRDT[A, Id] =
    copy(
      edges = // TODO ensure this does commute
        edges.remove(edges.prepareRemove(edge)),
      edgesServiceInfo = edgesServiceInfo - edge.nodeId,
      edgesByNodeId = edgesByNodeId - edge.nodeId,
      edgesByParentId = edgesByParentId
        .updated(edge.parentId, edgesByParentId.get(edge.parentId).map(_ - edge.nodeId).getOrElse(Map.empty))
    )

  private def resolveConcurrentAddition(existingEdge: Edge[A, Id],
                                        edge: Edge[A, Id],
                                        serviceInfo: ServiceInfo): TreeCRDT[A, Id] =
    if (existingEdge.parentId == edge.parentId) // both nodes are under same parent
      if (existingEdge.payload == edge.payload) this // nodes are equal
      else applyMappingPolicy(existingEdge, edge, serviceInfo)
    else applyMappingPolicy(existingEdge, edge, serviceInfo)

  private def applyMappingPolicy(existingEdge: Edge[A, Id],
                                 edge: Edge[A, Id],
                                 serviceInfo: ServiceInfo): TreeCRDT[A, Id] =
    treeConfig.policies.mappingPolicy match {
      case MappingPolicy.Zero =>
        removeEdge(existingEdge)

      case MappingPolicy.LastWriteWins =>
        val existingEdgeServiceInfo = edgesServiceInfo(existingEdge.nodeId)
        val winner = getLastWriteWinner(existingEdge, existingEdgeServiceInfo, edge, serviceInfo)
        if (winner eq existingEdge) this else replaceEdge(existingEdge, edge, serviceInfo)

      case policy@MappingPolicy.Custom() =>
        if (policy.resolver.firstWins(existingEdge.payload, edge.payload))
          this // existing node won
        else
          replaceEdge(existingEdge, existingEdge.copy(payload = edge.payload), serviceInfo) // update payload
    }

  private def getLastWriteWinner(edge1: Edge[A, Id], edge1ServiceInfo: ServiceInfo,
                                 edge2: Edge[A, Id], edge2ServiceInfo: ServiceInfo): Edge[A, Id] =
    Seq((edge1, edge1ServiceInfo), (edge2, edge2ServiceInfo))
      .sortWith { case ((_, first), (_, second)) =>
        if (first.vectorTimestamp <-> second.vectorTimestamp) // concurrent
          if (first.systemTimestamp != second.systemTimestamp)
            first.systemTimestamp > second.systemTimestamp // first happened last
          else // both happened simultaneously - use one with lesser (alphabetically) emitter id
            first.emitterId < second.emitterId
        else if (first.vectorTimestamp equiv second.vectorTimestamp)
          first.emitterId < second.emitterId
        else
          first.vectorTimestamp > second.vectorTimestamp || // first happened last
            !(first.vectorTimestamp < second.vectorTimestamp) // second happened last
      }
      .head
      ._1

  private def resolveConcurrentAdditionDeletion(edge: Edge[A, Id], serviceInfo: ServiceInfo): TreeCRDT[A, Id] =
    treeConfig.policies.connectionPolicy match {
      case ConnectionPolicy.Skip =>
        this
      case ConnectionPolicy.Root =>
        addEdge(edge.copy(parentId = treeConfig.rootNodeId), serviceInfo)
    }

  private def getChildEdges(edge: Edge[A, Id]): Iterable[Edge[A, Id]] = {
    @annotation.tailrec
    def loop(parentIds: Iterable[Id], acc: Iterable[Edge[A, Id]]): Iterable[Edge[A, Id]] =
      if (parentIds.nonEmpty) {
        val childEdges = parentIds.flatMap { parentId =>
          edgesByParentId.get(parentId).fold(Iterable.empty[Edge[A, Id]])(_.values)
        }

        loop(childEdges.map(_.nodeId), acc ++ childEdges)
      } else {
        acc
      }

    loop(Iterable(edge.nodeId), Iterable.empty)
  }

}

object TreeCRDT {

  def apply[A, Id](implicit treeConfig: TreeConfig[A, Id]): TreeCRDT[A, Id] = TreeCRDT[A, Id]()

  class TreeCRDTServiceOps[A, Id](implicit treeConfig: TreeConfig[A, Id])
    extends CRDTServiceOps[TreeCRDT[A, Id], Tree[A, Id]] {

    override def zero: TreeCRDT[A, Id] = TreeCRDT[A, Id]

    override def value(crdt: TreeCRDT[A, Id]): Tree[A, Id] = crdt.value

    override def prepare(crdt: TreeCRDT[A, Id], operation: Any): Try[Option[Any]] =
      operation match {
        case CreateChildNodeOp(parentId, nodeId, payload) =>
          crdt.prepareCreateChildNode(coerseId(parentId), coerseId(nodeId), coersePayload(payload))
        case DeleteSubTreeOp(nodeId) =>
          crdt.prepareDeleteSubTree(coerseId(nodeId))
        case _ =>
          super.prepare(crdt, operation)
      }

    override def effect(crdt: TreeCRDT[A, Id], operation: Any, event: DurableEvent): TreeCRDT[A, Id] =
      operation match {
        case CreateChildNodeOpPrepared(_, _, _) =>
          crdt.createChildNode(operation.asInstanceOf[CreateChildNodeOpPrepared[Id, A]], toServiceInfo(event))
        case DeleteSubTreeOpPrepared(_, _, _, _) =>
          crdt.deleteSubTree(operation.asInstanceOf[DeleteSubTreeOpPrepared[Id]])
      }

    @inline private def coerseId(id: Any): Id = id.asInstanceOf[Id]
    @inline private def coersePayload(payload: Any): A = payload.asInstanceOf[A]

    private def toServiceInfo(event: DurableEvent): ServiceInfo =
      ServiceInfo(event.vectorTimestamp, event.emitterId, event.systemTimestamp)

  }

  implicit def TreeCrdtServiceOps[A, Id](
    implicit treeConfig: TreeConfig[A, Id]
  ): CRDTServiceOps[TreeCRDT[A, Id], Tree[A, Id]] = new TreeCRDTServiceOps[A, Id]()

}
