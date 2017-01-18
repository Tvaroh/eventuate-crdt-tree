package io.treev.eventuate.crdt.tree

import com.rbmhtechnology.eventuate.crdt.{CRDTServiceOps, ORSet}
import com.rbmhtechnology.eventuate.{DurableEvent, VectorTime, Versioned}
import io.treev.eventuate.crdt.tree.model.exception._
import io.treev.eventuate.crdt.tree.model.internal._
import io.treev.eventuate.crdt.tree.model.op._
import io.treev.eventuate.crdt.tree.model._

import scala.annotation.tailrec
import scala.collection.mutable
import scala.util.{Failure, Success, Try}

/** Unordered tree CRDT. */
case class UnorderedTree[A, Id](
  edges: ORSet[Edge[A, Id]] = ORSet[Edge[A, Id]],
  edgesMetainfo: Map[Id, Map[VectorTime, EdgeMetainfo]] = Map.empty[Id, Map[VectorTime, EdgeMetainfo]]
)(implicit treeConfig: TreeConfig[A, Id]) {

  /** Get whole tree value from underlying CRDT. */
  def value: Tree[A, Id] = {
    val (edgesByParentId, edgesByNodeId) = buildLookups(edges)

    def asTree(nodeId: Id, payload: A): Tree[A, Id] =
      edgesByParentId
        .get(nodeId)
        .fold(Tree(nodeId, payload)) { childEdges =>
          val resolvedChildEdges =
            childEdges
              .groupBy(_.value.nodeId)
              .flatMap {
                case (_, singletonList@(_ :: Nil)) => singletonList
                case (_, duplicates) => applyMappingPolicy(duplicates).toList // apply same-parent mapping policy
              }
              .filter { versionedEdge =>
                val winnerOpt = edgesByNodeId.getOrElse(versionedEdge.value.nodeId, List.empty) match {
                  case single :: Nil => Some(single)
                  case duplicates =>
                    // apply different-parent mapping policy
                    val differentParentDuplicates = duplicates.filter(_.value.parentId != versionedEdge.value.parentId)
                    applyMappingPolicy(versionedEdge :: differentParentDuplicates)
                }
                winnerOpt.contains(versionedEdge)
              }

          Tree(
            nodeId, payload,
            resolvedChildEdges
              .map(versionedEdge => asTree(versionedEdge.value.nodeId, versionedEdge.value.payload))
              .toSeq
          )
        }

    asTree(treeConfig.rootNodeId, treeConfig.rootPayload)
  }

  private[tree]
  def prepareCreateChildNode(parentId: Id, nodeId: Id, payload: A): Try[Option[CreateChildNodeOpPrepared[Id, A]]] =
    if (parentId != nodeId) {
      val edgesValue = edges.value
      val it = edgesValue.iterator // ensure single iteration

      var nodeExists = isRoot(nodeId)
      var parentExists = isRoot(parentId)

      while (it.hasNext && !(nodeExists && parentExists)) {
        val edge = it.next()
        if (edge.nodeId == nodeId) { nodeExists = true }
        if (edge.nodeId == parentId) { parentExists = true }
      }

      if (parentExists)
        if (!nodeExists) Success(Some(CreateChildNodeOpPrepared(parentId, nodeId, payload)))
        else Failure(NodeAlreadyExistsException(nodeId))
      else Failure(ParentNodeNotExistsException(parentId))
    } else {
      Failure(NodeIsParentOfItselfException(nodeId))
    }

  private[tree]
  def createChildNode(prepared: CreateChildNodeOpPrepared[Id, A],
                      vectorTimestamp: VectorTime,
                      edgeMetainfo: EdgeMetainfo): UnorderedTree[A, Id] = {
    val edge = Edge(prepared.nodeId, prepared.parentId, prepared.payload)
    copy(
      edges = edges.add(edge, vectorTimestamp),
      edgesMetainfo =
        edgesMetainfo.updated(
          prepared.nodeId,
          edgesMetainfo.getOrElse(prepared.nodeId, Map.empty).updated(vectorTimestamp, edgeMetainfo)
        )
    )
  }

  private[tree]
  def prepareDeleteSubTree(nodeId: Id): Try[Option[DeleteSubTreeOpPrepared[Id]]] =
    if (!isRoot(nodeId)) {
      val (edgesByParentId, edgesByNodeId) = buildLookups(edges)

      @tailrec
      def loop(parentIds: Seq[Id],
               acc: Set[Versioned[Edge[A, Id]]] = Set.empty): Set[Versioned[Edge[A, Id]]] =
        if (parentIds.nonEmpty) {
          val parentEdges = parentIds.flatMap(parentId => edgesByNodeId.getOrElse(parentId, Seq.empty))
          val childEdges = parentIds.flatMap(parentId => edgesByParentId.getOrElse(parentId, Seq.empty))
          loop(childEdges.map(_.value.nodeId), acc ++ parentEdges)
        } else {
          acc
        }

      val edgesToDelete = loop(Seq(nodeId))
      if (edgesToDelete.nonEmpty)
        Success {
          Some(DeleteSubTreeOpPrepared(nodeId, edgesToDelete.map(_.value.nodeId), edgesToDelete.map(_.vectorTimestamp)))
        }
      else
        Failure(NodeNotExistsException(nodeId))
    } else {
      Failure(CannotDeleteRootNodeException(nodeId))
    }

  private[tree]
  def deleteSubTree(prepared: DeleteSubTreeOpPrepared[Id]): UnorderedTree[A, Id] =
    copy(
      edges = edges.remove(prepared.timestamps),
      edgesMetainfo = edgesMetainfo -- prepared.nodeIds
    )

  private def buildLookups(
    edges: ORSet[Edge[A, Id]]
  ): (collection.Map[Id, List[Versioned[Edge[A, Id]]]], collection.Map[Id, List[Versioned[Edge[A, Id]]]]) = {
    val edgesByParentId = mutable.Map.empty[Id, List[Versioned[Edge[A, Id]]]]
    val edgesByNodeId = mutable.Map.empty[Id, List[Versioned[Edge[A, Id]]]]

    edges.versionedEntries.foreach { versionedEdge =>
      val edge = versionedEdge.value
      edgesByParentId.update(edge.parentId, versionedEdge :: edgesByParentId.getOrElse(edge.parentId, List.empty))
      edgesByNodeId.update(edge.nodeId, versionedEdge :: edgesByNodeId.getOrElse(edge.nodeId, List.empty))
    }

    (edgesByParentId, edgesByNodeId)
  }

  private def applyMappingPolicy(duplicates: List[Versioned[Edge[A, Id]]]): Option[Versioned[Edge[A, Id]]] =
    treeConfig.policies.mappingPolicy match {
      case MappingPolicy.Zero() => None
      case MappingPolicy.LastWriteWins() => duplicates.reduceOption(getLastWriteWinner)
      case MappingPolicy.Custom(resolver) => duplicates.reduceOption(getCustomWinner(resolver))
    }

  private def getLastWriteWinner(edge1: Versioned[Edge[A, Id]],
                                 edge2: Versioned[Edge[A, Id]]): Versioned[Edge[A, Id]] = {
    val edge1Metainfo = getEdgeMetainfo(edge1)
    val edge2Metainfo = getEdgeMetainfo(edge2)

    val firstWon =
      if (edge1.vectorTimestamp <-> edge2.vectorTimestamp) // concurrent
        if (edge1Metainfo.systemTimestamp != edge2Metainfo.systemTimestamp)
          edge1Metainfo.systemTimestamp > edge2Metainfo.systemTimestamp // first happened last
        else
          edge1Metainfo.emitterId < edge2Metainfo.emitterId // use one with lesser (alphabetically) emitter id
      else
        edge1.vectorTimestamp > edge2.vectorTimestamp || // first happened last
          !(edge1.vectorTimestamp < edge2.vectorTimestamp) // second happened last

    if (firstWon) edge1 else edge2
  }

  private def getCustomWinner(resolver: ConflictResolver[A, Id])
                             (edge1: Versioned[Edge[A, Id]],
                              edge2: Versioned[Edge[A, Id]]): Versioned[Edge[A, Id]] =
    if (resolver.firstWins(edge1.value.payload, edge1.value.parentId, edge2.value.payload, edge2.value.parentId))
      edge1
    else
      edge2

  private def getEdgeMetainfo(edge: Versioned[Edge[A, Id]]): EdgeMetainfo =
    edgesMetainfo.getOrElse(edge.value.nodeId, Map.empty)(edge.vectorTimestamp)

  private def isRoot(nodeId: Id): Boolean =
    nodeId == treeConfig.rootNodeId

}

object UnorderedTree {

  def apply[A, Id](implicit treeConfig: TreeConfig[A, Id]): UnorderedTree[A, Id] = new UnorderedTree[A, Id]()

  class UnorderedTreeServiceOps[A, Id](implicit treeConfig: TreeConfig[A, Id])
    extends CRDTServiceOps[UnorderedTree[A, Id], Tree[A, Id]] {

    override def zero: UnorderedTree[A, Id] = UnorderedTree[A, Id]

    override def value(crdt: UnorderedTree[A, Id]): Tree[A, Id] = crdt.value

    override def prepare(crdt: UnorderedTree[A, Id], operation: Any): Try[Option[Any]] =
      operation match {
        case CreateChildNodeOp(parentId, nodeId, payload) =>
          crdt.prepareCreateChildNode(coerseId(parentId), coerseId(nodeId), coersePayload(payload))
        case DeleteSubTreeOp(nodeId) =>
          crdt.prepareDeleteSubTree(coerseId(nodeId))
        case _ =>
          super.prepare(crdt, operation)
      }

    override def effect(crdt: UnorderedTree[A, Id], operation: Any, event: DurableEvent): UnorderedTree[A, Id] =
      operation match {
        case CreateChildNodeOpPrepared(_, _, _) =>
          crdt.createChildNode(
            operation.asInstanceOf[CreateChildNodeOpPrepared[Id, A]],
            event.vectorTimestamp,
            EdgeMetainfo(event.emitterId, event.systemTimestamp)
          )
        case DeleteSubTreeOpPrepared(_, _, _) =>
          crdt.deleteSubTree(operation.asInstanceOf[DeleteSubTreeOpPrepared[Id]])
      }

    @inline private def coerseId(id: Any): Id = id.asInstanceOf[Id]
    @inline private def coersePayload(payload: Any): A = payload.asInstanceOf[A]

  }

  implicit def UnorderedTreeServiceOps[A, Id](
    implicit treeConfig: TreeConfig[A, Id]
  ): CRDTServiceOps[UnorderedTree[A, Id], Tree[A, Id]] = new UnorderedTreeServiceOps[A, Id]()

}
