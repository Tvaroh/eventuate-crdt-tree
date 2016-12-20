package io.treev.eventuate.crdt.tree.model.internal

import com.rbmhtechnology.eventuate.VectorTime

private[tree]
case class Edge[A, Id](nodeId: Id,
                       parentId: Id,
                       payload: A,
                       serviceInfo: ServiceInfo)

private[tree]
case class ServiceInfo(vectorTimestamp: VectorTime,
                       emitterId: String,
                       systemTimestamp: Long)
