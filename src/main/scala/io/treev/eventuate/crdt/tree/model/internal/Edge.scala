package io.treev.eventuate.crdt.tree.model.internal

private[tree]
case class Edge[A, Id](nodeId: Id,
                       parentId: Id,
                       payload: A,
                       systemTimestamp: Long,
                       emitterId: String)
