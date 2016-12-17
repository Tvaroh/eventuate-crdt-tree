package io.treev.eventuate.crdt.tree

private[tree]
case class Edge[A, Id](nodeId: Id,
                       parentId: Id,
                       payload: A)
