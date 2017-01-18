package io.treev.eventuate.crdt.tree.model.exception

case class NodeIsParentOfItselfException(nodeId: Any)
  extends RuntimeException(s"Node with id $nodeId cannot be parent of itself")
