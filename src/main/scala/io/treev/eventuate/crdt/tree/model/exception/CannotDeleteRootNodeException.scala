package io.treev.eventuate.crdt.tree.model.exception

case class CannotDeleteRootNodeException(rootNodeId: Any)
  extends RuntimeException(s"Cannot delete root node with id $rootNodeId")
