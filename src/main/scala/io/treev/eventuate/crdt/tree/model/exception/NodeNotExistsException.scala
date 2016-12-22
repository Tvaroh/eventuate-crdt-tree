package io.treev.eventuate.crdt.tree.model.exception

case class NodeNotExistsException(nodeId: Any)
  extends RuntimeException(s"Node with id $nodeId doesn't exist")
