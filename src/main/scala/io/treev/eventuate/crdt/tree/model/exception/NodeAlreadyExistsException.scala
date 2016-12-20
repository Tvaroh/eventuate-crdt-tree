package io.treev.eventuate.crdt.tree.model.exception

case class NodeAlreadyExistsException(nodeId: Any)
  extends RuntimeException(s"Node with id $nodeId already exists")
