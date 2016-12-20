package io.treev.eventuate.crdt.tree.model.exception

case class ParentNodeNotExistsException(parentId: Any)
  extends RuntimeException(s"Parent node with id $parentId doesn't exist")
