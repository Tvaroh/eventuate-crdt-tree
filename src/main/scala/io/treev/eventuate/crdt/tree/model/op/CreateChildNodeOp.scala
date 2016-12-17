package io.treev.eventuate.crdt.tree.model.op

case class CreateChildNodeOp(parentId: Any, nodeId: Any, payload: Any)

case class CreateChildNodeOpPrepared[Id, A](parentId: Id, nodeId: Id, payload: A)
