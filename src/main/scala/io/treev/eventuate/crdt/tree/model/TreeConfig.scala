package io.treev.eventuate.crdt.tree.model

case class TreeConfig[A, Id](rootNodeId: Id,
                             rootPayload: A)
