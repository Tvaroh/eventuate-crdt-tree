package io.treev.eventuate.crdt.tree.model

case class Tree[A, Id](nodeId: Id,
                       payload: A,
                       children: Set[Tree[A, Id]] = Set.empty[Tree[A, Id]])
