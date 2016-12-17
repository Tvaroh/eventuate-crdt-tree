package io.treev.eventuate.crdt.tree

case class Tree[A, Id](nodeId: Id,
                       payload: A,
                       children: Set[Tree[A, Id]])
