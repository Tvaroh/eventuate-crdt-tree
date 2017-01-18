package io.treev.eventuate.crdt.tree.model

case class Tree[A, Id](nodeId: Id,
                       payload: A,
                       children: Seq[Tree[A, Id]] = Seq.empty[Tree[A, Id]])
