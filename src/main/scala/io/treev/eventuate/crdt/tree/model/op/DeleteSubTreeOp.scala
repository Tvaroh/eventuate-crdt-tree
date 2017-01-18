package io.treev.eventuate.crdt.tree.model.op

import com.rbmhtechnology.eventuate.VectorTime

case class DeleteSubTreeOp(nodeId: Any)

case class DeleteSubTreeOpPrepared[Id](nodeId: Id,
                                       nodeIds: Set[Id],
                                       timestamps: Set[VectorTime])
