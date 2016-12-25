package io.treev.eventuate.crdt.tree.model.internal

import com.rbmhtechnology.eventuate.VectorTime

private[tree]
case class ServiceInfo(vectorTimestamp: VectorTime,
                       emitterId: String,
                       systemTimestamp: Long)
