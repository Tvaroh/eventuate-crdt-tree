package io.treev.eventuate.crdt.tree.model.internal

import com.rbmhtechnology.eventuate.VectorTime

private[tree]
case class EdgeMetainfo(vectorTimestamp: VectorTime,
                        emitterId: String,
                        systemTimestamp: Long)
