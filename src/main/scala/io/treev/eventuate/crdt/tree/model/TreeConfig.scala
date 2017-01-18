package io.treev.eventuate.crdt.tree.model

/** Tree CRDT configuration. */
case class TreeConfig[A, Id](rootNodeId: Id,
                             rootPayload: A,
                             policies: Policies[A, Id] = Policies.default[A, Id])

/** Conflicts resolution policies.
  * @param connectionPolicy concurrent addition/removal conflict resolution policy
  * @param mappingPolicy concurrent addition conflict resolution policy */
case class Policies[A, Id](connectionPolicy: ConnectionPolicy,
                           mappingPolicy: MappingPolicy[A, Id])
object Policies {
  def default[A, Id]: Policies[A, Id] =
    Policies(ConnectionPolicy.Skip, MappingPolicy.LastWriteWins[A, Id]())
}

/** Concurrent addition/removal conflict resolution policy. */
sealed trait ConnectionPolicy
object ConnectionPolicy {

  /** Drop orphan node. */
  case object Skip extends ConnectionPolicy

}

/** Concurrent addition conflict resolution policy. */
sealed trait MappingPolicy[A, Id]
object MappingPolicy {

  /** Remove both conflicted nodes. */
  case class Zero[A, Id]() extends MappingPolicy[A, Id]

  /** Use "last write wins" conflict resolution policy. */
  case class LastWriteWins[A, Id]() extends MappingPolicy[A, Id]

  /** Resolve conflict using user-defined `ConflictResolver` typeclass.
    * @tparam A payload type */
  case class Custom[A, Id](resolver: ConflictResolver[A, Id]) extends MappingPolicy[A, Id]

}

/** Typeclass used to resolve conflicts using custom logic. */
trait ConflictResolver[A, Id] {

  /** Whether first candidate wins in conflict resolution. */
  def firstWins(firstPayload: A, firstParentId: Id, secondPayload: A, secondParentId: Id): Boolean

}
object ConflictResolver {

  def instance[A, Id](firstWinsF: (A, Id, A, Id) => Boolean): ConflictResolver[A, Id] =
    (firstPayload: A, firstParentId: Id, secondPayload: A, secondParentId: Id) =>
      firstWinsF(firstPayload, firstParentId, secondPayload, secondParentId)

}
