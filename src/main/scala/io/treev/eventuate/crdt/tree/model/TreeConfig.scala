package io.treev.eventuate.crdt.tree.model

/** Tree CRDT configuration. */
case class TreeConfig[A, Id](rootNodeId: Id,
                             rootPayload: A,
                             policies: Policies[A, Id] = Policies.default)

/** Conflicts resolution policies.
  * @param connectionPolicy concurrent addition/removal conflict resolution policy
  * @param sameParentMappingPolicy concurrent addition under same parent conflict resolution policy
  *                                ('custom' policy is applied to payload)
  * @param mappingPolicy concurrent addition under different parents conflict resolution policy
  *                      ('custom' policy is applied to parents ids) */
case class Policies[A, Id](connectionPolicy: ConnectionPolicy,
                           sameParentMappingPolicy: MappingPolicy[A],
                           mappingPolicy: MappingPolicy[Id])
object Policies {
  def default[A, Id]: Policies[A, Id] =
    Policies(ConnectionPolicy.Root, MappingPolicy.LastWriteWins, MappingPolicy.LastWriteWins)
}

/** Concurrent addition/removal conflict resolution policy. */
sealed trait ConnectionPolicy
object ConnectionPolicy {

  /** Drop orphan node. */
  case object Skip extends ConnectionPolicy

  /** Put orphan node under tree root. */
  case object Root extends ConnectionPolicy

}

/** Concurrent addition conflict resolution policy. */
sealed trait MappingPolicy[+T]
object MappingPolicy {

  /** Remove both conflicted nodes. */
  case object Zero extends MappingPolicy[Nothing]

  /** Use "last write wins" conflict resolution policy. */
  case object LastWriteWins extends MappingPolicy[Nothing]

  /** Resolve conflict using user-defined `ConflictResolver` typeclass. */
  case class Custom[T : ConflictResolver]() extends MappingPolicy[T] {
    val resolver: ConflictResolver[T] = implicitly
  }

}

/** Typeclass used to resolve conflicts using custom logic. */
trait ConflictResolver[-T] {

  /** Whether `first` argument wins in conflict resolution. */
  def firstWins(first: T, second: T): Boolean

}
object ConflictResolver {

  def apply[T : ConflictResolver]: ConflictResolver[T] = implicitly

  def instance[T](firstWinsF: (T, T) => Boolean): ConflictResolver[T] =
    (first: T, second: T) => firstWinsF(first, second)

}
