package io.treev.eventuate.crdt.tree.model

/** Tree CRDT configuration. */
case class TreeConfig[A, Id](rootNodeId: Id,
                             rootPayload: A,
                             policies: Policies[A] = Policies.default[A])

/** Conflicts resolution policies.
  * @param connectionPolicy concurrent addition/removal conflict resolution policy
  * @param mappingPolicy concurrent addition conflict resolution policy */
case class Policies[A](connectionPolicy: ConnectionPolicy,
                       mappingPolicy: MappingPolicy[A])
object Policies {
  def default[A]: Policies[A] =
    Policies(ConnectionPolicy.Root, MappingPolicy.LastWriteWins)
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
sealed trait MappingPolicy[+A]
object MappingPolicy {

  /** Remove both conflicted nodes. */
  case object Zero extends MappingPolicy[Nothing]

  /** Use "last write wins" conflict resolution policy. */
  case object LastWriteWins extends MappingPolicy[Nothing]

  /** Resolve conflict using user-defined `ConflictResolver` typeclass.
    * @tparam A payload type */
  case class Custom[A : ConflictResolver]() extends MappingPolicy[A] {
    val resolver: ConflictResolver[A] = implicitly
  }

}

/** Typeclass used to resolve conflicts using custom logic. */
trait ConflictResolver[-T] {

  /** Whether `first` argument wins in conflict resolution. */
  def firstWins(first: T, second: T): Boolean

}
object ConflictResolver {

  def apply[A : ConflictResolver]: ConflictResolver[A] = implicitly

  def instance[A](firstWinsF: (A, A) => Boolean): ConflictResolver[A] =
    (first: A, second: A) => firstWinsF(first, second)

}
