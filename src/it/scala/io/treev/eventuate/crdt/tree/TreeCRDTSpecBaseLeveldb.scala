package io.treev.eventuate.crdt.tree

import com.typesafe.config.{Config, ConfigFactory}
import io.treev.eventuate.crdt.tree.model.{Tree, TreeConfig}
import org.scalatest.Matchers

trait TreeCRDTSpecBaseLeveldb extends Matchers {

  protected val crdtId = "1"

  protected val customConfig: Config = ConfigFactory.parseString(
    """eventuate.log.write-batch-size = 3
      |eventuate.log.replication.retry-delay = 100ms""".stripMargin
  )

  protected implicit val treeConfig: TreeConfig[String, String] =
    TreeConfig[String, String]("root", "rootPayload")

  protected def noChildren: Set[Tree[String, String]] = Set.empty

}
