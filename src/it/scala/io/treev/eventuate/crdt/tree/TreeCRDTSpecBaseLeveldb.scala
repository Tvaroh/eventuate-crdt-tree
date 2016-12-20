package io.treev.eventuate.crdt.tree

import com.rbmhtechnology.eventuate.{MultiLocationSpecLeveldb, ReplicationEndpoint}
import com.typesafe.config.{Config, ConfigFactory}
import io.treev.eventuate.crdt.tree.model.{Tree, TreeConfig}
import org.scalatest.Matchers
import org.scalatest.concurrent.ScalaFutures

trait TreeCRDTSpecBaseLeveldb
  extends Matchers
    with ScalaFutures
    with MultiLocationSpecLeveldb {

  protected val crdtId = "1"

  protected val customConfig: Config = ConfigFactory.parseString(
    """eventuate.log.write-batch-size = 3
      |eventuate.log.replication.retry-delay = 100ms""".stripMargin
  )

  protected implicit val treeConfig: TreeConfig[String, String] =
    TreeConfig[String, String]("root", "rootPayload")

  protected def service(endpoint: ReplicationEndpoint): TreeCRDTService[String, String] = {
    implicit val system = endpoint.system
    new TreeCRDTService[String, String](endpoint.id, endpoint.logs("L1"))
  }

  protected def noChildren: Set[Tree[String, String]] = Set.empty

}
