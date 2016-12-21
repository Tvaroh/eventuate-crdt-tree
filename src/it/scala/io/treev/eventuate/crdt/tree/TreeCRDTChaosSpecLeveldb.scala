package io.treev.eventuate.crdt.tree

import com.rbmhtechnology.eventuate.{MultiLocationSpecLeveldb, ReplicationEndpoint}
import com.typesafe.config.{Config, ConfigFactory}
import io.treev.eventuate.crdt.tree.model.TreeConfig
import org.scalatest.{Matchers, WordSpec}

class TreeCRDTChaosSpecLeveldb extends WordSpec with Matchers with MultiLocationSpecLeveldb {
  import com.rbmhtechnology.eventuate.ReplicationIntegrationSpec.replicationConnection

  "A replicated TreeCRDT" must {
    "converge under concurrent updates and write failures" in {
      val locationA = location("A", customConfig = customConfig)
      val locationB = location("B", customConfig = customConfig)
      val locationC = location("C", customConfig = customConfig)
      val locationD = location("D", customConfig = customConfig)

      val endpointA = locationA.endpoint(Set("L1"), Set(replicationConnection(locationB.port), replicationConnection(locationC.port), replicationConnection(locationD.port)))
      val endpointB = locationB.endpoint(Set("L1"), Set(replicationConnection(locationA.port)))
      val endpointC = locationC.endpoint(Set("L1"), Set(replicationConnection(locationA.port)))
      val endpointD = locationD.endpoint(Set("L1"), Set(replicationConnection(locationA.port)))

      val serviceA = service(endpointA)
      val serviceB = service(endpointB)
      val serviceC = service(endpointC)
      val serviceD = service(endpointD)

      // TODO
    }
  }

  private implicit val treeConfig: TreeConfig[String, String] =
    TreeConfig[String, String]("root", "root's payload")

  private val customConfig: Config = ConfigFactory.parseString(
    """eventuate.log.write-batch-size = 3
      |eventuate.log.replication.retry-delay = 100ms""".stripMargin
  )

  private def service(endpoint: ReplicationEndpoint): TreeCRDTService[String, String] = {
    implicit val system = endpoint.system
    new TreeCRDTService[String, String](endpoint.id, endpoint.logs("L1"))
  }

}
