package io.treev.eventuate.crdt.tree

import org.scalatest.WordSpec

class TreeCRDTChaosSpecLeveldb extends WordSpec with TreeCRDTSpecBaseLeveldb {
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

}
