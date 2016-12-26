package io.treev.eventuate.crdt.tree

import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.atomic.AtomicInteger

import akka.testkit.TestProbe
import com.rbmhtechnology.eventuate.{MultiLocationSpecLeveldb, ReplicationEndpoint}
import com.typesafe.config.{Config, ConfigFactory}
import io.treev.eventuate.crdt.tree.model.{ConflictResolver, MappingPolicy, Tree, TreeConfig}
import io.treev.eventuate.crdt.tree.model.op.CreateChildNodeOpPrepared
import org.scalatest.{Assertion, AsyncWordSpec, Matchers}

import scala.collection.immutable.Seq
import scala.concurrent.Future

class TreeCRDTChaosSpecLeveldb extends AsyncWordSpec with Matchers with MultiLocationSpecLeveldb {
  import com.rbmhtechnology.eventuate.ReplicationIntegrationSpec.replicationConnection

  "A replicated TreeCRDT" must {

    def convergeConcurrentAdditionsSameParent(mappingPolicy: MappingPolicy[Payload]): Future[Assertion] = {
      implicit val config = treeConfig.copy(policies = treeConfig.policies.copy(mappingPolicy = mappingPolicy))

      val numLocations = 4

      val locationA = location("A", customConfig = customConfig)
      val locationB = location("B", customConfig = customConfig)
      val locationC = location("C", customConfig = customConfig)
      val locationD = location("D", customConfig = customConfig)

      val endpointA = locationA.endpoint(Set("L1"), Set(replicationConnection(locationB.port), replicationConnection(locationC.port), replicationConnection(locationD.port)))
      val endpointB = locationB.endpoint(Set("L1"), Set(replicationConnection(locationA.port)))
      val endpointC = locationC.endpoint(Set("L1"), Set(replicationConnection(locationA.port)))
      val endpointD = locationD.endpoint(Set("L1"), Set(replicationConnection(locationA.port)))

      val (serviceA, probeA) = service(endpointA, numLocations)
      val (serviceB, probeB) = service(endpointB, numLocations)
      val (serviceC, probeC) = service(endpointC, numLocations)
      val (serviceD, probeD) = service(endpointD, numLocations)

      val services = Seq(serviceA, serviceB, serviceC, serviceD)
      val probes = Seq(probeA, probeB, probeC, probeD)

      val (node0Id, _) = node(0)

      def batchAdd(service: TreeCRDTService[Payload, Id]): Future[Unit] =
        (1 to ThreadLocalRandom.current.nextInt(maxBatchSize / 2, maxBatchSize))
          .foldLeft(Future.successful(())) {
            case (acc, _) =>
              val (nodeId, payload) = randomNode(1, 8)
              acc.flatMap(_ => createChildNodeMaskingErrors(service, node0Id, nodeId, payload))
          }

      for {
        _ <- Future.sequence(services.map(start))
        _ <- Future.sequence(services.map(service => batchAdd(service).flatMap(_ => stop(service))))
      } yield {
        probeA.expectMsg(Messages.started)
        probeB.expectMsg(Messages.started)
        probeC.expectMsg(Messages.started)
        probeD.expectMsg(Messages.started)

        probes.map(getValue).distinct.size should be (1) // should converge to same value
      }
    }

    "converge under concurrent additions to the same parent when Zero mapping policy is used" in {
      convergeConcurrentAdditionsSameParent(MappingPolicy.Zero)
    }

    "converge under concurrent additions to the same parent when LastWriteWins mapping policy is used" in {
      convergeConcurrentAdditionsSameParent(MappingPolicy.LastWriteWins)
    }

    "converge under concurrent additions to the same parent when Custom mapping policy is used" in {
      implicit val resolver = ConflictResolver.instance[Payload]((p1, p2) => p1 < p2)
      convergeConcurrentAdditionsSameParent(MappingPolicy.Custom())
    }

  }

  private type Id = String
  private type Payload = String

  private val crdtId = "1"
  private val maxBatchSize = 100

  private object Messages {
    val start = "start"
    val stop = "stop"
    val started = "started"
  }

  private val treeConfig: TreeConfig[Payload, Id] =
    TreeConfig[Payload, Id]("root", "root's payload")

  private val customConfig: Config = ConfigFactory.parseString(
    """eventuate.log.write-batch-size = 3
      |eventuate.log.replication.retry-delay = 100ms""".stripMargin
  )

  private def service(endpoint: ReplicationEndpoint, numLocations: Int)
                     (implicit treeConfig: TreeConfig[Payload, Id]): (TreeCRDTService[Payload, Id], TestProbe) = {
    implicit val system = endpoint.system

    val probe = TestProbe()
    val service = new TreeCRDTService[Payload, Id](endpoint.id, endpoint.logs("L1")) {
      val startCounter = new AtomicInteger()
      val stopCounter = new AtomicInteger()

      override def onChange(crdt: TreeCRDT[Payload, Id], operation: Any): Unit = {
        operation match {
          case CreateChildNodeOpPrepared(_ , _, payload: Payload) if payload startsWith Messages.start =>
            startCounter.incrementAndGet()
          case CreateChildNodeOpPrepared(_ , _, payload: Payload) if payload startsWith Messages.stop =>
            stopCounter.incrementAndGet()
          case _ => ()
        }

        if (startCounter.get == numLocations) {
          probe.ref ! Messages.started
          startCounter.set(0)
        }

        if (stopCounter.get == numLocations) {
          probe.ref ! crdt.value
          stopCounter.set(0)
        }
      }
    }

    (service, probe)
  }

  private def node(number: Int): (Id, Payload) =
    (s"child$number", s"child$number's payload")

  private def randomNode(minId: Int, maxId: Int): (Id, Payload) = {
    val number = ThreadLocalRandom.current.nextInt(minId, maxId + 1)
    node(number)
  }

  private def createChildNodeMaskingErrors(service: TreeCRDTService[Payload, Id],
                                           parentId: Id,
                                           nodeId: Id,
                                           payload: Payload): Future[Unit] =
    service.createChildNode(crdtId, parentId, nodeId, payload).map(_ => ()).recover { case _ => () }

  private def message(service: TreeCRDTService[Payload, Id], message: String): Future[Unit] = {
    val nodeId = s"$message-${service.serviceId}"
    createChildNodeMaskingErrors(service, treeConfig.rootNodeId, nodeId, nodeId)
  }

  private def start(service: TreeCRDTService[Payload, Id]): Future[Unit] =
    message(service, Messages.start)
  private def stop(service: TreeCRDTService[Payload, Id]): Future[Unit] =
    message(service, Messages.stop)

  private def getValue(probe: TestProbe): Tree[Payload, Id] =
    probe.expectMsgClass(classOf[Tree[Payload, Id]])

}
