package io.treev.eventuate.crdt.tree

import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.atomic.AtomicInteger

import akka.testkit.TestProbe
import com.rbmhtechnology.eventuate.{Location, MultiLocationSpecLeveldb, ReplicationEndpoint}
import com.typesafe.config.{Config, ConfigFactory}
import io.treev.eventuate.crdt.tree.TestHelpers._
import io.treev.eventuate.crdt.tree.model._
import io.treev.eventuate.crdt.tree.model.op._
import org.scalatest.{Assertion, AsyncWordSpec, Matchers}

import scala.collection.immutable.Seq
import scala.concurrent.Future
import scala.util.Random

class UnorderedTreeChaosISpecLeveldb extends AsyncWordSpec with Matchers with MultiLocationSpecLeveldb {
  import com.rbmhtechnology.eventuate.ReplicationIntegrationSpec.replicationConnection

  "A replicated UnorderedTree" must {

    def convergeConcurrentAdditionsSameParent(mappingPolicy: MappingPolicy[Payload, Id]): Future[Assertion] = {
      implicit val config = treeConfig.copy(policies = treeConfig.policies.copy(mappingPolicy = mappingPolicy))
      val setup = new TestSetup; import setup._

      val (parentId, parentPayload) = node(0)

      def batchAdd(service: UnorderedTreeService[Payload, Id]): Future[Unit] =
        batch(service) { service =>
          val (nodeId, payload) = randomNode(10, 20)
          maskErrors(service.createChildNode(crdtId, parentId, nodeId, payload))
        }

      for {
        _ <- serviceA.createChildNode(crdtId, treeConfig.rootNodeId, parentId, parentPayload)
        _ <- Future.sequence(services.map(start))
        _ <- Future.sequence(services.map(service => batchAdd(service).flatMap(_ => stop(service))))
      } yield {
        probes.map(_.expectMsg(Messages.started))

        val values = probes.map(getValue)
        values.map(_.normalize).distinct.size should be (1) // should converge to same value
      }
    }

    "converge under concurrent additions to the same parent when Zero mapping policy is used" in {
      convergeConcurrentAdditionsSameParent(MappingPolicy.Zero())
    }

    "converge under concurrent additions to the same parent when Custom mapping policy is used" in {
      convergeConcurrentAdditionsSameParent(customMappingPolicy)
    }

    def convergeConcurrentAdditionsDifferentParents(mappingPolicy: MappingPolicy[Payload, Id]): Future[Assertion] = {
      implicit val config = treeConfig.copy(policies = treeConfig.policies.copy(mappingPolicy = mappingPolicy))
      val setup = new TestSetup; import setup._

      val parentNumbers = 1 to 5
      val parentNodes = parentNumbers.map(node)

      def batchAdd(service: UnorderedTreeService[Payload, Id]): Future[Unit] =
        batch(service) { service =>
          val parentId = parentNodes(Random.nextInt(parentNodes.size))._1
          val (nodeId, payload) = randomNode(10, 20)
          maskErrors(service.createChildNode(crdtId, parentId, nodeId, payload))
        }

      for {
        _ <- Future.sequence {
          parentNodes.map { case (nodeId, payload) =>
            serviceA.createChildNode(crdtId, treeConfig.rootNodeId, nodeId, payload)
          }
        }
        _ <- Future.sequence(services.map(start))
        _ <- Future.sequence(services.map(service => batchAdd(service).flatMap(_ => stop(service))))
      } yield {
        probes.map(_.expectMsg(Messages.started))

        val values = probes.map(getValue)
        values.map(_.normalize).distinct.size should be (1) // should converge to same value
      }
    }

    "converge under concurrent additions to different parents when Zero mapping policy is used" in {
      convergeConcurrentAdditionsDifferentParents(MappingPolicy.Zero[Payload, Id]())
    }

    "converge under concurrent additions to different parents when Custom mapping policy is used" in {
      convergeConcurrentAdditionsDifferentParents(customMappingPolicy)
    }

    def convergeConcurrentAdditionRemoval(connectionPolicy: ConnectionPolicy): Future[Assertion] = {
      implicit val config = treeConfig.copy(policies = treeConfig.policies.copy(connectionPolicy = connectionPolicy))
      val setup = new TestSetup; import setup._

      def batchAddRemove(service: UnorderedTreeService[Payload, Id]): Future[Unit] =
        batch(service) { service =>
          val (parentId, parentPayload) = node(0)
          val (nodeId, payload) = randomNode(1, 3)

          for {
            _ <- maskErrors(service.createChildNode(crdtId, treeConfig.rootNodeId, parentId, parentPayload))
            _ <- maskErrors(service.createChildNode(crdtId, parentId, nodeId, payload))
            _ <- maskErrors(service.deleteSubTree(crdtId, parentId))
          } yield ()
        }

      for {
        _ <- Future.sequence(services.map(start))
        _ <- Future.sequence(services.map(service => batchAddRemove(service).flatMap(_ => stop(service))))
      } yield {
        probes.map(_.expectMsg(Messages.started))

        val values = probes.map(getValue)
        values.map(_.normalize).distinct.size should be (1) // should converge to same value
      }
    }

    "converge under concurrent addition/deletion when Skip connection policy is used" in {
      convergeConcurrentAdditionRemoval(ConnectionPolicy.Skip)
    }

  }

  private class TestSetup(implicit treeConfig: TreeConfig[Payload, Id]) {
    val numLocations = 4

    val locationA: Location = location("A", customConfig = customConfig)
    val locationB: Location = location("B", customConfig = customConfig)
    val locationC: Location = location("C", customConfig = customConfig)
    val locationD: Location = location("D", customConfig = customConfig)

    def endpoint(location: Location)(replicationLocations: Location*): ReplicationEndpoint =
      location.endpoint(Set("L1"), replicationLocations.map(location => replicationConnection(location.port)).toSet)

    val endpointA: ReplicationEndpoint = endpoint(locationA)(locationB, locationC, locationD)
    val endpointB: ReplicationEndpoint = endpoint(locationB)(locationA)
    val endpointC: ReplicationEndpoint = endpoint(locationC)(locationA)
    val endpointD: ReplicationEndpoint = endpoint(locationD)(locationA)

    val (serviceA, probeA) = service(endpointA, numLocations)
    val (serviceB, probeB) = service(endpointB, numLocations)
    val (serviceC, probeC) = service(endpointC, numLocations)
    val (serviceD, probeD) = service(endpointD, numLocations)

    val services = Seq(serviceA, serviceB, serviceC, serviceD)
    val probes = Seq(probeA, probeB, probeC, probeD)

    def maskErrors[T](f: Future[T]): Future[Unit] =
      f.map(_ => ()).recover { case _ => () }

    def batch(service: UnorderedTreeService[Payload, Id])
             (op: => UnorderedTreeService[Payload, Id] => Future[Unit]): Future[Unit] =
      (1 to ThreadLocalRandom.current.nextInt(maxBatchSize / 2, maxBatchSize))
        .foldLeft(Future.successful(())) {
          case (acc, _) => acc.flatMap(_ => op(service))
        }

    def message(service: UnorderedTreeService[Payload, Id], message: String): Future[Unit] = {
      val nodeId = s"$message-${service.serviceId}"
      maskErrors(service.createChildNode(crdtId, treeConfig.rootNodeId, nodeId, nodeId))
    }

    def start(service: UnorderedTreeService[Payload, Id]): Future[Unit] =
      message(service, Messages.start)
    def stop(service: UnorderedTreeService[Payload, Id]): Future[Unit] =
      message(service, Messages.stop)

    def getValue(probe: TestProbe): Tree[Payload, Id] =
      probe.expectMsgClass(classOf[Tree[Payload, Id]])
  }

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
                     (implicit treeConfig: TreeConfig[Payload, Id]): (UnorderedTreeService[Payload, Id], TestProbe) = {
    implicit val system = endpoint.system

    val probe = TestProbe()
    val service = new UnorderedTreeService[Payload, Id](endpoint.id, endpoint.logs("L1")) {
      val startCounter = new AtomicInteger()
      val stopCounter = new AtomicInteger()

      override def onChange(crdt: UnorderedTree[Payload, Id], operation: Any): Unit = {
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

  private def randomNode(minId: Int, maxId: Int): (Id, Payload) = {
    val number = ThreadLocalRandom.current.nextInt(minId, maxId + 1)
    node(number)
  }

  private def customMappingPolicy: MappingPolicy[Payload, Id] =
    MappingPolicy.Custom { (payload1, parentId1, payload2, parentId2) =>
      if (payload1 != payload2) payload1 < payload2
      else parentId1 < parentId2
    }

}
