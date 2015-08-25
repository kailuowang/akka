package akka.routing

import java.time.{ LocalDateTime, Duration ⇒ JDuration }

import akka.actor._
import akka.testkit._
import akka.testkit.TestEvent._

import PerformanceBasedResizer._
import PerformanceBasedResizerSpec._

import scala.collection.immutable
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.concurrent.duration.FiniteDuration
import scala.util.Random

object PerformanceBasedResizerSpec {

  class TestActor(timeout: FiniteDuration = 1000.milliseconds) extends Actor {
    def receive = {
      case latch: TestLatch ⇒
        Await.ready(latch, timeout)
    }
  }

  def routee(implicit system: ActorSystem): Routee =
    ActorRefRoutee(system.actorOf(Props(new TestActor)))

  case class TestRouter(routees: Vector[Routee], resizer: Resizer)(implicit system: ActorSystem) {
    var msgs: Set[TestLatch] = Set()
    def mockSend(l: TestLatch = TestLatch(), index: Int = Random.nextInt(routees.length))(implicit sender: ActorRef): TestLatch = {
      resizer.onMessageForwardedToRoutee(routees)
      routees(index).send(l, sender)
      msgs = msgs + l
      l
    }

    def close(): Unit = msgs.foreach(_.open())
  }

}

@org.junit.runner.RunWith(classOf[org.scalatest.junit.JUnitRunner])
class PerformanceBasedResizerSpec extends AkkaSpec(ResizerSpec.config) with DefaultTimeout with ImplicitSender {

  "PerformanceBasedResizer isTimeForResize" must {

    "be false with empty history" in {
      val resizer = PerformanceBasedResizer()
      resizer.isTimeForResize(100) should ===(false)
    }

    "be false without enough history" in {
      val resizer = PerformanceBasedResizer(actionFrequency = JDuration.ofSeconds(10))
      resizer.performanceLog = Vector(SizePerformanceLogEntry(10, JDuration.ofMillis(1), LocalDateTime.now.minusSeconds(8)),
        SizePerformanceLogEntry(6, JDuration.ofMillis(1), LocalDateTime.now.minusSeconds(5)))

      resizer.isTimeForResize(100) should ===(false)
    }

  }

  "PerformanceBasedResizer onMessageForwardedToRoutee" must {

    "record recent processProcessingLog with correct routee size" in {
      val resizer = PerformanceBasedResizer()
      resizer.onMessageForwardedToRoutee(Vector(routee))
      resizer.recentProcessingLog.head.numOfRoutees should ===(1)
    }

    "record recent processProcessingLog with correct occupied routee size" in {
      val resizer = PerformanceBasedResizer()
      val rt1 = routee
      val l = TestLatch(2)
      rt1.send(l, self)
      val rt2 = routee
      resizer.onMessageForwardedToRoutee(Vector(rt1, rt2))
      resizer.recentProcessingLog.head.numOfRoutees should ===(2)
      resizer.recentProcessingLog.head.occupiedRoutees should ===(1)

      l.open()
    }

    "record recent processProcessingLog with correct queue length" in {
      val resizer = PerformanceBasedResizer()
      val router = TestRouter(Vector(routee, routee), resizer)

      router.mockSend()
      router.mockSend()
      router.mockSend()

      resizer.recentProcessingLog.head.queueLength should ===(3)

      router.close()
    }

    "record recent processing with correct processed messages" in {
      val resizer = PerformanceBasedResizer(historySampleRate = JDuration.ofNanos(0))
      val router = TestRouter(Vector(routee, routee), resizer)
      val latch1 = router.mockSend()
      router.mockSend(latch1)
      router.mockSend()

      resizer.recentProcessingLog.head.processed should ===(0)

      latch1.open()

      Thread.sleep(40)

      router.mockSend()

      resizer.recentProcessingLog.head.processed should ===(2)

      router.close()
    }

    "congregate immediate processing into the same log" in {
      val resizer = PerformanceBasedResizer(historySampleRate = JDuration.ofSeconds(10))
      val router = TestRouter(Vector(routee), resizer)

      router.mockSend()
      router.mockSend()

      resizer.recentProcessingLog.length should ===(2)

      router.mockSend()

      resizer.recentProcessingLog.length should ===(2)

      router.close()
    }

    "congregate immediate processing into the same log with correct processed message" in {
      val resizer = PerformanceBasedResizer(historySampleRate = JDuration.ofSeconds(10))
      val router = TestRouter(Vector(routee), resizer)

      val l1 = router.mockSend()
      resizer.recentProcessingLog.head.processed should ===(0)

      l1.open()
      Thread.sleep(10)

      val l2 = router.mockSend()
      resizer.recentProcessingLog.head.processed should ===(1)

      l2.open()
      Thread.sleep(10)

      router.mockSend()
      resizer.recentProcessingLog.head.processed should ===(2)

      router.close()

    }

    "collect utilizationRecord when not fully utilized" in {
      val resizer = PerformanceBasedResizer(historySampleRate = JDuration.ofSeconds(10))
      val router = TestRouter(Vector(routee, routee, routee), resizer)
      router.mockSend(index = 0)
      Thread.sleep(10)

      router.mockSend(index = 1)
      resizer.utilizationRecord.lastFullyUtilized shouldBe empty
      resizer.utilizationRecord.highestUtilization should be(1) //the last mock send only hits routee after the record is updated.

      router.close()
    }

    "collect utilizationRecord when fully utilized" in {
      val resizer = PerformanceBasedResizer(historySampleRate = JDuration.ofSeconds(10))
      val router = TestRouter(Vector(routee, routee), resizer)
      router.mockSend(index = 0)
      Thread.sleep(10)

      router.mockSend(index = 1)
      Thread.sleep(10)
      router.mockSend()
      resizer.utilizationRecord.lastFullyUtilized.get.isAfter(LocalDateTime.now.minusSeconds(1)) should be(true)
      router.close()
    }

  }
}
