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

  def routees(num: Int = 10)(implicit system: ActorSystem) = (1 to num).map(_ ⇒ routee)

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

  def performanceLogsOf(p: (PoolSize, Long)*): SizePerformanceLog = {
    p.zipWithIndex.map {
      case ((size, speed), idx) ⇒
        SizePerformanceLogEntry(size, JDuration.ofMillis(speed), LocalDateTime.now.minusSeconds(idx))
    }.toVector
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

      Thread.sleep(10)
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
      val streakStart = resizer.utilizationRecord.underutilizationStreakStart.get
      streakStart.isBefore(LocalDateTime.now) should be(true)
      resizer.utilizationRecord.highestUtilization should be(1) //the last mock send only hits routee after the record is updated.

      Thread.sleep(20)

      router.mockSend(index = 0)
      resizer.utilizationRecord.highestUtilization should be(2)
      resizer.utilizationRecord.underutilizationStreakStart.get should be(streakStart)
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
      resizer.utilizationRecord.underutilizationStreakStart shouldBe empty
      router.close()
    }

  }

  "PerformanceBasedResizer resize consolidateLogs" must {

    "consolidate recentProcessingLogs into a performance log entry" in {
      val resizer = PerformanceBasedResizer()
      resizer.recentProcessingLog = Vector(
        RecentProcessingLogEntry(10, 4, 2, 10, LocalDateTime.now),
        RecentProcessingLogEntry(10, 4, 1, 10, LocalDateTime.now.minusSeconds(1)),
        RecentProcessingLogEntry(10, 4, 2, 10, LocalDateTime.now.minusSeconds(2)))
      resizer.resize(routees())

      resizer.performanceLog.length should be(1)
      resizer.performanceLog.head.poolSize should be(10)
      resizer.performanceLog.head.processSpeed.toMillis.toInt should be(400 +- 1)
    }

    "ignore old processing logs entries when at different pool size" in {
      val resizer = PerformanceBasedResizer()
      resizer.recentProcessingLog = Vector(
        RecentProcessingLogEntry(10, 4, 2, 10, LocalDateTime.now),
        RecentProcessingLogEntry(10, 4, 2, 10, LocalDateTime.now.minusSeconds(1)),
        RecentProcessingLogEntry(9, 4, 100, 10, LocalDateTime.now.minusSeconds(2)))

      resizer.resize(routees())

      resizer.performanceLog.head.poolSize should be(10)
      resizer.performanceLog.head.processSpeed.toMillis.toInt should be(250 +- 1)
    }

    "ignore none-fully utilized logs" in {
      val resizer = PerformanceBasedResizer()
      resizer.recentProcessingLog = Vector(
        RecentProcessingLogEntry(10, 4, 2, 9, LocalDateTime.now),
        RecentProcessingLogEntry(10, 4, 2, 9, LocalDateTime.now.minusSeconds(1)))

      resizer.resize(routees())

      resizer.performanceLog shouldBe empty
    }

    "remove old performance log entry that is no longer relevant" in {
      val resizer = PerformanceBasedResizer(retentionPeriod = JDuration.ofHours(24))
      resizer.performanceLog = Vector(SizePerformanceLogEntry(19, JDuration.ofMillis(10), LocalDateTime.now.minusHours(25)))
      resizer.recentProcessingLog = Vector(
        RecentProcessingLogEntry(10, 4, 2, 10, LocalDateTime.now),
        RecentProcessingLogEntry(10, 4, 2, 10, LocalDateTime.now.minusSeconds(1)))

      resizer.resize(routees())

      resizer.performanceLog.length should be(1)
      resizer.performanceLog.head.poolSize should be(10)

    }
  }

  "PerformanceBasedResizer resize" must {
    "downsize to close to the highest retention when a streak of underutilization started downsizeAfterUnderutilizedFor" in {
      val resizer = PerformanceBasedResizer(
        downsizeAfterUnderutilizedFor = JDuration.ofHours(72),
        bufferRatio = 0.25)
      resizer.utilizationRecord = UtilizationRecord(underutilizationStreakStart = Some(LocalDateTime.now.minusHours(73)), highestUtilization = 8)
      resizer.resize(routees(20)) should be(8 * 1.25 - 20)
    }

    "does not downsize on empty history" in {
      val resizer = PerformanceBasedResizer()
      resizer.resize(routees()) should be(0)
    }

    "always go to lowerBound if below it" in {
      val resizer = PerformanceBasedResizer(lowerBound = 50, upperBound = 100)
      resizer.resize(routees(20)) should be(30)
    }

    "always go to uppperBound if above it" in {
      val resizer = PerformanceBasedResizer(upperBound = 50)
      resizer.resize(routees(80)) should be(-30)
    }

    "explore when there is performance log but not go beyond exploreStepSize" in {
      val resizer = PerformanceBasedResizer(exploreStepSize = 0.3, explorationRatio = 1)
      resizer.performanceLog = performanceLogsOf((11, 1), (13, 1), (12, 3))

      val rts = routees(10)
      val exploreSamples = (1 to 100).map(_ ⇒ resizer.resize(rts))
      exploreSamples.forall(change ⇒ Math.abs(change) >= 1 && Math.abs(change) <= (10 * 0.3)) should be(true)

    }
  }

  "PerformanceBasedResizer optimize" must {
    "optimize towards the fastest pool size" in {
      val resizer = PerformanceBasedResizer(explorationRatio = 0)
      resizer.performanceLog = performanceLogsOf((7, 5), (10, 3), (11, 2), (12, 4))
      resizer.resize(routees(10)) should be(1)
      resizer.resize(routees(12)) should be(-1)
      resizer.resize(routees(7)) should be(2)
    }

    "ignore further away sample data when optmizing" in {
      val resizer = PerformanceBasedResizer(explorationRatio = 0, numOfAdjacentSizesToConsiderDuringOptimization = 4)

      resizer.performanceLog = performanceLogsOf(
        (7, 5),
        (8, 2),
        (10, 3),
        (11, 4),
        (12, 3),
        (13, 1))

      resizer.resize(routees(10)) should be(-1)
    }

  }

}
