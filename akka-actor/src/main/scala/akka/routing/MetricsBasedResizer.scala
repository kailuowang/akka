/**
 * Copyright (C) 2009-2015 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.routing

import java.time.{ Duration, LocalDateTime }
import java.time.temporal.{ Temporal, ChronoUnit }

import MetricsBasedResizer._

import scala.collection.immutable

import com.typesafe.config.Config

import akka.actor._

import scala.util.Random

case object MetricsBasedResizer {
  type PoolSize = Int

  case class RecentProcessingLogEntry(numOfRoutees: PoolSize, queueLength: Int, processed: Int, occupiedRoutees: Int, time: LocalDateTime) {
    def fullyUtilized = numOfRoutees == occupiedRoutees

    def aggregate(that: RecentProcessingLogEntry): RecentProcessingLogEntry = {
      copy(processed = that.processed + processed)
    }
  }

  case class SizePerformanceLogEntry(poolSize: PoolSize, processSpeed: Duration, time: LocalDateTime)

  case class UtilizationRecord(underutilizationStreakStart: Option[LocalDateTime] = None, highestUtilization: Int = 0)

  type RecentProcessingLog = Vector[RecentProcessingLogEntry]
  type SizePerformanceLog = Vector[SizePerformanceLogEntry]

  def apply(resizerCfg: Config): MetricsBasedResizer =
    MetricsBasedResizer(
      lowerBound = resizerCfg.getInt("lower-bound"),
      upperBound = resizerCfg.getInt("upper-bound"),
      chanceOfScalingDownWhenFull = resizerCfg.getDouble("chance-of-ramping-down-when-full"),
      actionInterval = resizerCfg.getDuration("action-interval"),
      retentionPeriod = resizerCfg.getDuration("retention-period"),
      downsizeAfterUnderutilizedFor = resizerCfg.getDuration("downsize-after-underutilized-for"),
      exploreStepSize = resizerCfg.getDouble("explore-step-size"),
      explorationRatio = resizerCfg.getDouble("chance-of-exploration"),
      bufferRatio = resizerCfg.getDouble("buffer-ratio-when-downsizing"))

}

/**
 * Implementation of [[Resizer]] that adjust the [[Pool]] based on performance per size.
 * It keeps the performance log so it's stateful as well as a larger memory footprint than the defaultresizer
 * For documentation about the the parameters, see the reference.conf akka.actor.deployment.default.metrics-based-resizer
 */
@SerialVersionUID(1L)
case class MetricsBasedResizer(
  lowerBound: PoolSize = 1,
  upperBound: PoolSize = 30,
  chanceOfScalingDownWhenFull: Double = 0.2,
  actionInterval: Duration = Duration.ofSeconds(15),
  retentionPeriod: Duration = Duration.ofHours(24),
  numOfAdjacentSizesToConsiderDuringOptimization: Int = 6,
  exploreStepSize: Double = 0.1,
  bufferRatio: Double = 0.2,
  downsizeAfterUnderutilizedFor: Duration = Duration.ofHours(72),
  explorationRatio: Double = 0.4) extends Resizer {

  if (lowerBound < 0) throw new IllegalArgumentException("lowerBound must be >= 0, was: [%s]".format(lowerBound))
  if (upperBound < 0) throw new IllegalArgumentException("upperBound must be >= 0, was: [%s]".format(upperBound))
  if (upperBound < lowerBound) throw new IllegalArgumentException("upperBound must be >= lowerBound, was: [%s] < [%s]".format(upperBound, lowerBound))

  //accessible only for testing purpose
  private[routing] var recentProcessingLog: RecentProcessingLog = Vector.empty
  private[routing] var performanceLog: SizePerformanceLog = Vector.empty
  private[routing] var utilizationRecord: UtilizationRecord = UtilizationRecord()

  val historySampleRate: Duration = actionInterval dividedBy 100

  def isTimeForResize(messageCounter: Long): Boolean = {
    performanceLog.headOption.fold(true)(_.time.isBefore(LocalDateTime.now.minus(actionInterval)))
  }

  def resize(currentRoutees: immutable.IndexedSeq[Routee]): Int = {
    val currentSize = currentRoutees.length

    consolidateLogs(currentSize)

    val proposedChange =
      if (utilizationRecord.underutilizationStreakStart.fold(false)(_.isBefore(LocalDateTime.now.minus(downsizeAfterUnderutilizedFor))))
        downsize(currentSize)
      else if (performanceLog.isEmpty) {
        0
      } else {
        if (Random.nextDouble() < explorationRatio)
          explore(currentSize)
        else
          optimize(currentSize)
      }

    if (proposedChange + currentSize > upperBound) {
      upperBound - currentSize
    } else if (proposedChange + currentSize < lowerBound) {
      lowerBound - currentSize
    } else
      proposedChange
  }

  override def onMessageForwardedToRoutee(queueLength: Int, routees: immutable.IndexedSeq[Routee]): Unit = {
    val occupiedRoutees = routees count {
      case ActorRefRoutee(a: ActorRefWithCell) ⇒
        a.underlying match {
          case cell: ActorCell ⇒
            cell.currentMessage != null || cell.mailbox.hasMessages
          case cell ⇒
            cell.hasMessages
        }
      case x ⇒ false
    }

    val processed = recentProcessingLog.headOption.fold(0)(_.queueLength - queueLength)

    val newEntry = RecentProcessingLogEntry(routees.length, queueLength + 1, processed, occupiedRoutees, LocalDateTime.now)

    //Replace the last entry when it's too close to the previous entry to achieve sampling while retaining the latest status
    val sampling = historySampleRate.toNanos > 0 && recentProcessingLog.length > 1 &&
      recentProcessingLog.tail.head.time.plus(historySampleRate).isAfter(newEntry.time) &&
      newEntry.numOfRoutees == recentProcessingLog.head.numOfRoutees

    recentProcessingLog = if (sampling) {
      newEntry.aggregate(recentProcessingLog.head) +: recentProcessingLog.tail
    } else {
      (newEntry +: recentProcessingLog) match {
        case init :+ last if last.time.isBefore(LocalDateTime.now.minus(actionInterval)) ⇒ init
        case l ⇒ l
      }
    }

    val fullyUtilized = occupiedRoutees == routees.length

    utilizationRecord =
      if (fullyUtilized)
        utilizationRecord.copy(underutilizationStreakStart = None)
      else
        utilizationRecord.copy(
          underutilizationStreakStart = utilizationRecord.underutilizationStreakStart orElse Some(LocalDateTime.now),
          highestUtilization = Math.max(utilizationRecord.highestUtilization, occupiedRoutees))

  }

  private def consolidateLogs(currentSize: PoolSize): Unit = {
    val relevantProcessingLogs = recentProcessingLog.takeWhile { le ⇒
      le.fullyUtilized && le.numOfRoutees == currentSize
    }

    val totalProcessed = relevantProcessingLogs.map(_.processed).sum
    if (totalProcessed > 0 && relevantProcessingLogs.length >= 2) {
      val duration = Duration.between(relevantProcessingLogs.last.time, relevantProcessingLogs.head.time)
      val speed = duration dividedBy totalProcessed
      val newEntry = SizePerformanceLogEntry(currentSize, speed, LocalDateTime.now)

      performanceLog = (newEntry +: performanceLog) match {
        case init :+ last if last.time.isBefore(oldestRetention) ⇒ init
        case l ⇒ l
      }
    }
  }

  private def oldestRetention = LocalDateTime.now.minus(retentionPeriod)

  private def downsize(currentSize: Int): Int = {
    val downsizeTo = (utilizationRecord.highestUtilization * (1 + bufferRatio)).toInt
    Math.min(downsizeTo - currentSize, 0)
  }

  private def optimize(currentSize: PoolSize): Int = {

    val avgDispatchWaitForEachSize: Map[PoolSize, Duration] = performanceLog.groupBy(_.poolSize).mapValues { logs ⇒
      if (logs.length > 1) {
        val init = logs.init
        val avgOfInit = init.foldLeft[Duration](Duration.ofNanos(0))(_ plus _.processSpeed).dividedBy(init.size)
        (avgOfInit plus logs.last.processSpeed) dividedBy 2 //half weight on the latest speed, todo: this math could be improved.
      } else logs.head.processSpeed
    }

    val adjacentDispatchWaits: Map[PoolSize, Duration] = {
      def adjacency = (size: Int) ⇒ Math.abs(currentSize - size)
      val sizes = avgDispatchWaitForEachSize.keys.toSeq
      val numOfSizesEachSide = numOfAdjacentSizesToConsiderDuringOptimization / 2
      val leftBoundary = sizes.filter(_ < currentSize).sortBy(adjacency).take(numOfSizesEachSide).lastOption.getOrElse(currentSize)
      val rightBoundary = sizes.filter(_ >= currentSize).sortBy(adjacency).take(numOfSizesEachSide).lastOption.getOrElse(currentSize)
      avgDispatchWaitForEachSize.filter { case (size, _) ⇒ size >= leftBoundary && size <= rightBoundary }
    }

    val optimalSize = adjacentDispatchWaits.minBy(_._2)._1
    val movement = (optimalSize - currentSize) / 2.0
    if (movement < 0)
      Math.floor(movement).toInt
    else
      Math.ceil(movement).toInt

  }

  private def explore(currentSize: PoolSize): Int = {
    val change = Math.max(1, Random.nextInt(Math.ceil(currentSize * exploreStepSize).toInt))
    if (Random.nextDouble() < chanceOfScalingDownWhenFull)
      -change
    else
      change
  }

}
