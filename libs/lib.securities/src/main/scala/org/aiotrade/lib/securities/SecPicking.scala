package org.aiotrade.lib.securities

import java.text.SimpleDateFormat
import java.util.Date
import java.util.logging.Logger
import org.aiotrade.lib.collection.ArrayList
import org.aiotrade.lib.math.signal.Side
import org.aiotrade.lib.math.timeseries.TFreq
import org.aiotrade.lib.securities.model.Sec
import org.aiotrade.lib.securities.model.SectorSec
import org.aiotrade.lib.util.ValidTime
import org.aiotrade.lib.util.actors.Publisher
import scala.collection.mutable

final case class SecPickingEvent(secValidTime: ValidTime[Sec], side: Side)

/**
 * 
 * @author Caoyuan Deng
 */
class SecPicking extends Publisher {
  private val log = Logger.getLogger(getClass.getName)
  
  private var prevTime = 0L
  private val validTimes = new ArrayList[ValidTime[Sec]]
  val secToValidTimes = new mutable.HashMap[Sec, List[ValidTime[Sec]]]()
  val setToWeightValidTimes = new mutable.HashMap[Sec, List[ValidTime[Double]]]
  
  private def addToMap(secValidTime: ValidTime[Sec]) {
    secToValidTimes(secValidTime.ref) = secValidTime :: secToValidTimes.getOrElse(secValidTime.ref, Nil)
  }

  private def removeFromMap(secValidTime: ValidTime[Sec]) {
    secToValidTimes.getOrElse(secValidTime.ref, Nil) filter (_ != secValidTime) match {
      case Nil => secToValidTimes -= secValidTime.ref
      case xs => secToValidTimes(secValidTime.ref) = xs
    }
  }
  
  private def addToMap(sec: Sec, weightValidTime: ValidTime[Double]) {
    setToWeightValidTimes(sec) = weightValidTime :: setToWeightValidTimes.getOrElse(sec, Nil)
  }

  private def removeFromMap(sec: Sec, weightValidTime: ValidTime[Double]) {
    setToWeightValidTimes.getOrElse(sec, Nil) filter (_ != weightValidTime) match {
      case Nil => setToWeightValidTimes -= sec
      case xs => setToWeightValidTimes(sec) = xs
    }
  }

  def allSecs = secToValidTimes.keySet
  
  def at(times: Long*): Array[Sec] = {
    val secs = new ArrayList[Sec]()
    val itr = new IteratorAtTime(times: _*)
    while (itr.hasNext) {
      secs += itr.next
    }
    secs.toArray
  }

  def go(time: Long) {
    var i = 0
    while (i < validTimes.length) {
      val validTime = validTimes(i)
      if (validTime.isValid(time) && !validTime.isValid(prevTime)) {
        publish(SecPickingEvent(validTime, Side.EnterPicking))
      } else if (!validTime.isValid(time) && validTime.isValid(prevTime)) {
        publish(SecPickingEvent(validTime, Side.ExitPicking))
      }
      i += 1
    }
    prevTime = time
  }
  
  
  // --- secs
  
  def +(secValidTime: ValidTime[Sec]) {
    validTimes += secValidTime
    addToMap(secValidTime)
  }
  
  def +(sec: Sec, fromTime: Long, toTime: Long) {
    this.+(ValidTime(sec, fromTime, toTime))
  }
  
  def -(secValidTime: ValidTime[Sec]) {
    validTimes -= secValidTime
    removeFromMap(secValidTime)
  }
  
  def -(sec: Sec, fromTime: Long, toTime: Long) {
    this.-(ValidTime(sec, fromTime, toTime))
  }
  
  def +=(secValidTime: ValidTime[Sec]): SecPicking = {
    this.+(secValidTime)
    this
  }

  def +=(sec: Sec, fromTime: Long, toTime: Long): SecPicking = {
    this.+(sec, fromTime, toTime)
    this
  }

  def -=(secValidTime: ValidTime[Sec]): SecPicking = {
    this.-(secValidTime)
    this
  }

  def -=(sec: Sec, fromTime: Long, toTime: Long): SecPicking = {
    this.-(sec, fromTime, toTime)
    this
  }

  def ++(secValidTimes: Seq[ValidTime[Sec]]) {
    this.validTimes ++= secValidTimes
    secValidTimes foreach addToMap
  }
  
  def ++=(secValidTimes: Seq[ValidTime[Sec]]): SecPicking = {
    this.++(secValidTimes)
    this
  }
  
  def --(secValidTimes: Seq[ValidTime[Sec]]) {
    this.validTimes --= secValidTimes
    secValidTimes foreach removeFromMap
  }
  
  def --=(secValidTimes: Seq[ValidTime[Sec]]): SecPicking = {
    this.--(secValidTimes)
    this
  }
  
  // --- weights
  
  def +(sec: Sec, weightValidTime: ValidTime[Double]) {
    addToMap(sec, weightValidTime)
  }
  
  def +(sec: Sec, weight: Double, fromTime: Long, toTime: Long) {
    this.+(sec, ValidTime(weight, fromTime, toTime))
  }
  
  def -(sec: Sec, weightValidTime: ValidTime[Double]) {
    removeFromMap(sec, weightValidTime)
  }
  
  def -(sec: Sec, weight: Double, fromTime: Long, toTime: Long) {
    this.-(sec, ValidTime(weight, fromTime, toTime))
  }
  
  def +=(sec: Sec, weightValidTime: ValidTime[Double]): SecPicking = {
    this.+(sec, weightValidTime)
    this
  }

  def +=(sec: Sec, weight: Double, fromTime: Long, toTime: Long): SecPicking = {
    this.+(sec, weight, fromTime, toTime)
    this
  }

  def -=(sec: Sec, weightValidTime: ValidTime[Double]): SecPicking = {
    this.-(sec, weightValidTime)
    this
  }

  def -=(sec: Sec, weight: Double, fromTime: Long, toTime: Long): SecPicking = {
    this.-(sec, weight, fromTime, toTime)
    this
  }
  
  /**
   * @return weight of sec at time, NaN if none
   */
  def weightAt(sec: Sec)(time: Long): Double = {
    setToWeightValidTimes.getOrElse(sec, Nil) find (_.isValid(time)) map (_.ref) getOrElse(Double.NaN)
  }
  
  /**
   * CSI300 = SUM(Price * AdjustedShares) / BaseDayIndex * 1000
   *        = SUM(AdjustedMarketCapitalization) / SUM(BaseDayAdjustedMarketCapitalization) * 1000
   * 
   * weight is a convinence efficient calculated at end time of report period:
   *   weight = Price * AdjustedShares / SUM(Price * AdjustedShares)
   * or
   *   weight = AdjustedMarketCapitalization / SUM(AdjustedMarketCapitalization)
   *   
   * 报告期指数 = ∑(市价 × 调整股本数) / 基日成份股调整市值 × 1000
   * 权重 = 市价 × 调整股本数 / ∑(市价 × 调整股本数)
   * 报告期指数 = ∑(市价×调整股本数) / 基日成份股调整市值 × 1000
   * 权重 = 市价 × 调整股本数 / (报告期指数 x 基日成份股调整市值 / 1000)
   * 调整股本数 = 权重 x (报告期指数 x 基日成份股调整市值 / 1000) / 市价
   * 报告期指数 = ∑(市价 × (权重 x (报告期指数 x 基日成份股调整市值 / 1000) / 市价)) / 基日成份股调整市值 × 1000
   *          = ∑(权重 x 报告期指数 x 基日成份股调整市值 / 1000) / 基日成份股调整市值 × 1000
   *          = ∑(权重 x 报告期指数)
   * 1        = ∑(权重)          
   */
  def weightedValueAt(freq: TFreq)(prevValue: Double, prevTime: Long, time: Long): Double = {
    val secs = at(time)
    var value = 0.0
    var assignedWeight = 0.0
    var i = 0
    while (i < secs.length) {
      val sec = secs(i)
      for (ser <- sec.serOf(freq)) {
        var weight = weightAt(sec)(prevTime)
        if (!weight.isNaN) {
          weight = if (assignedWeight + weight > 1) {
            1 - assignedWeight
          } else weight
          assignedWeight += weight
          val funds = prevValue * weight
          val prevIdx = ser.timestamps.indexOfOccurredTime(prevTime)
          val idx = ser.timestamps.indexOfOccurredTime(time)
          if (prevIdx >= 0 && idx >= 0) {
            val weightPrice = ser.close(prevIdx) // price when weight is calculated
            val price = ser.close(idx)
            value += funds * price / weightPrice // positionSize = funds / weightPrice
          } else {
            value += funds // funds substitute
          }
        } else {
          log.warning("NaN weight of " + sec.uniSymbol + ", time=" + new Date(time))
        }
      }
      i += 1
    }
    
    log.info("Sum of weights: " + assignedWeight)
    value + (1 - assignedWeight) * prevValue
  }
  
  def isInvalid(sec: Sec, time: Long) = !isValid(sec, time)
  def isValid(sec: Sec, time: Long): Boolean = {
    secToValidTimes.get(sec) match {
      case Some(xs) => xs.exists(_.isValid(time))
      case None => false
    }
  }

  def iterator(time: Long): Iterator[Sec] = new IteratorAtTime(time)
  
  /**
   * Do (block) for each valid sec
   */
  def foreach(time: Long)(block: Sec => Unit) {
    val itr = iterator(time)
    while (itr.hasNext) {
      val sec = itr.next
      block(sec)
    }
  }

  def foldLeft[T](time: Long)(block: (Sec, T) => T)(result: T) = {
    var acc = result
    val itr = iterator(time)
    while (itr.hasNext) {
      val sec = itr.next
      acc = block(sec, acc)
    }
    acc
  }
  
  override 
  def toString = {
    val df = new SimpleDateFormat("yyyy-MM-dd")
    val sb = new StringBuilder()
    for {
      (sec, validTimes) <- secToValidTimes
      validTime <- validTimes
    } {
      val validFrom = if (validTime.validFrom == 0) "__________" else df.format(new Date(validTime.validFrom))
      val validTo = if (validTime.validTo == 0) "----------" else df.format(new Date(validTime.validTo))
      sb.append(validTime.ref.uniSymbol).append(": ").append(validFrom).append(" -> ").append(validTo).append("\n")
    }
    sb.toString
  }
  
  final class IteratorAtTime(times: Long*) extends Iterator[Sec] {
    private var index = 0
      
    def hasNext = {
      while (index < validTimes.length && times.foldLeft(false){(s, x) => s || validTimes(index).isInvalid(x)}) {
        index += 1
      }
      index < validTimes.length
    }
      
    def next = {
      if (hasNext) {
        val sec = validTimes(index).ref
        index += 1
        sec
      } else {
        null
      }
    }
  }

}

object SecPicking {
  val a = new SecPicking()
  val b = a += (new Sec, 1, 1)
  
  
  /**
   * @param The sector<->sec relation table records. They should belongs to the same sector
   */
  def toSecPicking(sectorSecs: Seq[SectorSec]): SecPicking = {
    val stockPicking = new SecPicking()
    for (sectorSec <- sectorSecs if sectorSec.sec ne null) {
      stockPicking += sectorSec.toSecValidTime
    }
    stockPicking
  }
}
