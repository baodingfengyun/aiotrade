/*
 * Copyright (c) 2006-2007, AIOTrade Computing Co. and Contributors
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without 
 * modification, are permitted provided that the following conditions are met:
 * 
 *  o Redistributions of source code must retain the above copyright notice, 
 *    this list of conditions and the following disclaimer. 
 *    
 *  o Redistributions in binary form must reproduce the above copyright notice, 
 *    this list of conditions and the following disclaimer in the documentation 
 *    and/or other materials provided with the distribution. 
 *    
 *  o Neither the name of AIOTrade Computing Co. nor the names of 
 *    its contributors may be used to endorse or promote products derived 
 *    from this software without specific prior written permission. 
 *    
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" 
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, 
 * THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR 
 * PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR 
 * CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, 
 * EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, 
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; 
 * OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, 
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR 
 * OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, 
 * EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package org.aiotrade.lib.securities.dataserver

import java.util.logging.Logger
import org.aiotrade.lib.math.timeseries.TFreq
import org.aiotrade.lib.math.timeseries.datasource.DataServer
import org.aiotrade.lib.securities.TickerSnapshot
import org.aiotrade.lib.securities.model.Tickers
import org.aiotrade.lib.securities.model.Exchange
import org.aiotrade.lib.securities.model.Execution
import org.aiotrade.lib.securities.model.ExecutionEvent
import org.aiotrade.lib.securities.model.Executions
import org.aiotrade.lib.securities.model.LightTicker
import org.aiotrade.lib.securities.model.MarketDepth
import org.aiotrade.lib.securities.model.Quote
import org.aiotrade.lib.securities.model.SecSnap
import org.aiotrade.lib.securities.model.Ticker
import org.aiotrade.lib.securities.model.TickersLast
import org.aiotrade.lib.util.reactors.Event
import org.aiotrade.lib.util.actors.Publisher
import org.aiotrade.lib.collection.ArrayList
import ru.circumflex.orm._
import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet

/**
 *
 * @author Caoyuan Deng
 */
case class TickerEvent(ticker: Ticker) extends Event // TickerEvent only accept Ticker
case class TickersEvent(tickers: Array[LightTicker]) extends Event // TickersEvent accept LightTicker

case class DepthSnap (
  prevPrice: Double,
  prevDepth: MarketDepth,
  execution: Execution
)

object TickerServer extends Publisher
abstract class TickerServer extends DataServer[Ticker] {
  type C = TickerContract

  private val log = Logger.getLogger(this.getClass.getName)
  private val config = org.aiotrade.lib.util.config.Config()
  protected val isServer = !config.getBool("dataserver.client", false)
  log.info("Ticker server is started as " + (if (isServer) "server" else "client"))

  def tickerSnapshotOf(uniSymbol: String): TickerSnapshot = {
    val sec = Exchange.secOf(uniSymbol).get
    sec.tickerSnapshot
  }

  override def subscribe(contract: TickerContract) {
    super.subscribe(contract)

    /**
     * !NOTICE
     * the symbol-tickerSnapshot pair must be immutable, other wise, if
     * the symbol is subscribed repeatly by outside, the old observers
     * of tickerSnapshot may not know there is a new tickerSnapshot for
     * this symbol, the older one won't be updated any more.
     */
    val symbol = toUniSymbol(contract.srcSymbol)
    val sec = Exchange.secOf(symbol).get
    val tickerSnapshot = sec.tickerSnapshot
    tickerSnapshot.symbol = symbol
  }

  override def unsubscribe(contract: TickerContract) {
    super.unsubscribe(contract)

    val symbol = contract.srcSymbol
    val sec = Exchange.secOf(symbol).get
    val tickerSnapshot = sec.tickerSnapshot
  }

  override protected def postLoadHistory(values: Array[Ticker], contracts: Iterable[TickerContract]): Long = {
    composeSer(values)
  }

  override protected def postRefresh(values: Array[Ticker]): Long = {
    composeSer(values)
  }

  private val allTickers = new ArrayList[Ticker]
  private val allExecutions = new ArrayList[Execution]
  private val allDepthSnaps = new ArrayList[DepthSnap]
  private val updatedDailyQuotes = new ArrayList[Quote]


  private val exchangeToLastTime = new HashMap[Exchange, Long]

  private def toSecSnaps(values: Array[Ticker]): (Seq[SecSnap], Seq[Ticker]) = {
    val processedSymbols = new HashSet[String] // used to avoid duplicate symbols of each refreshing

    val secSnaps = new ArrayList[SecSnap]
    val tickersLast = new ArrayList[Ticker]
    var i = 0
    while (i < values.length) {
      val ticker = values(i)
      val symbol = ticker.symbol

      if (!processedSymbols.contains(symbol) && ticker.dayHigh != 0 && ticker.dayLow != 0) {
        processedSymbols.add(symbol)
        
        Exchange.secOf(symbol) match {
          case Some(sec) =>
            ticker.sec = sec

            val exchange = sec.exchange
            val tickerx = exchange.gotLastTicker(ticker)
            if (subscribedSrcSymbols.contains(symbol)) {
              tickersLast += tickerx
              secSnaps += sec.secSnap.setByTicker(ticker)
            } else {
              log.fine("subscribedSrcSymbols doesn't contain " + symbol)
            }
          case None => log.warning("No sec for " + symbol)
        }
      } else {
        log.info("Discard ticker: " + ticker.symbol)
      }

      i += 1
    }
    
    (secSnaps, tickersLast)
  }

  /**
   * compose ser using data from Tickers
   * @param Tickers
   */
  def composeSer(values: Array[Ticker]): Long = {
    var lastTime = Long.MinValue

    log.info("Composing ser from tickers: " + values.length)
    if (values.length == 0) return lastTime
    //values.foreach{ticker => if(ticker.symbol == "399001.SZ"){log.fine("Composing " + ticker.symbol + ": " + ticker)}}

    val (secSnaps, tickersLast) = toSecSnaps(values)
    log.info("Composing ser from secSnaps: " + secSnaps.length)
    if (secSnaps.length == 0) return lastTime

    allTickers.clear
    allExecutions.clear
    allDepthSnaps.clear
    updatedDailyQuotes.clear

    exchangeToLastTime.clear

    var i = 0
    while (i < secSnaps.length) {
      val secSnap = secSnaps(i)

      val sec = secSnap.sec
      val ticker = secSnap.newTicker
      val lastTicker = secSnap.lastTicker
      val isDayFirst = ticker.isDayFirst
      val dayQuote = secSnap.dailyQuote
      val minQuote = secSnap.minuteQuote

      log.fine("Composing from ticker: " + ticker + ", lasticker: " + lastTicker)

      var tickerValid = false
      var execution: Execution = null
      if (isDayFirst) {
        log.fine("Got day's first ticker: " + ticker)
        
        /**
         * this is today's first ticker we got when begin update data server,
         * actually it should be, so maybe we should check this.
         * As this is the first data of today:
         * 1. set OHLC = Ticker.LAST_PRICE
         * 2. to avoid too big volume that comparing to following dataSeries.
         * so give it a small 0.0001 (if give it a 0, it will won't be calculated
         * in calcMaxMin() of ChartView)
         */

        tickerValid = true
        
        dayQuote.unjustOpen_!

        minQuote.unjustOpen_!
        minQuote.open   = ticker.dayOpen
        minQuote.high   = ticker.dayHigh
        minQuote.low    = ticker.dayLow
        minQuote.close  = ticker.lastPrice
        minQuote.volume = ticker.dayVolume
        minQuote.amount = ticker.dayAmount

        execution = new Execution
        execution.sec = sec
        execution.time = ticker.time
        execution.price  = ticker.lastPrice
        execution.volume = ticker.dayVolume
        execution.amount = ticker.dayAmount
        allExecutions += execution

      } else {

        /**
         *    ticker.time    prevTicker.time
         *          |------------------|------------------->
         *          |<----- 1000 ----->|
         */
        if (ticker.time + 1000 > lastTicker.time) { // 1000ms, @Note: we may add +1 to ticker.time later
          // some datasources only count on second, but we may truly have a new ticker
          if (ticker.time <= lastTicker.time) {
            ticker.time = lastTicker.time + 1 // avoid duplicate key
          }

          tickerValid = true

          if (ticker.dayVolume > lastTicker.dayVolume) {
            execution = new Execution
            execution.sec = sec
            execution.time = ticker.time
            execution.price = ticker.lastPrice
            execution.volume = ticker.dayVolume - lastTicker.dayVolume
            execution.amount = ticker.dayAmount - lastTicker.dayAmount
            allExecutions += execution
          } else {
            log.fine("dayVolome curr: " + ticker.dayVolume + ", last: " + lastTicker.dayVolume)
          }

          if (minQuote.justOpen_?) {
            minQuote.unjustOpen_!
            
            // init minQuote values:
            minQuote.open = ticker.lastPrice
            minQuote.high = ticker.lastPrice
            minQuote.low  = ticker.lastPrice
            minQuote.volume = 0
            minQuote.amount = 0
          } 

          if (lastTicker.dayHigh > 0 && ticker.dayHigh > 0) {
            if (ticker.dayHigh > lastTicker.dayHigh) {
              /** this is a new day high happened during prevTicker to this ticker */
              minQuote.high = ticker.dayHigh
            }
          }
          if (ticker.lastPrice > 0) {
            minQuote.high = math.max(minQuote.high, ticker.lastPrice)
          }

          if (lastTicker.dayLow > 0 && ticker.dayLow > 0) {
            if (ticker.dayLow < lastTicker.dayLow) {
              /** this is a new day low happened during prevTicker to this ticker */
              minQuote.low = ticker.dayLow
            }
          }
          if (ticker.lastPrice > 0) {
            minQuote.low = math.min(minQuote.low, ticker.lastPrice)
          }
          
          minQuote.close = ticker.lastPrice
          if (execution != null && execution.volume > 0) {
            minQuote.volume += execution.volume
            minQuote.amount += execution.amount
          }

        } else {
          log.warning("Discard ticker " + ticker.toString)
        }
      }


      if (execution != null) {
        val prevPrice = if (isDayFirst) ticker.prevClose else lastTicker.lastPrice
        val prevDepth = if (isDayFirst) MarketDepth.Empty else MarketDepth(lastTicker.bidAsks, copy = true)
        allDepthSnaps += DepthSnap(prevPrice, prevDepth, execution)

        log.fine("Will published execution")
        sec.publish(ExecutionEvent(ticker.prevClose, execution))
        log.fine("Published execution: " + ExecutionEvent(ticker.prevClose, execution))
      }

      if (tickerValid) {
        allTickers += ticker
        lastTicker.copyFrom(ticker)
        sec.publish(TickerEvent(ticker))

        // update daily quote and ser
        updateDailyQuoteByTicker(dayQuote, ticker)

        // update chainSers
        if (!isServer && sec.isSerCreated(TFreq.ONE_SEC)) {
          sec.realtimeSer.updateFrom(minQuote)
        }
        if (isServer || sec.isSerCreated(TFreq.ONE_MIN)) {
          sec.serOf(TFreq.ONE_MIN) foreach {_.updateFrom(minQuote)}
        }
        if (isServer || sec.isSerCreated(TFreq.DAILY)) {
          sec.serOf(TFreq.DAILY) foreach {_.updateFrom(dayQuote)}
        }

        exchangeToLastTime.put(sec.exchange, ticker.time)

        lastTime = math.max(lastTime, ticker.time)
      } else{
        log.warning("Invalid ticker: " + ticker)
      }

      i += 1
    }
    
    /* else {

     /**
      * no new ticker got, but should consider if it's necessary to to update quoteSer
      * as the quote window may be just opened.
      */
     sec.lastData.prevTicker match {
     case null =>
     case ticker =>
     if (ticker != null && ticker.dayHigh != 0 && ticker.dayLow != 0) {
     val dayQuote = sec.dailyQuoteOf(ticker.time)
     updateDailyQuote(dayQuote, ticker)
     // update chainSers
     sec.serOf(TFreq.DAILY) match {
     case Some(x) if x.loaded => x.updateFrom(dayQuote)
     case _ =>
     }
     sec.serOf(TFreq.ONE_MIN) match {
     case Some(x) if x.loaded => x.updateFrom(minQuote)
     case _ =>
     }
     }
     }
     } */

    // batch save to db

    log.info("Going to save to db ...")
    var willCommit = false
    val (tickersLastToInsert, tickersLastToUpdate) = tickersLast.partition(_.isTransient)
    if (tickersLastToInsert.length > 0) {
      TickersLast.insertBatch_!(tickersLastToInsert.toArray)
      willCommit = true
    }
    if (tickersLastToUpdate.length > 0) {
      TickersLast.updateBatch_!(tickersLastToUpdate.toArray)
      willCommit = true
    }

    if (willCommit) {
      log.info("Committing: tickersLastToInsert=" + tickersLastToInsert.length + ", tickersLastToUpdate=" + tickersLastToUpdate.length)
    }

    if (isServer) {
      if (allTickers.length > 0) {
        Tickers.insertBatch_!(allTickers.toArray)
        willCommit = true
      }

      if (allExecutions.length > 0) {
        Executions.insertBatch_!(allExecutions.toArray)
        willCommit = true
      }

      if (willCommit) {
        log.info("Committing: tickers=" + allTickers.length + ", executions=" + allExecutions.length)
//        allTickers.foreach{ticker => if(ticker.symbol == "399001.SZ"){log.fine("Will commit " + ticker)}}
      }
    }

    // @Note if there is no update/insert on db, do not call commit, which may cause deadlock
    if (willCommit) {
      commit
      log.info("Committed")
    }

    if (allDepthSnaps.length > 0) {
      processDepthSnaps(allDepthSnaps.toArray)
    }

    for ((exchange, lastTime) <- exchangeToLastTime) {
      val status = exchange.tradingStatusOf(lastTime)
      log.info("Trading status of " + exchange.code + ": " + status)
      exchange.tradingStatus = status
      exchange.tryClosing(isServer)
    }

    // publish events
    if (allTickers.length > 0) {
      TickerServer.publish(TickersEvent(allTickers.toArray.asInstanceOf[Array[LightTicker]]))
//      allTickers.foreach(t => if(t.symbol == "399001.SZ"){log.info("Published " + t.symbol + ": " + t)})
    }

    lastTime
  }

  protected def processDepthSnaps(depthSnaps: Array[DepthSnap]) = ()

  private def updateDailyQuoteByTicker(dailyQuote: Quote, ticker: Ticker) {
    dailyQuote.open   = ticker.dayOpen
    dailyQuote.high   = ticker.dayHigh
    dailyQuote.low    = ticker.dayLow
    dailyQuote.close  = ticker.lastPrice
    dailyQuote.volume = ticker.dayVolume
    dailyQuote.amount = ticker.dayAmount
  }

  def toSrcSymbol(uniSymbol: String): String = uniSymbol
  def toUniSymbol(srcSymbol: String): String = srcSymbol
}
