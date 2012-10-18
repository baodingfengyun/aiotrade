package org.aiotrade.lib.trading

import java.util.logging.Logger
import org.aiotrade.lib.securities._
import org.aiotrade.lib.securities.dataserver.NullQuoteServer
import org.aiotrade.lib.securities.dataserver.NullTickerServer
import org.aiotrade.lib.securities.model.Sec
import org.aiotrade.lib.math.timeseries.TFreq
import org.aiotrade.lib.math.timeseries.descriptor.Content


/**
 * 
 * @Noet If we implement DataLoader as actor to async load data from db, the db performance 
 * may become the bottleneck, and the worse, the db connections may be exhausted, and causes
 * series problems.
 * 
 * @author Caoyuan Deng
 */
class DataLoader(val sec: Sec, 
                 quoteFreqs: List[TFreq], indFreqs: List[TFreq],
                 quoteServerClass: String = NullQuoteServer.getClass.getName, 
                 tickerServerClass: String = NullTickerServer.getClass.getName
) {
  private val log = Logger.getLogger(this.getClass.getName)

  def act() {
    val symbol = sec.uniSymbol
    
    log.info("Loading " + symbol)

    val content = sec.content

    for (freq <- quoteFreqs) {
      val quoteContract = createQuoteContract(symbol, "", "", freq, false, quoteServerClass)
      content.addDescriptor(quoteContract)
    }
    val tickerContract = createTickerContract(symbol, "", "", TFreq.ONE_MIN, tickerServerClass)
    sec.tickerContract = tickerContract

    for (freq <- indFreqs) {
      createAndAddIndicatorDescritors(content, freq)
    }
  

    // * init indicators before loadSer, so, they can receive the Loaded evt
    val inds = for (freq <- indFreqs; ser <- sec.serOf(freq)) yield initIndicators(content, ser) 

    for (freq <- quoteFreqs; ser <- sec.serOf(freq)) {
      sec.loadSer(ser)
      ser.adjust()
    }

    // * Here, we test two possible conditions:
    // * 1. inds may have been computed by Loaded evt,
    // * 2. data loading may not finish yet
    // * For what ever condiction, we force to compute it again to test concurrent
    inds flatMap (x => x) foreach computeAsync
  }
    
  /**
   * Added indicators that want to load here. @Todo specify indicator via config ?
   */
  def createAndAddIndicatorDescritors(content: Content, freq: TFreq) {}

  act
}
