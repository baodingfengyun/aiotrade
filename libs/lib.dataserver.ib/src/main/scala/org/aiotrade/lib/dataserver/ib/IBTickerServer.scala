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
package org.aiotrade.lib.dataserver.ib

import com.ib.client.Contract
import java.util.TimeZone
import org.aiotrade.lib.securities.dataserver.TickerContract
import org.aiotrade.lib.securities.dataserver.TickerServer
import org.aiotrade.lib.securities.model.Exchange
import org.aiotrade.lib.securities.model.Sec
import org.aiotrade.lib.securities.model.Ticker


/**
 *
 * @author Caoyuan Deng
 */
object IBTickerServer extends TickerServer {

  def getSingleton = this

  private lazy val ibWrapper = IBWrapper
    
  protected def connect: Boolean = {
    if (!ibWrapper.isConnected) {
      ibWrapper.connect
    }
        
    ibWrapper.isConnected
  }
    
  /**
   * Template:
   * http://quote.yahoo.com/download/javasoft.beans?symbols=^HSI+YHOO+SUMW&&format=sl1d1t1c1ohgvbap
   */
  @throws(classOf[Exception])
  protected def request {
    for (contract <- subscribedContracts if !ibWrapper.isMktDataRequested(contract.reqId)) {
      /** request seems lost, re-request */
      var m_rc = false
      var m_marketDepthRows = 0
      val m_contract = new Contract
            
      m_rc = false
      try {
                
        // set contract fields
        m_contract.m_symbol = contract.srcSymbol
        val sec = Exchange.secOf(contract.srcSymbol)
        val kind = Sec.Kind.Stock // @TODO
        m_contract.m_secType = IBWrapper.getSecKind(kind).get
        m_contract.m_expiry = ""
        m_contract.m_strike = 0
        m_contract.m_right = ""
        m_contract.m_multiplier = ""
        m_contract.m_exchange = "SMART"
        m_contract.m_primaryExch = "SUPERSOES"
        m_contract.m_currency = "USD"
        m_contract.m_localSymbol = ""
                
        // set market depth rows
        m_marketDepthRows = 20
      } catch {case ex: Exception => ex.printStackTrace; return}
      m_rc = true
            
      val tickerSnapshot = tickerSnapshotOf(contract.srcSymbol)
      val reqId = ibWrapper.reqMktData(this, m_contract, tickerSnapshot)
      contract.reqId = reqId
    }
  }

  @throws(classOf[Exception])
  protected def read {
    val tickers = ibWrapper.tickers
    var res: Array[Ticker] = null
    tickers synchronized {
      res = tickers.toArray
      tickers.clear
    }
    loadedTime = postRefresh(res)
  }

  override protected def cancelRequest(contract: TickerContract) {
    val tickerSnapshot = tickerSnapshotOf(contract.srcSymbol)
    tickerSnapshot.removeObservers
    ibWrapper.cancelMktDataRequest(contract.reqId)
  }
    
  /**
   * Retrive data from Yahoo finance website
   * Template:
   * http://quote.yahoo.com/download/javasoft.beans?symbols=^HSI+YHOO+SUMW&&format=sl1d1t1c1ohgvbap
   *
   * @param afterThisTime from time
   */
  protected def loadFromSource(afterThisTime: Long, contracts: Iterable[TickerContract]): Array[Ticker] = {
    fromTime = afterThisTime + 1
        
    if (!connect) {
      return EmptyValues
    }
    
    try {
      request
      read
    } catch {case ex: Exception => println("Error in loading from source: " + ex.getMessage)
    }
        
    EmptyValues
  }
    
  override def createNewInstance: Option[TickerServer] = Some(this)
    
  def displayName = {
    "IB TWS"
  }
    
  def defaultDateFormatPattern = {
    "yyyyMMdd HH:mm:ss"
  }
    
  def sourceSerialNumber = 6

  def sourceTimeZone: TimeZone = {
    TimeZone.getTimeZone("America/New_York")
  }

}




