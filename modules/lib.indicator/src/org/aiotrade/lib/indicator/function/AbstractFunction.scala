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
package org.aiotrade.lib.indicator.function

import java.lang.ref.WeakReference
import java.util.HashSet
import java.util.Map
import java.util.Set
import java.util.WeakHashMap
import java.util.concurrent.ConcurrentHashMap
import org.aiotrade.lib.math.timeseries.{DefaultSer,Ser,Var}
import org.aiotrade.lib.math.timeseries.computable.{Factor}
import org.aiotrade.lib.securities.{QuoteSer}

/**
 *
 * @author Caoyuan Deng
 */
object AbstractFunction {

  private val idToFunctions = new ConcurrentHashMap[FunctionID[_], Function]

  def getInstance[T <: Function](tpe: Class[T], baseSer: Ser, args: Any*): T = {
    val id = FunctionID(tpe, baseSer, args:_*)
    idToFunctions.get(id) match {
      case null =>
        /** if none got from functionSet, try to create new one */
        try {
          val function = tpe.newInstance
          /** don't forget to call set(baseSer, args) immediatley */
          function.set(baseSer, args:_*)
          idToFunctions.putIfAbsent(id, function)
          function
        } catch {
          case ex: IllegalAccessException => ex.printStackTrace; null.asInstanceOf[T]
          case ex: InstantiationException => ex.printStackTrace; null.asInstanceOf[T]
        }
      case x => x.asInstanceOf[T]
    }
  }
}

abstract class AbstractFunction extends DefaultSer with FunctionSer {
  import AbstractFunction._

  /**
   * Use computing session to avoid redundant computation on same idx of same
   * function instance by different callers.
   *
   * A session is a series of continuant computing usally called by Indicator
   * It may contains a couple of functions that being called during it.
   *
   * The sessionId is injected in by the caller.
   */
  private var sessionId = -Long.MaxValue
  protected var computedIdx = -Integer.MAX_VALUE

  /** base series to compute this. */
  protected var _baseSer: Ser = _
  /** base series' item size */
  protected var _itemSize: Int = _
    
  /** To store values of open, high, low, close, volume: */
  protected var O: Var[Float] = _
  protected var H: Var[Float] = _
  protected var L: Var[Float] = _
  protected var C: Var[Float] = _
  protected var V: Var[Float] = _

  var id: FunctionID[_] = _
        
  def set(baseSer: Ser, args: Any*): Unit = {
    init(baseSer)
    id = FunctionID(this.getClass.asInstanceOf[Class[Function]], _baseSer, args)
  }
    
  protected def init(baseSer: Ser): Unit = {
    super.init(baseSer.freq)
    this._baseSer = baseSer

    this.attach(baseSer.timestamps)

    initPredefinedVarsOfBaseSer
  }
    
  /** override this method to define your own pre-defined vars if necessary */
  protected def initPredefinedVarsOfBaseSer: Unit = {
    _baseSer match {
      case x: QuoteSer =>
        O = x.open
        H = x.high
        L = x.low
        C = x.close
        V = x.volume
      case _ =>
    }
  }
        
  /**
   * This method will compute from computedIdx <b>to</b> idx.
   *
   * and AbstractIndicator.compute(final long begTime) will compute <b>from</b>
   * begTime to last item
   *
   * @param sessionId, the sessionId usally is controlled by outside caller,
   *        such as an indicator
   * @param idx, the idx to be computed to
   */
  def computeTo(sessionId: Long, idx: Int): Unit = {
    try {
      timestamps.readLock.lock
            
      preComputeTo(sessionId, idx)
        
      /**
       * if in same session and idx has just been computed, do not do
       * redundance computation
       */
      if (this.sessionId == sessionId && idx <= computedIdx) {
        return
      }
        
      this.sessionId = sessionId
        
      /** computedIdx itself has been computed, so, compare computedIdx + 1 with idx */
      var begIdx = Math.min(computedIdx + 1, idx)
      if (begIdx < 0) {
        begIdx = 0
      }
        
      /**
       * get baseSer's itemList size via protected _itemSize here instead of by
       * indicator's subclass when begin computeCont, because we could not
       * sure if the baseSer's _itemSize size has been change by others
       * (DataServer etc.)
       */
      val size = timestamps.size
        
      val endIdx = Math.min(idx, size - 1)
      /** fill with clear items from begIdx, then call computeSpot(i): */
      var i = begIdx
      while (i <= endIdx) {
        val time = timestamps(i)
        createItemOrClearIt(time)
            
        computeSpot(i)
        i += 1
      }
        
      computedIdx = idx
        
      postComputeTo(sessionId, idx)
    } finally {
      timestamps.readLock.unlock
    }
  }
    
  /**
   * override this method to do something before computeTo, such as set computedIdx etc.
   */
  protected def preComputeTo(sessionId: Long, idx: Int): Unit = {
  }
    
  /**
   * override this method to do something post computeTo
   */
  protected def postComputeTo(sessionId: Long, idx: Int): Unit = {
  }
    
  /**
   * @param i, idx of spot
   */
  protected def computeSpot(i: Int): Unit

  override def equals(o:Any) = o match {
    case x:Function => this.id.equals(x.id)
    case _ => false
  }

  override def hashCode = id.hashCode
    
  /**
   * Define functions
   * --------------------------------------------------------------------
   */
    
  /**
   * Functions of helper
   * ----------------------------------------------------------------------
   */
    
  protected def indexOfLastValidValue(var1: Var[_]) :Int = {
    val values = var1.values
    var i = values.size - 1; while (i > 0) {
      val value = values(i)
      if (value != null && !value.equals(Float.NaN)) {
        return _baseSer.indexOfOccurredTime(timestamps(i))
      }

      i -= 1
    }
    -1
  }
    
  /**
   * ---------------------------------------------------------------------
   * End of functions of helper
   */
    
    
    
    
  /**
   * Functions from FunctionSereis
   * ----------------------------------------------------------------------
   */
    
  protected def sum(idx: Int, baseVar: Var[_], period: Factor): Float = {
    getInstance(classOf[SUMFunction], _baseSer, baseVar, period).sum(sessionId, idx)
  }
    
  protected def max(idx: Int, baseVar: Var[_], period: Factor): Float = {
    getInstance(classOf[MAXFunction], _baseSer, baseVar, period).max(sessionId, idx)
  }
    
  protected def min(idx: Int, baseVar: Var[_], period: Factor): Float = {
    getInstance(classOf[MINFunction], _baseSer, baseVar, period).min(sessionId, idx)
  }
    
  protected def ma(idx: Int, baseVar: Var[_], period: Factor): Float = {
    getInstance(classOf[MAFunction], _baseSer, baseVar, period).ma(sessionId, idx)
  }
    
  protected def ema(idx: Int, baseVar: Var[_], period: Factor): Float = {
    getInstance(classOf[EMAFunction], _baseSer, baseVar, period).ema(sessionId, idx)
  }
    
  protected def stdDev(idx: Int, baseVar: Var[_], period: Factor): Float = {
    getInstance(classOf[STDDEVFunction], _baseSer, baseVar, period).stdDev(sessionId, idx)
  }
    
  protected def probMass(idx: Int, baseVar: Var[Float], period: Factor, nInterval: Factor): Array[Array[Float]] = {
    getInstance(classOf[PROBMASSFunction], _baseSer, baseVar, null, period, nInterval).probMass(sessionId, idx)
  }
    
  protected def probMass(idx: Int, baseVar: Var[Float], weight: Var[Float] , period: Factor, nInterval: Factor): Array[Array[Float]] = {
    getInstance(classOf[PROBMASSFunction], _baseSer, baseVar, weight, period, nInterval).probMass(sessionId, idx)
  }
    
  protected def tr(idx: Int): Float = {
    getInstance(classOf[TRFunction], _baseSer).tr(sessionId, idx)
  }
    
  protected def dmPlus(idx: Int): Float = {
    getInstance(classOf[DMFunction], _baseSer).dmPlus(sessionId, idx)
  }
    
  protected def dmMinus(idx: Int): Float = {
    getInstance(classOf[DMFunction], _baseSer).dmMinus(sessionId, idx)
  }
    
  protected def diPlus(idx: Int, period: Factor): Float = {
    getInstance(classOf[DIFunction], _baseSer, period).diPlus(sessionId, idx)
  }
    
  protected def diMinus(idx: Int, period: Factor): Float = {
    getInstance(classOf[DIFunction], _baseSer, period).diMinus(sessionId, idx)
  }
    
  protected def dx(idx: Int, period: Factor): Float = {
    getInstance(classOf[DXFunction], _baseSer, period).dx(sessionId, idx)
  }
    
  protected def adx(idx: Int, periodDi: Factor, periodAdx: Factor): Float = {
    getInstance(classOf[ADXFunction], _baseSer, periodDi, periodAdx).adx(sessionId, idx)
  }
    
  protected def adxr(idx: Int, periodDi: Factor, periodAdx: Factor): Float = {
    getInstance(classOf[ADXRFunction], _baseSer, periodDi, periodAdx).adxr(sessionId, idx)
  }
    
  protected def bollMiddle(idx: Int, baseVar: Var[_], period: Factor, alpha: Factor): Float = {
    getInstance(classOf[BOLLFunction], _baseSer, baseVar, period, alpha).bollMiddle(sessionId, idx)
  }
    
  protected def bollUpper(idx: Int, baseVar: Var[_], period: Factor, alpha: Factor): Float = {
    getInstance(classOf[BOLLFunction], _baseSer, baseVar, period, alpha).bollUpper(sessionId, idx)
  }
    
  protected def bollLower(idx: Int, baseVar: Var[_], period: Factor, alpha: Factor): Float = {
    getInstance(classOf[BOLLFunction], _baseSer, baseVar, period, alpha).bollLower(sessionId, idx)
  }
    
  protected def cci(idx: Int, period: Factor, alpha: Factor): Float = {
    getInstance(classOf[CCIFunction], _baseSer, period, alpha).cci(sessionId, idx)
  }
    
  protected def macd(idx: Int, baseVar: Var[_], periodSlow: Factor, periodFast: Factor): Float = {
    getInstance(classOf[MACDFunction], _baseSer, baseVar, periodSlow, periodFast).macd(sessionId, idx)
  }
    
  protected def mfi(idx: Int, period: Factor): Float = {
    getInstance(classOf[MFIFunction], _baseSer, period).mfi(sessionId, idx)
  }
    
  protected def mtm(idx: Int, baseVar: Var[_], period: Factor): Float = {
    getInstance(classOf[MTMFunction], _baseSer, baseVar, period).mtm(sessionId, idx)
  }
    
  protected def obv(idx: Int): Float = {
    getInstance(classOf[OBVFunction], _baseSer).obv(sessionId, idx)
  }
    
  protected def roc(idx: Int, baseVar: Var[_], period: Factor): Float = {
    getInstance(classOf[ROCFunction], _baseSer, baseVar, period).roc(sessionId, idx)
  }
    
  protected def rsi(idx: Int, period: Factor): Float = {
    getInstance(classOf[RSIFunction], _baseSer, period).rsi(sessionId, idx)
  }
    
  protected def sar(idx: Int, initial: Factor, step: Factor, maximum: Factor): Float = {
    getInstance(classOf[SARFunction], _baseSer, initial, step, maximum).sar(sessionId, idx)
  }
    
  protected def sarDirection(idx: Int, initial: Factor, step: Factor, maximum: Factor) :Direction = {
    getInstance(classOf[SARFunction], _baseSer, initial, step, maximum).sarDirection(sessionId, idx)
  }
    
  protected def stochK(idx: Int, period: Factor, periodK: Factor): Float = {
    getInstance(classOf[STOCHKFunction], _baseSer, period, periodK).stochK(sessionId, idx)
  }
    
  protected def stochD(idx: Int, period: Factor, periodK: Factor, periodD: Factor): Float = {
    getInstance(classOf[STOCHDFunction], _baseSer, period, periodK, periodD).stochD(sessionId, idx)
  }
    
  protected def stochJ(idx: Int, period: Factor, periodK: Factor, periodD: Factor): Float = {
    getInstance(classOf[STOCHJFunction], _baseSer, period, periodK, periodD).stochJ(sessionId, idx)
  }
    
  protected def wms(idx: Int, period: Factor): Float = {
    getInstance(classOf[WMSFunction], _baseSer, period).wms(sessionId, idx)
  }
    
  protected def zigzag(idx: Int, percent: Factor): Float = {
    getInstance(classOf[ZIGZAGFunction], _baseSer, percent).zigzag(sessionId, idx)
  }
    
  protected def pseudoZigzag(idx: Int, percent: Factor): Float = {
    getInstance(classOf[ZIGZAGFunction], _baseSer, percent).pseudoZigzag(sessionId, idx)
  }
    
  protected def zigzagDirection(idx: Int, percent: Factor) :Direction = {
    getInstance(classOf[ZIGZAGFunction], _baseSer, percent).zigzagDirection(sessionId, idx)
  }
    
    
  /**
   * ----------------------------------------------------------------------
   * End of Functions from FunctionSereis
   */
}
