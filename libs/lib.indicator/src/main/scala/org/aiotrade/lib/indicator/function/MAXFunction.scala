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

import org.aiotrade.lib.math.indicator.StatisticFunction
import org.aiotrade.lib.math.timeseries.Null
import org.aiotrade.lib.math.timeseries.BaseTSer
import org.aiotrade.lib.math.timeseries.TVar
import org.aiotrade.lib.math.indicator.Factor

/**
 *
 * @author Caoyuan Deng
 */
class MAXFunction extends AbstractFunction {
  final protected def imax(idx: Int, baseVar: TVar[Float], period: Float, prev: Float): Float = {
    StatisticFunction.imax(idx, baseVar.values, period.toInt, prev)
  }
    
  var period: Factor = _
  var baseVar: TVar[Float] = _
    
  val _max = TVar[Float]()
    
  override def set(baseSer: BaseTSer, args: Any*): Unit = {
    super.set(baseSer)
        
    this.baseVar = args(0).asInstanceOf[TVar[Float]]
    this.period  = args(1).asInstanceOf[Factor]
  }
    
  protected def computeSpot(i: Int): Unit = {
    if (i < period.value - 1) {
            
      _max(i) = Null.Float
            
    } else {
            
      _max(i) = imax(i, baseVar, period.value, _max(i - 1))
            
    }
  }
    
  def max(sessionId: Long, idx: Int): Float = {
    computeTo(sessionId, idx)
        
    _max(idx)
  }
    
}






