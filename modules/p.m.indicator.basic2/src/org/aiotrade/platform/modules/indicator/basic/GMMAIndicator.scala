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
package org.aiotrade.platform.modules.indicator.basic;

import org.aiotrade.lib.math.timeseries.Var;
import org.aiotrade.lib.math.timeseries.computable.Factor;
import org.aiotrade.lib.math.timeseries.plottable.Plot;
import org.aiotrade.lib.indicator.ContIndicator;

/**
 * Guppy multiple Moving Average
 *
 * @author Caoyuan Deng
 */
class GMMAIndicator extends ContIndicator {
    _sname = "GMMA"
    _lname = "Guppy Multiple Moving Average"
    
    val period01 = Factor("Period Short 1", 3)
    val period02 = Factor("Period Short 2", 5)
    val period03 = Factor("Period Short 3", 8)
    val period04 = Factor("Period Short 4", 10)
    val period05 = Factor("Period Short 5", 12)
    val period06 = Factor("Period Short 6", 15)
    val period07 = Factor("Period Long 1",  30)
    val period08 = Factor("Period Long 2",  35)
    val period09 = Factor("Period Long 3",  40)
    val period10 = Factor("Period Long 4",  45)
    val period11 = Factor("Period Long 5",  50)
    val period12 = Factor("Period Long 6",  60)
    
    val  ma01 = TimeVar[Float]("MA01", Plot.Line)
    val  ma02 = TimeVar[Float]("MA02", Plot.Line)
    val  ma03 = TimeVar[Float]("MA03", Plot.Line)
    val  ma04 = TimeVar[Float]("MA04", Plot.Line)
    val  ma05 = TimeVar[Float]("MA05", Plot.Line)
    val  ma06 = TimeVar[Float]("MA06", Plot.Line)
    val  ma07 = TimeVar[Float]("MA07", Plot.Line)
    val  ma08 = TimeVar[Float]("MA08", Plot.Line)
    val  ma09 = TimeVar[Float]("MA09", Plot.Line)
    val  ma10 = TimeVar[Float]("MA10", Plot.Line)
    val  ma11 = TimeVar[Float]("MA11", Plot.Line)
    val  ma12 = TimeVar[Float]("MA12", Plot.Line)
    
    protected def computeCont(begIdx:Int) :Unit = {
        var i = begIdx;
        while (i < _itemSize) {
            ma01(i) = ma(i, C, period01)
            ma02(i) = ma(i, C, period02)
            ma03(i) = ma(i, C, period03)
            ma04(i) = ma(i, C, period04)
            ma05(i) = ma(i, C, period05)
            ma06(i) = ma(i, C, period06)
            ma07(i) = ma(i, C, period07)
            ma08(i) = ma(i, C, period08)
            ma09(i) = ma(i, C, period09)
            ma10(i) = ma(i, C, period10)
            ma11(i) = ma(i, C, period11)
            ma12(i) = ma(i, C, period12)
            i += 1
        }
    }
    
}




