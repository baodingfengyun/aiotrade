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
package org.aiotrade.lib.securities

import java.io.IOException
import java.io.ObjectInputStream
import java.io.ObjectOutputStream
import java.io.Reader
import java.io.Writer
import java.text.SimpleDateFormat
import java.util.Calendar
import org.aiotrade.lib.math.timeseries.TVal
import org.aiotrade.lib.json.JsonBuilder
import org.aiotrade.lib.json.JsonSerializable
import scala.collection.mutable.Map

/**
 *
 * This is just a lightweight value object. So, it can be used to lightly store
 * tickers at various time. That is, you can store many many tickers for same
 * symbol efficiently, as in case of composing an one minute ser.
 *
 * The TickerSnapshot will present the last current snapshot ticker for one
 * symbol, and implement Observable. You only need one TickerSnapshot for each
 * symbol.
 *
 * @author Caoyuan Deng
 */
object LightTicker {
  private val PREV_CLOSE = 0
  private val LAST_PRICE = 1
  private val DAY_OPEN   = 2
  private val DAY_HIGH   = 3
  private val DAY_LOW    = 4
  private val DAY_VOLUME = 5
  private val DAY_AMOUNT = 6
  private val DAY_CHANGE = 7

  private val FIELD_LENGTH = 8
}

import LightTicker._
@cloneable @serializable @SerialVersionUID(1L)
class LightTicker(val depth: Int) extends TVal with JsonSerializable {

  final var symbol: String = _

  private val values = new Array[Float](FIELD_LENGTH)

  def this() = this(5)

  final def prevClose = values(PREV_CLOSE)
  final def lastPrice = values(LAST_PRICE)
  final def dayOpen   = values(DAY_OPEN)
  final def dayHigh   = values(DAY_HIGH)
  final def dayLow    = values(DAY_LOW)
  final def dayVolume = values(DAY_VOLUME)
  final def dayAmount = values(DAY_AMOUNT)
  final def dayChange = values(DAY_CHANGE)

  final def prevClose_=(v: Float) = updateFieldValue(PREV_CLOSE, v)
  final def lastPrice_=(v: Float) = updateFieldValue(LAST_PRICE, v)
  final def dayOpen_=  (v: Float) = updateFieldValue(DAY_OPEN,   v)
  final def dayHigh_=  (v: Float) = updateFieldValue(DAY_HIGH,   v)
  final def dayLow_=   (v: Float) = updateFieldValue(DAY_LOW,    v)
  final def dayVolume_=(v: Float) = updateFieldValue(DAY_VOLUME, v)
  final def dayAmount_=(v: Float) = updateFieldValue(DAY_AMOUNT, v)
  final def dayChange_=(v: Float) = updateFieldValue(DAY_CHANGE, v)

  protected def updateFieldValue(fieldIdx: Int, v: Float): Boolean = {
    val isChanged = values(fieldIdx) != v
    values(fieldIdx) = v
    isChanged
  }

  def reset {
    time = 0

    var i = 0
    while (i < values.length) {
      values(i) = 0
      i += 1
    }
  }

  def copyFrom(another: LightTicker) {
    this.time   = another.time
    this.symbol = another.symbol
    System.arraycopy(another.values, 0, values, 0, values.length)
  }

  def isValueChanged(another: LightTicker): Boolean = {
    var i = 0
    while (i < values.length) {
      if (values(i) != another.values(i)) {
        return true
      }
      i += 1
    }

    false
  }

  override def clone: LightTicker = {
    try {
      return super.clone.asInstanceOf[LightTicker]
    } catch {case ex: CloneNotSupportedException => ex.printStackTrace}

    null
  }

  @throws(classOf[IOException])
  def writeJson(out: Writer) {
    out.write("{")

    out.write("\"s\":\"")
    out.write(symbol)
    out.write("\",")

    out.write("\"t\":")
    out.write((time / 1000).toString)
    out.write(",")

    out.write("\"v\":[")
    val lastIdx = values.length - 1
    var i = 0
    while (i < values.length) {
      out.write((values(i) * 100).toInt.toString)
      if (i < lastIdx) {
        out.write(",")
      }
      i += 1
    }
    out.write("]")

    out.write("}")
    out.close
  }

  @throws(classOf[IOException])
  def readJson(in: Reader) {
    val fields = JsonBuilder.readJson(in).asInstanceOf[Map[String, _]]
    symbol     = fields("s").asInstanceOf[String]
    time       = fields("t").asInstanceOf[Long] * 1000
    var values = fields("v").asInstanceOf[List[Number]]
    var i = 0
    while (!values.isEmpty) {
      this.values(i) = values.head.floatValue / 100
      values = values.tail
      i += 1
    }
    
    in.close
  }


  @throws(classOf[IOException])
  private def writeObject_todo(out: ObjectOutputStream) {
    out.defaultWriteObject

    out.writeObject(symbol)
    for (value <- values) {
      out.writeFloat(value)
    }

    out.close
  }

  @throws(classOf[IOException]) @throws(classOf[ClassNotFoundException])
  private def readObject_todo(in: ObjectInputStream) {
    in.defaultReadObject

    symbol = in.readObject.asInstanceOf[String]
    var i = 0
    while (i < values.length) {
      values(i) = in.readFloat
      i += 1
    }

    in.close
  }

  override def toString = {
    val df = new SimpleDateFormat("hh:mm:ss")
    val cal = Calendar.getInstance
    cal.setTimeInMillis(time)
    symbol + ", " + df.format(cal.getTime) + ", " + values.mkString("[", ",", "]")
  }
}
