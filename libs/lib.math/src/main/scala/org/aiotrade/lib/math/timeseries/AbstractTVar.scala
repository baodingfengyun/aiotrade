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
package org.aiotrade.lib.math.timeseries

import org.aiotrade.lib.math.indicator.Plot

/**
 * This is a horizotal view of DefaultSer. Is' a reference of one of
 * the field vars.
 *
 * @author Caoyuan Deng
 */
abstract class AbstractTVar[V](var name: String, var plot: Plot)(protected implicit val m: Manifest[V]) extends TVar[V] {
  val LAYER_NOT_SET = -1

  var layer = LAYER_NOT_SET

  def toArray(fromTime: Long, toTime: Long): Array[V] = {
    try {
      timestamps.readLock.lock

      val frIdx = timestamps.indexOfNearestOccurredTimeBehind(fromTime)
      val toIdx = timestamps.indexOfNearestOccurredTimeBefore(toTime)
      val len = toIdx - frIdx + 1
      val values1 = new Array[V](len)

      values.copyToArray(values1, frIdx, len)
      values1

    } finally {
      timestamps.readLock.unlock
    }
  }

  def toArrayWithTime(fromTime: Long, toTime: Long): (Array[Long], Array[V]) = {
    try {
      timestamps.readLock.lock

      val frIdx = timestamps.indexOfNearestOccurredTimeBehind(fromTime)
      val toIdx = timestamps.indexOfNearestOccurredTimeBefore(toTime)
      val len = toIdx - frIdx + 1
      val times1 = new Array[Long](len)
      val values1 = new Array[V](len)

      timestamps.copyToArray(times1, frIdx, len)
      values.copyToArray(values1, frIdx, len)
      (times1, values1)
      
    } finally {
      timestamps.readLock.unlock
    }
  }

  def toDoubleArray: Array[Double] = {
    val length = size
    val result = new Array[Double](length)
        
    if (length > 0 && this(0).isInstanceOf[Number]) {
      var i = 0
      while (i < length) {
        result(i) = this(i).asInstanceOf[Number].doubleValue
        i += 1
      }
    }
        
    result
  }

  def float(time: Long): Float = toFloat(apply(time))
  def float(idx: Int):   Float = toFloat(apply(idx))

  def double(time: Long): Double = toDouble(apply(time))
  def double(idx: Int):   Double = toDouble(apply(idx))

  def toFloat(v: V): Float = {
    v match {
      case null => Null.Float
      case x: Byte   => x.toFloat
      case x: Short  => x.toFloat
      case x: Char   => x.toFloat
      case x: Int    => x.toFloat
      case x: Long   => x.toFloat
      case x: Float  => x
      case x: Double => x.toFloat
      case x: Number => x.floatValue
      case x: AnyRef =>
        assert(false, "Why you get here(TVar.float) ? " +
               v + " Check your code and give me Float instead of float: " +
               x.asInstanceOf[AnyRef].getClass.getCanonicalName)
        Null.Float
    }
  }

  def toDouble(v: V): Double = {
    v match {
      case null => Null.Double
      case x: Byte   => x.toDouble
      case x: Short  => x.toDouble
      case x: Char   => x.toDouble
      case x: Int    => x.toDouble
      case x: Long   => x.toDouble
      case x: Float  => x.toDouble
      case x: Double => x
      case n: Number => n.doubleValue
      case o: AnyRef =>
        assert(false, "Why you get here(TVar.double) ? " +
               v + " Check your code and give me Double instead of double: " +
               o.asInstanceOf[AnyRef].getClass.getCanonicalName)
        Null.Double
    }
  }

  /**
   * Clear values that >= fromIdx
   */
  def clear(fromIdx: Int): Unit = {
    if (fromIdx < 0) {
      return
    }
    var i = values.size - 1
    while (i >= fromIdx) {
      values.remove(i)
      i += 1
    }
  }

  def size: Int = values.size

  /**
   * All instances of TVar or extended classes will be equals if they have the
   * same values, this prevent the duplicated manage of values.
   */
  override 
  def equals(o: Any): Boolean = {
    o match {
      case x: TVar[_] => this.values eq x.values
      case _ => false
    }
  }

  /**
   * All instances of TVar or extended classes use identityHashCode as hashCode
   */
  private val _hashCode = System.identityHashCode(this)
  override 
  def hashCode: Int = _hashCode

  override 
  def toString = name
}
