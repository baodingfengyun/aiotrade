package org.aiotrade.lib.trading

import java.util.Calendar
import java.util.Date
import org.aiotrade.lib.collection.ArrayList
import org.aiotrade.lib.math.timeseries.TFreq

/**
 * 
 * @author Caoyuan Deng
 */
class Benchmark(freq: TFreq) {
  case class Profit(time: Long, nav: Double, accRate: Double, periodRate: Double, riskFreeRate: Double) {
    val periodRateForSharpe = periodRate - riskFreeRate
    
    override 
    def toString = {
      "%1$tY.%1$tm.%1$td \t %2$ 8.2f \t %3$ 8.2f%% \t %4$ 8.2f%% \t %5$ 8.2f%% \t %6$ 8.2f%%".format(
        new Date(time), nav, accRate * 100, periodRate * 100, riskFreeRate * 100, periodRateForSharpe * 100
      )
    }
  }
  
  var tradeCount: Int = _
  var tradeFromTime: Long = Long.MinValue
  var tradeToTime: Long = Long.MinValue
  var tradePeriod: Int = _
  
  var times = new ArrayList[Long]
  var equities = new ArrayList[Double]()
  
  var initialEquity = Double.NaN
  var profitRatio = 0.0
  var annualizedProfitRatio = 0.0
  private var lastEquity = 0.0
  private var maxEquity = Double.MinValue
  private var maxDrawdownEquity = Double.MaxValue
  var maxDrawdownRatio = Double.MinValue
  
  var profits: Array[Profit] = Array()
  var rrr: Double = _
  var sharpeRatio: Double = _
  
  var monthlyRiskFreeRate = 0.0 // 0.003
  
  var reportDayOfMonth = 26
  
  def at(time: Long, equity: Double) {
    times += time
    equities += equity
    
    if (tradeFromTime == Long.MinValue) tradeFromTime = time
    tradeToTime = time
    
    if (initialEquity.isNaN) initialEquity = equity

    lastEquity = equity
    profitRatio = equity / initialEquity - 1
    calcMaxDropdown(equity)
  }
  
  def report: String = {
    tradePeriod = daysBetween(tradeFromTime, tradeToTime)
    annualizedProfitRatio = math.pow(1 + profitRatio, 365.24 / tradePeriod) - 1
    
    rrr = annualizedProfitRatio / maxDrawdownRatio
    profits = calcMonthlyReturn(times.toArray, toNavs(initialEquity, equities))
    sharpeRatio = 12 * calcSharpeRatio(profits)
    
    toString
  }
  
  private def calcMaxDropdown(equity: Double) {
    if (equity > maxEquity) {
      maxEquity = math.max(equity, maxEquity)
      maxDrawdownEquity = equity
    } else if (equity < maxEquity) {
      maxDrawdownEquity = math.min(equity, maxDrawdownEquity)
      maxDrawdownRatio = math.max((maxEquity - maxDrawdownEquity) / maxEquity, maxDrawdownRatio)
    }
  }
  
  private def calcSharpeRatio(xs: Array[Profit]) = {
    if (xs.length > 0) {
      var sum = 0.0
      var i = 0
      while (i < xs.length) {
        val x = xs(i)
        sum += x.periodRateForSharpe
        i += 1
      }
      val average = sum / xs.length
      var devSum = 0.0
      i = 0
      while (i < xs.length) {
        val x = xs(i).periodRateForSharpe - average
        devSum += x * x
        i += 1
      }
      val stdDev = math.sqrt(devSum / xs.length)
      average / stdDev
    } else {
      0.0
    }
  }
  
  private def toNavs(initalEquity: Double, equities: ArrayList[Double]) = {
    val navs = Array.ofDim[Double](equities.length)
    var i = 0
    while (i < equities.length) {
      navs(i) = equities(i) / initialEquity
      i += 1
    }
    navs
  }
  
  /**
   * navs Net Asset Value
   */
  private def calcMonthlyReturn(times: Array[Long], navs: Array[Double]) = {
    val reportTimes = new ArrayList[Long]()
    val reportNavs = new ArrayList[Double]()
    val now = Calendar.getInstance
    var prevNav = 1.0
    var prevTime = 0L
    var reportTime = Long.MaxValue
    var i = 0
    while (i < times.length) {
      val time = times(i)
      val nav = navs(i)
      now.setTimeInMillis(time)
      if (reportTime == Long.MaxValue) {
        reportTime = getReportTime(now, reportDayOfMonth)
      }
      if (time > reportTime && prevTime <= reportTime) {
        reportTimes += reportTime
        reportNavs += prevNav
        reportTime = getReportTime(now, reportDayOfMonth)
      } else {
        if (i == times.length - 1) {
          reportTimes += getReportTime(now, reportDayOfMonth)
          reportNavs += nav
        }
      }
      
      prevTime = time
      prevNav = nav
      i += 1
    }
    
    if (reportTimes.length > 0) {
      val profits = new ArrayList[Profit]
 
      var i = 0
      while (i < reportTimes.length) {
        val time = reportTimes(i)
        val nav = reportNavs(i)
        val accRate = nav - 1
        val periodRate = if (i > 0) nav / profits(i - 1).nav - 1 else 0.0
        profits += Profit(time, nav, accRate, periodRate, monthlyRiskFreeRate)
        i += 1
      }
      
      profits.toArray
    } else {
      Array[Profit]()
    }
  }
  
  /**
   * @param the current date
   * @param the day of month of settlement, last day of each month if -1 
   */
  private def getReportTime(now: Calendar, _reportDayOfMonth: Int = -1) = {
    val reportDayOfMonth = if (_reportDayOfMonth == -1) {
      now.getActualMaximum(Calendar.DAY_OF_MONTH)
    } else _reportDayOfMonth
    
    if (now.get(Calendar.DAY_OF_MONTH) > reportDayOfMonth) {
      now.add(Calendar.MONTH, 1) // will settlement in next month
    }
    
    now.set(Calendar.DAY_OF_MONTH, reportDayOfMonth)
    now.getTimeInMillis
  }

  override 
  def toString = {
    """================ Benchmark Report ================
Trade period      : %1$tY.%1$tm.%1$td --- %2$tY.%2$tm.%2$td in %3$s days
Initial equity    : %4$.0f
Final equity      : %5$.0f  
Total Return      : %6$.2f%%
Annualized Return : %7$.2f%% 
Max Drawdown      : %8$.2f%%
RRR               : %9$.2f
Sharpe ratio      : %10$.2f
================ Monthly Return ================
Date                  nav       acc-return   period-return       rf-return    sharpe-return
%11$s
    """.format(
      tradeFromTime, tradeToTime, tradePeriod,
      initialEquity,
      lastEquity,
      profitRatio * 100,
      annualizedProfitRatio * 100,
      maxDrawdownRatio * 100,
      rrr,
      sharpeRatio,
      profits.mkString("\n")
    )
  }
  
  /**
   * @Note time2 > time1
   */
  private def daysBetween(time1: Long, time2: Long): Int = {
    val cal1 = Calendar.getInstance
    val cal2 = Calendar.getInstance
    cal1.setTimeInMillis(time1)
    cal2.setTimeInMillis(time2)
    
    val years = cal2.get(Calendar.YEAR) - cal1.get(Calendar.YEAR)
    var days = cal2.get(Calendar.DAY_OF_YEAR) - cal1.get(Calendar.DAY_OF_YEAR)
    var i = 0
    while (i < years) {
      cal1.set(Calendar.YEAR, cal1.get(Calendar.YEAR) + 1)
      days += cal1.getActualMaximum(Calendar.DAY_OF_YEAR)
      i += 1
    }
    days
  }
}

