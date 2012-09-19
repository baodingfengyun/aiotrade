package org.aiotrade.lib.trading


import org.aiotrade.lib.securities.model.Sec
import org.aiotrade.lib.util.actors.Publisher

/**
 * Brokers that managed accounts and receive trade to fill orders
 *
 */
trait Broker extends Publisher {
  def id: Long
  def name: String

  @throws(classOf[BrokerException])
  def connect: Unit

  @throws(classOf[BrokerException])
  def disconnect: Unit

  @throws(classOf[BrokerException])
  def submit(order: Order)

  @throws(classOf[BrokerException])
  def cancel(order: Order): Unit

  @throws(classOf[BrokerException])
  def modify(order: Order): Unit
  def isAllowOrderModify: Boolean
  
  def allowedTypes: List[OrderType]
  def allowedSides: List[OrderSide]
  def allowedValidity: List[OrderValidity]
  def allowedRoutes: List[OrderRoute]
  def canTrade(sec: Sec): Boolean
  def getSecurityBySymbol(symbol: String): Sec
  def getSymbolBySecurity(sec: Sec)
  def accounts: Array[Account]
  def executingOrders: collection.Map[Sec, collection.Iterable[Order]]

  /**
   * For paper work, this method can receive ticker data to drive orders being executed gradually;
   * For real trading, thie method can receive execution report one by one.
   */
  def processTrade(sec: Sec, time: Long, price: Double, quantity: Double, amount: Double = Double.NaN, expenses: Double = Double.NaN)
  
  /**
   * Update account's funds, positions etc to newest status
   */
  def updateAccount(account: Account) {}
}


final case class BrokerException(message: String, cause: Throwable) extends Exception(message, cause)

sealed trait OrderDelta {
  def order: Order
}
object OrderDelta {
  final case class Added(order: Order) extends OrderDelta
  final case class Removed(order: Order) extends OrderDelta
  final case class Updated(order: Order) extends OrderDelta  
}

final case class OrderDeltasEvent(broker: Broker, deltas: Seq[OrderDelta])
