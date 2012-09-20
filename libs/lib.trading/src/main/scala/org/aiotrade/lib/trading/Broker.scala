package org.aiotrade.lib.trading


import java.util.Date
import org.aiotrade.lib.math.timeseries.TStamps
import org.aiotrade.lib.securities.QuoteSer
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
  
  def toOrder(orderCompose: OrderCompose): Option[Order]
  
  trait OrderCompose {
    def sec: Sec
    def side: OrderSide
    protected def referIdxAtDecision: Int
    
    /**
     * timestamps of refer
     */
    def timestamps: TStamps
    
    /**
     * Quote ser of this sec
     */
    def ser: QuoteSer

    private var _account: TradableAccount = _
    private var _price = Double.NaN
    private var _funds = Double.NaN
    private var _quantity = Double.NaN
    private var _afterIdx = 0

    def account: TradableAccount = _account
    def using(account: TradableAccount): this.type = {
      _account = account
      this
    }
    
    def price = _price
    def price(price: Double): this.type = {
      _price = price
      this
    }

    def funds = _funds
    def funds(funds: Double): this.type = {
      _funds = funds
      this
    }
    
    def quantity = _quantity
    def quantity(quantity: Double): this.type = {
      _quantity = quantity
      this
    }
        
    /** on t + idx */
    def after(i: Int): this.type = {
      _afterIdx += i
      this
    }
    
    def referIndex = referIdxAtDecision + _afterIdx

    override 
    def toString = {
      "OrderCompose(%1$s, %2$tY.%2$tm.%2$td, %3$s, %4$s, %5$10.2f, %6$d, %7$5.2f)".format(_account.description, new Date(timestamps(referIndex)), sec.uniSymbol, side, _funds, _quantity.toInt, _price)
    }

    def positionOf(sec: Sec): Option[Position] = {
      account.positions.get(sec)
    }
  }
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

