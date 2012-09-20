package org.aiotrade.lib.trading


import java.util.Date
import java.util.logging.Logger
import org.aiotrade.lib.math.timeseries.TStamps
import org.aiotrade.lib.securities.QuoteSer
import org.aiotrade.lib.securities.model.Sec
import org.aiotrade.lib.util.actors.Publisher
import scala.collection.mutable

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
  
  val executingOrders = new mutable.HashMap[Sec, mutable.HashSet[Order]]()

  /**
   * Update account's funds, positions etc to newest status
   */
  def updateAccount(account: Account) {}
  
  def toOrder(orderCompose: OrderCompose): Option[Order]
  
  final class SetDouble(v: Double) {   
    def isSet() = !v.isNaN
    def notSet() = v.isNaN
  } 
  
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

    implicit def ToSetDouble(v: Double) = new SetDouble(v)
    
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

abstract class TradeBroker extends Broker {
  private val log = Logger.getLogger(getClass.getName)
  
  def toOrder(oc: OrderCompose): Option[Order] = {
    import oc._
    val time = timestamps(referIndex)
    if (account.availableFunds > 0) {
      val order = side match {
        case OrderSide.Buy | OrderSide.SellShort =>
          if (funds.isSet) {
            if (price.notSet) {
              // funds set but without price, just ignore quantity, it's dangerous to get a terrible price
              Some(new Order(account, sec, quantity, price, side, OrderType.Market))
            } else {
              // ignore quantity, since funds is set, it's dangerous to get a terrible price
              Some(new Order(account, sec, quantity, price, side, OrderType.Market))
            }
          } else { // funds is not set
            if (price.isSet && quantity.isSet) {
              Some(new Order(account, sec, quantity, price, side, OrderType.Limit))
            } else {
              None
            }
          }
        case OrderSide.Sell | OrderSide.BuyCover =>
          // @Note quantity of position may be negative because of sellShort etc.
          val theQuantity = if (quantity.isSet) {
            math.abs(quantity)
          } else {
            positionOf(sec) match {
              case Some(position) => math.abs(position.quantity)
              case None => 0
            }  
          }
          if (price.isSet) {
            Some(new Order(account, sec, quantity, price, side, OrderType.Limit))
          } else {
            Some(new Order(account, sec, quantity, price, side, OrderType.Market))
          }
        case _ => None
      }
       
      println("Some order: %s".format(order))
      order map {x => x.time = time; x}
    } else None
  }
  
  /**
   * For real trading, thie method can receive execution report one by one.
   */
  @throws(classOf[BrokerException])
  def processTrade(order: Order, time: Long, price: Double, quantity: Double, amount: Double, expenses: Double) {
    var deltas = List[OrderDelta]()

    val sec = order.sec
    log.info("Process trade: " + order.sec.uniSymbol + ", time=" + new java.util.Date(time) + ", price = " + price + ", quantity=" + quantity)
    val secOrders = executingOrders synchronized {executingOrders.getOrElse(sec, new mutable.HashSet[Order]())}

    var toRemove = List[Order]()
    order.status match {
      case OrderStatus.PendingNew | OrderStatus.Partial =>
        order.tpe match {
          case OrderType.Market =>
            deltas ::= OrderDelta.Updated(order)
            fill(order, time, price, quantity, amount, expenses)

          case OrderType.Limit =>
            order.side match {
              case (OrderSide.Buy | OrderSide.SellShort) if price <= order.price =>
                deltas ::= OrderDelta.Updated(order)
                fill(order, time, price, quantity, amount, expenses)

              case (OrderSide.Sell | OrderSide.BuyCover) if price >= order.price =>
                deltas ::= new OrderDelta.Updated(order)
                fill(order, time, price, quantity, amount, expenses)

              case _ =>
            }

          case _ =>
        }

      case _ =>
    }

    if (order.status == OrderStatus.Filled) {
      toRemove ::= order
    }

    if (toRemove.nonEmpty) {
      executingOrders synchronized {
        secOrders --= toRemove
        if (secOrders.isEmpty) {
          executingOrders -= sec
        } else {
          executingOrders(sec) = secOrders
        }
      }
    }

    if (deltas.nonEmpty) {
      publish(OrderDeltasEvent(this, deltas))
    }
  }
  
  /**
   * Fill order by price and size, this will also process binding account.
   * Usually this method is used only in paper worker
   */
  def fill(order: Order, time: Long, price: Double, quantity: Double, amount: Double, expenses: Double) {
    val fillingQuantity = math.min(quantity, order.remainQuantity)
    
    if (fillingQuantity > 0) {
      val execution = FullExecution(order, time, price, fillingQuantity, amount, expenses)
      order.account.processTransaction(order, execution)
    } else {
      log.warning("Filling Quantity <= 0: feedPrice=%s, feedSize=%s, remainQuantity=%s".format(price, quantity, order.remainQuantity))
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

