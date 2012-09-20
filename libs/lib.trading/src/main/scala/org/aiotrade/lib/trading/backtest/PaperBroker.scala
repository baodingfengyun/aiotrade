package org.aiotrade.lib.trading.backtest

import java.text.SimpleDateFormat
import java.util.Date
import java.util.UUID
import java.util.logging.Logger
import org.aiotrade.lib.securities.model.Exchange
import org.aiotrade.lib.securities.model.Sec
import org.aiotrade.lib.trading.Account
import org.aiotrade.lib.trading.Broker
import org.aiotrade.lib.trading.BrokerException
import org.aiotrade.lib.trading.Order
import org.aiotrade.lib.trading.OrderDelta
import org.aiotrade.lib.trading.OrderDeltasEvent
import org.aiotrade.lib.trading.OrderRoute
import org.aiotrade.lib.trading.OrderSide
import org.aiotrade.lib.trading.OrderStatus
import org.aiotrade.lib.trading.OrderType
import org.aiotrade.lib.trading.OrderValidity
import org.aiotrade.lib.trading.PaperExecution
import scala.collection.mutable

class PaperBroker(val name: String) extends Broker {
  private val log = Logger.getLogger(getClass.getName)
  private val orderIdFormatter = new SimpleDateFormat("yyMMddHHmmssSSS")
  
  val id: Long = UUID.randomUUID.getMostSignificantBits
  
  private val pendingSecToExecutingOrders = new mutable.HashMap[Sec, mutable.HashSet[Order]]()

  val allowedSides = List(
    OrderSide.Buy, 
    OrderSide.Sell
  )

  val allowedTypes = List(
    OrderType.Limit, 
    OrderType.Market
  )
  
  val allowedValidity = List(
    OrderValidity.Day
  )

  val allowedRoutes = List[OrderRoute]()

  @throws(classOf[BrokerException])
  def connect {
    // for paper broker, listenTo ticker server
  }

  @throws(classOf[BrokerException])
  def disconnect {
    // for paper broker, deafTo ticker server
  }
  
  @throws(classOf[BrokerException])
  override 
  def cancel(order: Order) {
    pendingSecToExecutingOrders synchronized {
      val executingOrders = pendingSecToExecutingOrders.getOrElse(order.sec, new mutable.HashSet[Order]())
      executingOrders -= order
      if (executingOrders.isEmpty) {
        pendingSecToExecutingOrders -= order.sec
      } else {
        pendingSecToExecutingOrders(order.sec) = executingOrders
      }
    }

    order.status = OrderStatus.Canceled
      
    log.info("Order Cancelled: %s".format(order))
        
    publish(OrderDeltasEvent(this, Array(OrderDelta.Updated(order))))
  }

  @throws(classOf[BrokerException])
  override
  def submit(order: Order) {
    pendingSecToExecutingOrders synchronized {
      val executingOrders = pendingSecToExecutingOrders.getOrElse(order.sec, new mutable.HashSet[Order]())
      executingOrders += order
      pendingSecToExecutingOrders(order.sec) = executingOrders
    }

    order.id = orderIdFormatter.format(new Date()).toLong
    order.status = OrderStatus.PendingNew

    log.info("Order Submitted: %s".format(order))

    publish(OrderDeltasEvent(this, Array(OrderDelta.Updated(order))))
    
    // for paper work, we assume all orders can be executed sucessfully.
    // for real trading, this should be trigged by returning order executed event 
    processTicker(order.sec, order.time, order.price, order.quantity)
  }  

  def isAllowOrderModify = false
  @throws(classOf[BrokerException])
  def modify(order: Order) {
    throw BrokerException("Modify not allowed", null)
  }
  

  def canTrade(sec: Sec) = {
    true
  }
  
  def getSecurityBySymbol(symbol: String): Sec = {
    Exchange.secOf(symbol).get
  }

  def getSymbolBySecurity(sec: Sec) = {
    sec.uniSymbol
  }

  def executingOrders = pendingSecToExecutingOrders
  
  def toOrder(oc: OrderCompose): Option[Order] = {
    if (oc.account.availableFunds > 0) {
      val time = oc.timestamps(oc.referIndex)
      oc.ser.valueOf(time) match {
        case Some(quote) =>
          oc.side match {
            case OrderSide.Buy | OrderSide.SellShort =>
              if (oc.price.isNaN) {
                oc.price(oc.account.tradingRule.buyPriceRule(quote))
              }
              if (oc.quantity.isNaN) {
                oc.quantity(oc.account.tradingRule.buyQuantityRule(quote, oc.price, oc.funds))
              }
            case OrderSide.Sell | OrderSide.BuyCover =>
              if (oc.price.isNaN) {
                oc.price(oc.account.tradingRule.sellPriceRule(quote))
              }
              if (oc.quantity.isNaN) {
                oc.quantity(oc.positionOf(oc.sec) match {
                    case Some(position) => 
                      // @Note quantity of position may be negative because of sellShort etc.
                      oc.account.tradingRule.sellQuantityRule(quote, oc.price, math.abs(position.quantity))
                    case None => 0
                  })
              }
            case _ =>
          }
          
          oc.quantity(math.abs(oc.quantity))
          if (oc.quantity > 0) {
            val order = new Order(oc.account, oc.sec, oc.quantity, oc.price, oc.side)
            order.time = time
            println("Some order: %s".format(this))
            Some(order)
          } else {
            println("None order: %s. Quote: volume=%5.2f, average=%5.2f, cost=%5.2f".format(
                this, quote.volume, quote.average, quote.average * oc.account.tradingRule.multiplier * oc.account.tradingRule.marginRate)
            )
            None
          }
          
        case None => None
      }
    } else None
  }
  
  /**
   * For paper work, use this method to receive ticker data to drive orders being executed gradually
   */
  def processTicker(sec: Sec, time: Long, price: Double, quantity: Double) {
    var deltas = List[OrderDelta]()

    val secOrders = pendingSecToExecutingOrders synchronized {pendingSecToExecutingOrders.getOrElse(sec, new mutable.HashSet[Order]())}
    
    var toRemove = List[Order]()
    for (order <- secOrders) {
      order.status match {
        case OrderStatus.PendingNew | OrderStatus.Partial =>
          order.tpe match {
            case OrderType.Market =>
              deltas ::= OrderDelta.Updated(order)
              fill(order, time, price, quantity)
                
            case OrderType.Limit =>
              order.side match {
                case (OrderSide.Buy | OrderSide.SellShort) if price <= order.price =>
                  deltas ::= OrderDelta.Updated(order)
                  fill(order, time, price, quantity)
                    
                case (OrderSide.Sell | OrderSide.BuyCover) if price >= order.price => 
                  deltas ::= new OrderDelta.Updated(order)
                  fill(order, time, price, quantity)

                case _ =>
              }
                
            case _ =>
          }
          
        case _ =>
      }
        
      if (order.status == OrderStatus.Filled) {
        toRemove ::= order
      }
    }
    
    if (toRemove.nonEmpty) {
      pendingSecToExecutingOrders synchronized {
        secOrders --= toRemove
        if (secOrders.isEmpty) {
          pendingSecToExecutingOrders -= sec
        } else {
          pendingSecToExecutingOrders(sec) = secOrders
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
  def fill(order: Order, time: Long, price: Double, quantity: Double) {
    val fillingQuantity = math.min(quantity, order.remainQuantity)
    
    if (fillingQuantity > 0) {
      val execution = PaperExecution(order, time, price, fillingQuantity)
      order.account.processTransaction(order, execution)
    } else {
      log.warning("Filling Quantity <= 0: feedPrice=%s, feedSize=%s, remainQuantity=%s".format(price, quantity, order.remainQuantity))
    }
  }

  def accounts: Array[Account] = {
    Array[Account]() // @todo get from db
  }
}
