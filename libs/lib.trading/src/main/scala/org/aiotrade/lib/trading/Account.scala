package org.aiotrade.lib.trading

import java.util.logging.Logger
import org.aiotrade.lib.collection.ArrayList
import org.aiotrade.lib.securities.model.Sec
import org.aiotrade.lib.util.actors.Publisher
import java.util.Currency
import java.util.Locale
import java.util.UUID
import scala.collection.mutable

abstract class Account(val description: String, protected var _balance: Double, 
                       val currency: Currency = Currency.getInstance(Locale.getDefault)
) extends Publisher {
  
  val id = UUID.randomUUID.getMostSignificantBits
  
  protected val _transactions = new ArrayList[TradeTransaction]()
  
  val initialEquity = _balance
  def balance = _balance
  def credit(funds: Double) {_balance += funds}
  def debit (funds: Double) {_balance -= funds}

  def transactions = _transactions.toArray

  def equity: Double
  def availableFunds: Double
}

abstract class TradableAccount($description: String, $balance: Double, 
                               $currency: Currency = Currency.getInstance(Locale.getDefault)
) extends Account($description, $balance, $currency) {
  private val log = Logger.getLogger(this.getClass.getName)
  
  protected val _secToPosition = new mutable.HashMap[Sec, Position]()

  def tradingRule: TradingRule

  def positions = _secToPosition
  def positionGainLoss: Double
  def positionEquity: Double
  def calcFundsToOpen(price: Double, quantity: Double): Double

  /**
   * @return  amount with signum
   */
  protected def calcSecTransactionAmount(order: Order, execution: Execution): Double
  /**
   * For lots of brokers, will directly return transaction, in that case, we can just call processTransaction
   */
  private def processExecution(order: Order, execution: Execution): TradeTransaction = {
    val tradeTransaction = execution match {
      case PaperExecution(order, time, price, quantity) =>
        val fillingAmount = calcSecTransactionAmount(order, execution: Execution)
        // @Note fillingAmount/fillingQuantity should consider signum for SecurityTransaction, which causes money in/out, position increase/decrease.
        val secTransaction = SecurityTransaction(time, order.sec, price, order.side.signum * quantity, fillingAmount, order.side)
      
        val expenses = if (order.side.isOpening)  
          tradingRule.expenseScheme.getOpeningExpenses(price, quantity)
        else
          tradingRule.expenseScheme.getClosingExpenses(price, quantity)
        val expensesTransaction = ExpensesTransaction(time, -expenses)
    
        TradeTransaction(time, Array(secTransaction), expensesTransaction, order)
        
      case FullExecution(order, time, price, quantity, amount, expenses) =>
        val fillingAmount = -order.side.signum * amount
        // @Note fillingAmount/fillingQuantity should consider signum for SecurityTransaction, which causes money in/out, position add/remove.
        val secTransaction = SecurityTransaction(time, order.sec, price, order.side.signum * quantity, fillingAmount, order.side)
        
        TradeTransaction(time, Array(secTransaction), ExpensesTransaction(time, -expenses), order)
    }
    _transactions += tradeTransaction

    tradeTransaction
  }
  
  def processTransaction(order: Order, execution: Execution) {
    order.fill(execution.price, execution.quantity)
    
    val transaction = processExecution(order, execution)
    
    log.info(transaction.toString)
    
    _balance += transaction.amount

    for (SecurityTransaction(time, sec, price, quantity, amount, side) <- transaction.securityTransactions) {
      _secToPosition.get(sec) match {
        case None => 
          val position = Position(this, time, sec, price, quantity)
          _secToPosition(sec) = position
          publish(PositionOpened(this, position))
        
        case Some(position) =>
          position.add(time, price, quantity)
          if (position.quantity == 0) {
            _secToPosition -= sec
            publish(PositionClosed(this, position))
          } else {
            publish(PositionChanged(this, position))
          }
      }
    }
  }
}

class StockAccount($description: String, $balance: Double, val tradingRule: TradingRule, 
                   $currency: Currency = Currency.getInstance(Locale.getDefault)
) extends TradableAccount($description, $balance, $currency) {

  def positionGainLoss = _secToPosition.foldRight(0.0){(x, s) => s + x._2.gainLoss}
  def positionEquity   = _secToPosition.foldRight(0.0){(x, s) => s + x._2.equity}

  def equity = _balance + positionEquity
  def availableFunds = _balance
  
  def calcFundsToOpen(price: Double, quantity: Double) = {
    quantity * price + tradingRule.expenseScheme.getOpeningExpenses(price, quantity)
  }
  
  protected def calcSecTransactionAmount(order: Order, execution: Execution): Double = {
    -order.side.signum * (execution.price * tradingRule.multiplier * tradingRule.marginRate) * execution.quantity
  }

  override 
  def toString = "%1$s, availableFunds=%2$.0f, equity=%3$.0f, positions=%4$s".format(
    description, availableFunds, equity, positions.values.size
  )
}

class FutureAccount($description: String, $balance: Double, val tradingRule: TradingRule, 
                    $currency: Currency = Currency.getInstance(Locale.getDefault)
) extends TradableAccount($description, $balance, $currency) {
  
  def riskLevel = positionMargin / equity * 100
  def positionMargin = positionEquity * tradingRule.marginRate

  def positionGainLoss = _secToPosition.foldRight(0.0){(x, s) => s + x._2.gainLoss * tradingRule.multiplier} // calculate tracking gain loass
  def positionEquity   = _secToPosition.foldRight(0.0){(x, s) => s + x._2.equity   * tradingRule.multiplier}
  
  def equity = _balance + positionGainLoss
  def availableFunds = equity - positionMargin
  
  def calcFundsToOpen(price: Double, quantity: Double) = {
    quantity * price * tradingRule.multiplier * tradingRule.marginRate + 
    tradingRule.expenseScheme.getOpeningExpenses(price * tradingRule.multiplier, quantity)
  }
  
  protected def calcSecTransactionAmount(order: Order, execution: Execution): Double = {
    if (order.side.isOpening) {
      // we won't minus the margin from balance, since the margin was actually not taken from balance, instead, availableFunds will minus mragin.
      0.0 
    } else { // is to close some positions
      _secToPosition.get(order.sec) match {
        case Some(position) =>
          // calculate offset gain loss of closed position right now
          order.side.signum * (execution.price - position.price) * execution.quantity * tradingRule.multiplier
        case _ => 0.0 // This should not happen!
      }
    }
  }

  override 
  def toString = "%1$s, availableFunds=%2$.0f, equity=%3$.0f, positionEquity=%4$.0f, positionMargin=%5$.0f, risk=%6$.2f%%, positions=%7$s".format(
    description, availableFunds, equity, positionEquity, positionMargin, riskLevel, positions.values.map(_.quantity).mkString("(", ",", ")")
  )
}

class CashAccount($description: String, $balance: Double,
                  $currency: Currency = Currency.getInstance(Locale.getDefault)
) extends Account($description, $balance, $currency) {

  def equity = _balance
  def availableFunds = _balance

  override 
  def toString = "%1$s, availableFunds=%2$.0f".format(
    description, availableFunds
  )

}