package org.aiotrade.lib.amqp

import com.rabbitmq.client.AMQP
import com.rabbitmq.client.Channel
import com.rabbitmq.client.Connection
import com.rabbitmq.client.ConnectionFactory
import com.rabbitmq.client.Consumer
import com.rabbitmq.client.DefaultConsumer
import com.rabbitmq.client.Envelope
import java.io.BufferedInputStream
import java.io.BufferedOutputStream
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.IOException
import java.io.InputStreamReader
import java.io.ObjectInputStream
import java.io.ObjectOutputStream
import java.io.OutputStreamWriter
import java.util.Timer
import java.util.TimerTask
import java.util.logging.Logger
import java.util.logging.Level
import java.util.zip.GZIPInputStream
import java.util.zip.GZIPOutputStream
import org.aiotrade.lib.amqp.datatype.ContentType
import org.aiotrade.lib.json.JsonBuilder
import org.aiotrade.lib.json.JsonSerializable
import scala.actors.Actor

/**
 * Option 1:
 * create one queue per consumer with several bindings, one for each stock.
 * Prices in this case will be sent with a topic routing key.
 *
 * Option 2:
 * Another option is to create one queue per stock. each consumer will be
 * subscribed to several queues. Messages will be sent with a direct routing key.
 *
 * Best Practice:
 * Option 1: should work fine, except there is no need to use a topic exchange.
 * Just use a direct exchange, one queue per user and for each of the stock
 * symbols a user is interested in create a binding between the user's queue
 * and the direct exchange.
 *
 * Option 2: each quote would only go to one consumer, which is probably not
 * what you want. In an AMQP system, to get the same message delivered to N
 * consumers you need (at least) N queues. Exchanges *copy* messages to queues,
 * whereas queues *round-robin* message delivery to consumers.
 */

/**
 * @param content A deserialized value received via AMQP.
 * @param props
 *
 * Messages received from AMQP are wrapped in this case class. When you
 * register a listener, this is the case class that you will be matching on.
 */
case class AMQPMessage(content: Any, props: AMQP.BasicProperties)

case class AMQPPublish(content: Any, routingKey: String, props: AMQP.BasicProperties)

/**
 * @param a The actor to add as a Listener to this Dispatcher.
 */
case class AMQPAddListener(a: Actor)

/**
 * Reconnect to the AMQP Server after a delay of {@code delay} milliseconds.
 */
case class AMQPReconnect(delay: Long)

case object AMQPStop

object AMQPDispatcher {
  /**
   * lzma properties with:
   * encoder.SetEndMarkerMode(true)     // must set true
   * encoder.SetDictionarySize(1 << 20) // 1048576
   */
  val lzmaProps = Array[Byte](93, 0, 0, 16, 0)
}

/**
 * The dispatcher that listens over the AMQP message endpoint.
 * It manages a list of subscribers to the trade message and also sends AMQP
 * messages coming in to the queue/exchange to the list of observers.
 */
import AMQPDispatcher._
abstract class AMQPDispatcher(cf: ConnectionFactory, host: String, port: Int, val exchange: String) extends Actor {
  case class State(conn: Connection, channel: Channel, consumer: Consumer)

  private val log = Logger.getLogger(this.getClass.getName)

  private lazy val reconnectTimer = new Timer("AMQPReconnectTimer")
  private var as: List[Actor] = Nil

  private var state = connect
  protected def conn = state.conn
  protected def channel = state.channel
  protected def consumer = state.consumer

  private def connect: State = {
    val conn = cf.newConnection(host, port)
    val channel = conn.createChannel
    val consumer = configure(channel)
    State(conn, channel, consumer)
  }

  /**
   * Registers queue and consumer.
   * @throws IOException if an error is encountered
   * @return the newly created and registered (queue, consumer)
   */
  @throws(classOf[IOException])
  protected def configure(channel: Channel): Consumer

  def act {
    Actor.loop {
      react {
        case msg: AMQPMessage =>
          as foreach (_ ! msg)
        case AMQPAddListener(a) =>
          as ::= a
        case AMQPPublish(content, routingKey, props) => publish(content, routingKey, props)
        case AMQPReconnect(delay) => reconnect(delay)
        case AMQPStop => disconnect; exit
      }
    }
  }

  protected def publish(exchange: String, content: Any, routingKey: String, props: AMQP.BasicProperties) {
    import ContentType._

    val contentType = props.contentType match {
      case null | "" => JAVA_SERIALIZED_OBJECT
      case x => ContentType(x)
    }

    val body = contentType.mimeType match {
      case JAVA_SERIALIZED_OBJECT.mimeType => encodeJava(content)
      case JSON.mimeType => encodeJson(content)
      case _ => encodeJava(content)
    }

    val body1 = props.contentEncoding match {
      case "gzip" => gzip(body)
      case "lzma" => lzma(body)
      case _ => body
    }

    //println(content + " sent: routingKey=" + routingKey + " size=" + body.length)
    channel.basicPublish(exchange, routingKey, props, body1)
  }

  protected def publish(content: Any, routingKey: String, props: AMQP.BasicProperties) {
    publish(exchange, content, routingKey, props)
  }

  protected def disconnect = {
    if (consumer != null) {
      channel.basicCancel(consumer.asInstanceOf[DefaultConsumer].getConsumerTag)
    }
    try {
      channel.close
    } catch {
      case e: IOException => log.log(Level.INFO, "Could not close AMQP channel %s:%s [%s]", Array(host, port, this))
      case _ => ()
    }
    try {
      conn.close
      log.log(Level.FINEST, "Disconnected AMQP connection at %s:%s [%s]", Array(host, port, this))
    } catch {
      case e: IOException => log.log(Level.WARNING, "Could not close AMQP connection %s:%s [%s]", Array(host, port, this))
      case _ => ()
    }
  }

  protected def reconnect(delay: Long) = {
    disconnect
    try {
      connect
      log.log(Level.FINEST, "Successfully reconnected to AMQP Server %s:%s [%s]", Array(host, port, this))
    } catch {
      case e: Exception =>
        val waitInMillis = delay * 2
        val self = this
        log.log(Level.FINEST, "Trying to reconnect to AMQP server in %n milliseconds [%s]", Array(waitInMillis, this))
        reconnectTimer.schedule(new TimerTask {
            def run {
              self ! AMQPReconnect(waitInMillis)
            }
          }, delay)
    }
  }

  def encodeJava(content: Any): Array[Byte] = {
    val out = new ByteArrayOutputStream
    val oout = new ObjectOutputStream(out)
    oout.writeObject(content)
    oout.close

    out.toByteArray
  }

  def decodeJava(body: Array[Byte]): Any = {
    // deserialize
    val in = new ObjectInputStream(new ByteArrayInputStream(body))
    in.readObject
  }

  def encodeJson(content: Any): Array[Byte] = {
    val out = new ByteArrayOutputStream
    val wout = new OutputStreamWriter(out, "UTF-8")

    content match {
      case x: JsonSerializable => x.writeJson(wout)
      case _ => // todo
    }
    wout.close

    out.toByteArray
  }

  def decodeJson(instance: JsonSerializable, body: Array[Byte]): Any = {
    println(new String(body))
    val rin = new InputStreamReader(new ByteArrayInputStream(body))
    instance.readJson(rin)
    instance
  }

  def decodeJson(body: Array[Byte]): Any = {
    val reader = new InputStreamReader(new ByteArrayInputStream(body))
    JsonBuilder.readJson(reader)
  }

  @throws(classOf[IOException])
  def gzip(input: Array[Byte]): Array[Byte] = {
    val out = new ByteArrayOutputStream
    val bout = new BufferedOutputStream(new GZIPOutputStream(out))
    bout.write(input)
    bout.close

    val body = out.toByteArray

    out.close
    body
  }

  @throws(classOf[IOException])
  def ungzip(input: Array[Byte]): Array[Byte] = {
    val in = new ByteArrayInputStream(input)
    val bin = new BufferedInputStream(new GZIPInputStream(in))
    val out = new ByteArrayOutputStream
    
    val buf = new Array[Byte](1024)
    var len = -1
    while ({len = bin.read(buf); len > 0}) {
      out.write(buf, 0, len)
    }
    
    val body = out.toByteArray

    in.close
    bin.close
    out.close
    body
  }

  @throws(classOf[IOException])
  def lzma(input: Array[Byte]): Array[Byte] = {
    val in = new ByteArrayInputStream(input)
    val out = new ByteArrayOutputStream

    val encoder = new SevenZip.Compression.LZMA.Encoder
    encoder.SetEndMarkerMode(true)     // must set true
    encoder.SetDictionarySize(1 << 20) // 1048576

    encoder.Code(in, out, -1, -1, null)

    val body = out.toByteArray

    in.close
    out.close
    body
  }

  @throws(classOf[IOException])
  def unlzma(input: Array[Byte]): Array[Byte] = {
    val in = new ByteArrayInputStream(input)
    val out = new ByteArrayOutputStream

    val decoder = new SevenZip.Compression.LZMA.Decoder
    decoder.SetDecoderProperties(lzmaProps)
    decoder.Code(in, out, -1)
    
    val body = out.toByteArray

    in.close
    out.close
    body
  }

  class AMQPConsumer(channel: Channel) extends DefaultConsumer(channel) {
    override def handleDelivery(tag: String, env: Envelope, props: AMQP.BasicProperties, body: Array[Byte]) {
      import ContentType._

      val body1 = props.contentEncoding match {
        case "gzip" => ungzip(body)
        case "lzma" => unlzma(body)
        case _ => body
      }

      val contentType = props.contentType match {
        case null | "" => JAVA_SERIALIZED_OBJECT
        case x => ContentType(x)
      }

      val content = contentType.mimeType match {
        case JAVA_SERIALIZED_OBJECT.mimeType => decodeJava(body1)
        case JSON.mimeType =>
          contentType.attribute("class") match {
            case Some(clzName) =>
              try {
                Class.forName(clzName).newInstance match {
                  case x: JsonSerializable => decodeJson(x, body1)
                  case _ => decodeJson(body1)
                }
              } catch {case ex => ex.printStackTrace; decodeJson(body1)}
            case _ => decodeJson(body1)
          }
        case _ => decodeJava(body1)
      }

      // send back to interested observers for further relay
      val msg = AMQPMessage(content, props)
      as foreach (_ ! msg)

      // if noAck is set false, messages will be blocked until an ack to broker,
      // so it's better always ack it. (Although prefetch may deliver more than
      // one message to consumer)
      channel.basicAck(env.getDeliveryTag, false)
    }
  }
}