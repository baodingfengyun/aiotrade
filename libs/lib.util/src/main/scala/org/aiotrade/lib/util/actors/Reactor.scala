package org.aiotrade.lib.util.actors

import akka.actor.Actor
import akka.actor.ActorSystem
import akka.actor.PoisonPill
import akka.actor.Props

/**
 * The counterpart to publishers. Listens to events from registered publishers.
 * 
 * @author Caoyuan Deng
 */
trait Reactor {

  /**
   * All reactions of this reactor.
   */
  val reactions: Reactions = new Reactions.Impl += {
    case x => //println("it seems messages that have no corresponding reactions will remain in mailbox?, anyway, just add this wild reaction for:\n" + x)
  }

  /**
   * Override for custom actor system and actor name, props etc.
   */
  lazy val underlying = Reactor.system.actorOf(Props(classOf[Reactor.UnderlyingActor], reactions))

  /**
   * Stop via message driven, so the reactor will react messages before finally exit.
   */
  def stop {
    underlying ! PoisonPill
  }

  def !(msg: Any) = {
    underlying ! msg
  }
  
  /**
   * Listen to the given publisher as long as <code>deafTo</code> isn't called for them.
   */
  def listenTo(ps: Publisher*) = for (p <- ps) p.subscribe(this)

  /**
   * Installed reaction won't receive events from the given publisher anylonger.
   */
  def deafTo(ps: Publisher*) = for (p <- ps) p.unsubscribe(this)
}

object Reactor {
  lazy val system = ActorSystem()
  
  final class UnderlyingActor(reactions: Reactions) extends Actor {
    def receive = {
      reactions
    }
  }
}