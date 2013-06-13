package com.lucho

import akka.actor.Props
import spray.util.SprayActorLogging
import spray.routing.HttpServiceActor
import reactivemongo.core.actors.{MonitorActor, MongoDBSystem}
import reactivemongo.api.{DefaultDB, MongoConnection}

class RouteActor extends HttpServiceActor with SprayActorLogging with Routes {
  import context.dispatcher

  val mongoSystemActor = actorRefFactory.actorOf(Props(new MongoDBSystem(List("localhost"), Seq.empty, 10 )))
  val monitorActor = actorRefFactory.actorOf(Props(new MonitorActor(mongoSystemActor)))
  val connection = new MongoConnection(context.system, mongoSystemActor, monitorActor)

  lazy val db: DefaultDB = connection("networkblame")

  def receive = runRoute(routes)

}
