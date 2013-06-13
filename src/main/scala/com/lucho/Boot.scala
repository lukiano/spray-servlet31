package com.lucho

import spray.servlet.WebBoot
import akka.actor.{ActorSystem, Props}

class Boot extends WebBoot {

  // we need an ActorSystem to host our application in
  val system = ActorSystem("servlet31")

  // the service actor replies to incoming HttpRequests
  val serviceActor = system.actorOf(Props[RouteActor])

  system.registerOnTermination {
    // put additional cleanup code here
    system.log.info("Application shut down")
  }
}
