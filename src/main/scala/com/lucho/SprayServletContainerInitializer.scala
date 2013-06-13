package com.lucho

import javax.servlet.{ServletContext, ServletContainerInitializer}
import java.util
import spray.servlet31.Servlet31ConnectorServlet

class SprayServletContainerInitializer extends ServletContainerInitializer {

  def onStartup(c: util.Set[Class[_]], ctx: ServletContext) {
    ctx.addListener("spray.servlet.Initializer")
    val sprayServlet = ctx.addServlet("SprayConnectorServlet", classOf[Servlet31ConnectorServlet])
    sprayServlet.setAsyncSupported(true)
    sprayServlet.addMapping("/*")
    sprayServlet.setLoadOnStartup(1)
  }

}
