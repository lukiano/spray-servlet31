package spray.servlet31

import spray.servlet.{Initializer, ConnectorSettings}
import javax.servlet.http.{HttpServlet, HttpServletResponse, HttpServletRequest}
import akka.actor.{ActorRef, ActorSystem}
import akka.event.{Logging, LoggingAdapter}
import akka.spray.RefUtils
import javax.servlet.{AsyncEvent, AsyncListener, AsyncContext}

class Servlet31ConnectorServlet extends HttpServlet {
  var system: ActorSystem = _
  var serviceActor: ActorRef = _
  var timeoutHandler: ActorRef = _
  implicit var settings: ConnectorSettings = _
  implicit var log: LoggingAdapter = _

  /**
   * The same as Spray Servlet 3.0
   */
  override def init() {
    import Initializer._
    system = getServletContext.getAttribute(SystemAttrName).asInstanceOf[ActorSystem]
    serviceActor = getServletContext.getAttribute(ServiceActorAttrName).asInstanceOf[ActorRef]
    settings = getServletContext.getAttribute(SettingsAttrName).asInstanceOf[ConnectorSettings]
    timeoutHandler = if (settings.timeoutHandler.isEmpty) serviceActor else system.actorFor(settings.timeoutHandler)
    require(system != null, "No ActorSystem configured")
    require(serviceActor != null, "No ServiceActor configured")
    require(settings != null, "No ConnectorSettings configured")
    require(RefUtils.isLocal(serviceActor), "The serviceActor must live in the same JVM as the Servlet30ConnectorServlet")
    require(RefUtils.isLocal(timeoutHandler), "The timeoutHandler must live in the same JVM as the Servlet30ConnectorServlet")
    log = Logging(system, this.getClass)
    log.info("Initialized Servlet API 3.1 <=> Spray Connector")
  }

  /**
   * Service returns quickly, to free HTTP thread pool.
   * @param hsRequest servlet request.
   * @param hsResponse servlet response.
   */
  override def service(hsRequest: HttpServletRequest, hsResponse: HttpServletResponse) {
    def requestStringForLog: String = "%s request to '%s'" format(hsRequest.getMethod, ModelConverter.rebuildUri(hsRequest))
    val asyncContext = hsRequest.startAsync()
    asyncContext.setTimeout(settings.requestTimeout.toMillis)
    asyncContext.addListener(new AsyncContextListener(asyncContext, requestStringForLog))
  }

  /**
   * Listen to timeout and error events.
   * @param asyncContext the context that this listener belongs to.
   * @param requestStringForLog friendly string that represents the request. Used for logging.
   */
  private class AsyncContextListener(private val asyncContext: AsyncContext,
                                     private val requestStringForLog: String) extends AsyncListener {

    /**
     * onStartAsync should be run quick enough for this to be set before a timeout.
     */
    var responder: Responder = null

    /**
     * A timeout happened. Functionality copied from Spray Servlet 3.0
     * @param event timeout event data.
     */
    def onTimeout(event: AsyncEvent) {
      log.warning("Timeout of {}", requestStringForLog)
      if (responder != null) responder.callTimeout(timeoutHandler)
      asyncContext.complete()
    }

    /**
     * An error happened. We log it.
     * @param event error event data.
     */
    def onError(event: AsyncEvent) {
      event.getThrowable match {
        case null ⇒ log.error("Unspecified Error during async processing of {}", requestStringForLog)
        case ex ⇒ log.error(ex, "Error during async processing of {}", requestStringForLog)
      }
    }

    /**
     * Parse request headers and hooks to read listener. Responder will take charge once read is completed.
     * @param event startAsync event data.
     */
    def onStartAsync(event: AsyncEvent) {
      val hsRequest = asyncContext.getRequest.asInstanceOf[HttpServletRequest]
      val futureRequest = ModelConverter.toHttpRequest(hsRequest)
      responder = new Responder(system, log, settings, asyncContext, requestStringForLog,
        futureRequest, serviceActor)
    }

    /**
     * We do nothing here.
     * @param event complete event data.
     */
    def onComplete(event: AsyncEvent) {
      log.debug("onComplete event of {}", requestStringForLog)
    }

  }

  /**
   * The same as Spray Servlet 3.0
   */
  override def destroy() {
    if (!system.isTerminated) {
      system.shutdown()
      system.awaitTermination()
    }
  }

}
