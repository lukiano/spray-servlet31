package spray.servlet31

import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import javax.servlet.{WriteListener, AsyncContext}
import scala.concurrent.duration.Duration
import akka.io.Tcp
import spray.http._
import akka.actor.{ActorSystem, ActorRef}
import java.io.{ByteArrayInputStream, IOException}
import javax.servlet.http.HttpServletResponse
import akka.spray.UnregisteredActorRef
import scala.util.control.NonFatal
import akka.event.LoggingAdapter
import spray.servlet.ConnectorSettings
import java.util.concurrent.ConcurrentLinkedQueue
import scala.concurrent.Future
import scala.util.Try
import scala.util.Failure
import spray.http.ChunkedResponseStart
import akka.actor.UnhandledMessage
import spray.http.HttpResponse
import scala.util.Success
import spray.http.HttpRequest
import spray.http.SetTimeoutTimeout
import spray.http.Timedout
import spray.http.SetRequestTimeout

/**
 * Actor that handles writing to the response stream, hooking to {@link javax.servlet.WriteListener }
 * @param system actorSystem that will contain this actor.
 * @param log facade to log events.
 * @param settings spray servlet connection settings.
 * @param asyncContext context to obtain the response OutputStream.
 * @param requestString friendly string that represents the request. Used for logging.
 * @param futureRequest Once completed, will hold the request data (or a failure)
 * @param serviceActor spray routing to pass the request (once completed) and wait for any response.
 */
private[servlet31] class Responder(system: ActorSystem, log: LoggingAdapter, settings: ConnectorSettings,
                asyncContext: AsyncContext, requestString: String, futureRequest: Future[HttpRequest],
                serviceActor: ActorRef) extends UnregisteredActorRef(system) {

  // Constants and mutable state used when dealing with chunks of data. Copied from Spray Servlet 3.0
  final val OPEN = 0
  final val STARTED = 1
  final val COMPLETED = 2
  val state = new AtomicInteger(OPEN)

  /**
   * Will contain the request once its future is completed. Maybe it should be Atomic.
   */
  private var theRequest: Option[HttpRequest] = None

  /**
   * @return a friendly string that represents the request. Used for logging.
   */
  private def requestStringForLog: String = theRequest.map(_.toString).getOrElse(requestString)

  private val queue: ConcurrentLinkedQueue[(ByteArrayInputStream, Option[PostProcessMessage], String)] =
    new ConcurrentLinkedQueue[(ByteArrayInputStream, Option[PostProcessMessage], String)]


  /**
   * The first time {@link WriteListener#onWritePossible() } is called, we need to extract
   * the request data from the Future. I know it will be completed because
   * the listener is set at {@link Future#onComplete() }.
   */
  private val firstTime = new AtomicBoolean(true)

  /**
   * Servlet response.
   */
  private val hsResponse = asyncContext.getResponse.asInstanceOf[HttpServletResponse]

  /**
   * Servlet 3.1 Write Listener
   */
  private val writeListener = new WriteListener {

    /**
     * Log the error event.
     * @param t the error that occured.
     */
    def onError(t: Throwable) {
      log.error(t, "Error during async processing of {}", requestStringForLog)
    }

    /**
     * Will be called by the servlet container. The first time, it will extract the data from the request and
     * pass it to spray routing.
     */
    def onWritePossible() {
      if (firstTime.get()) {
        firstTime.set(false)
        processRequestFromFuture()
      } else {
        tryWriteFromQueue()
      }
    }
  }

  //TODO maybe set the listener after the first response from spray routing.
  futureRequest.onComplete({
    case _ => hsResponse.getOutputStream.setWriteListener(writeListener)
  })(system.dispatcher)

  /**
   * The timeout duration can vary with a call to SetTimeout
   */
  private var timeoutTimeout: Duration = settings.timeoutTimeout

  /**
   * Helper method to handle null strings.
   * @param s a possibly null String.
   * @return s or an empty String if s is null.
   */
  private def nullAsEmpty(s: String): String = if (s == null) "" else s

  private def processRequestFromFuture() {
      val successOrFailure: Try[HttpRequest] = futureRequest.value.get
    successOrFailure match {
      case Success(request: HttpRequest) ⇒ {
        theRequest = Some(request)
        serviceActor.tell(request, this)
      }
      case Failure(t: Throwable) ⇒ t match {
        case e: IllegalRequestException ⇒ {
          log.warning("Illegal request {}\n\t{}\n\tCompleting with '{}' response",
            requestStringForLog, e.info.formatPretty, e.status)
          writeResponse(HttpResponse(e.status, e.info.format(settings.verboseErrorMessages)))
        }
        case e: RequestProcessingException ⇒ {
          log.warning("Request {} could not be handled normally\n\t{}\n\tCompleting with '{}' response",
            requestStringForLog, e.info.formatPretty, e.status)
          writeResponse(HttpResponse(e.status, e.info.format(settings.verboseErrorMessages)))
        }
        case NonFatal(e) ⇒ {
          log.error(e, "Error during processing of request {}", requestStringForLog)
          writeResponse(HttpResponse(500, entity = "The request could not be handled"))
        }
      }

    }
  }

  private def tryWriteFromQueue() {
    if (!queue.isEmpty) {
      val (byteInputStream, postProcessMessage, responseString) = queue.peek()
      var error: Option[Throwable] = None
      try {
        while (hsResponse.getOutputStream.isReady && byteInputStream.available > 0) {
          hsResponse.getOutputStream.write(byteInputStream.read())
        }
      } catch {
        case e: IOException ⇒
          log.error("Could not write response body, probably the request has either timed out or the client has " +
            "disconnected\nRequest: {}\nResponse: {}\nError: {}", requestStringForLog, responseString, e)
          error = Some(e)
        case NonFatal(e) ⇒
          log.error("Could not complete request\nRequest: {}\nResponse: {}\nError: {}", requestStringForLog, responseString, e)
          error = Some(e)
      }
      if (error.isDefined || byteInputStream.available() == 0) {
        queue.poll()
        if (postProcessMessage.isDefined) {
          if (postProcessMessage.get.completeContext) asyncContext.complete()
          postProcess(error, postProcessMessage.get)
        }
      }
    }
  }

  private case class PostProcessMessage(completeContext: Boolean, ack: Option[Any], close: Boolean, sender: ActorRef)
  private def postProcess(error: Option[Throwable], postProcessMessage: PostProcessMessage) {
    error match {
      case None ⇒
        postProcessMessage.ack.foreach(postProcessMessage.sender.tell(_, this))
        if (postProcessMessage.close) postProcessMessage.sender.tell(Tcp.Closed, this)
      case Some(e) ⇒
        postProcessMessage.sender.tell(Tcp.ErrorClosed(nullAsEmpty(e.getMessage)), this)
        asyncContext.complete()
    }
  }

  def handle(message: Any)(implicit sender: ActorRef) {
    val trueSender = sender
    message match {
      case wrapper: HttpMessagePartWrapper if wrapper.messagePart.isInstanceOf[HttpResponsePart] ⇒
        wrapper.messagePart.asInstanceOf[HttpResponsePart] match {
          case response: HttpResponse ⇒
            if (state.compareAndSet(OPEN, COMPLETED)) {
              writeResponse(response, Some(PostProcessMessage(completeContext = true, wrapper.ack, close = true, trueSender)))
            } else state.get match {
              case STARTED ⇒
                log.warning("Received an HttpResponse after a ChunkedResponseStart, dropping ...\nRequest: {}\nResponse: {}", requestStringForLog, response)
              case COMPLETED ⇒
                log.warning("Received a second response for a request that was already completed, dropping ...\nRequest: {}\nResponse: {}", requestStringForLog, response)
            }

          case response: ChunkedResponseStart ⇒
            if (state.compareAndSet(OPEN, STARTED)) {
              writeResponse(response, Some(PostProcessMessage(completeContext = false, wrapper.ack, close = false, trueSender)))
            } else state.get match {
              case STARTED ⇒
                log.warning("Received a second ChunkedResponseStart, dropping ...\nRequest: {}\nResponse: {}", requestStringForLog, response)
              case COMPLETED ⇒
                log.warning("Received a ChunkedResponseStart for a request that was already completed, dropping ...\nRequest: {}\nResponse: {}", requestStringForLog, response)
            }

          case MessageChunk(body, _) ⇒ state.get match {
            case OPEN ⇒
              log.warning("Received a MessageChunk before a ChunkedResponseStart, dropping ...\nRequest: {}\nChunk: {} bytes\n", requestStringForLog, body.length)
            case STARTED ⇒
              writeChunk(body, hsResponse, PostProcessMessage(completeContext = false, wrapper.ack, close = false, trueSender))
            case COMPLETED ⇒
              log.warning("Received a MessageChunk for a request that was already completed, dropping ...\nRequest: {}\nChunk: {} bytes", requestStringForLog, body.length)
          }

          case _: ChunkedMessageEnd ⇒
            if (state.compareAndSet(STARTED, COMPLETED)) {
              closeResponseStream(hsResponse, wrapper.ack, trueSender)
            } else state.get match {
              case OPEN ⇒
                log.warning("Received a ChunkedMessageEnd before a ChunkedResponseStart, dropping ...\nRequest: {}", requestStringForLog)
              case COMPLETED ⇒
                log.warning("Received a ChunkedMessageEnd for a request that was already completed, dropping ...\nRequest: {}", requestStringForLog)
            }
        }

      case msg@SetRequestTimeout(timeout) ⇒
        state.get match {
          case COMPLETED ⇒ notCompleted(msg)
          case _ ⇒
            val millis = if (timeout.isFinite()) timeout.toMillis else 0
            asyncContext.setTimeout(millis)
        }

      case msg@SetTimeoutTimeout(timeout) ⇒
        state.get match {
          case COMPLETED ⇒ notCompleted(msg)
          case _ ⇒ timeoutTimeout = timeout
        }

      case x ⇒ system.eventStream.publish(UnhandledMessage(x, sender, this))
    }
  }

  private def notCompleted(msg: Any) {
    log.warning("Received a {} for a request that was already completed, dropping ...\nRequest: {}", msg, requestStringForLog)
  }

  private def writeChunk(buffer: Array[Byte], hsResponse: HttpServletResponse, postProcessMessage: PostProcessMessage) {
    queue.add((new ByteArrayInputStream(buffer), Some(postProcessMessage), hsResponse.toString))
    tryWriteFromQueue()
  }

  private def closeResponseStream(hsResponse: HttpServletResponse, ack: Option[Any], sender: ActorRef) {
    var error: Option[Throwable] = None
    try {
      asyncContext.complete()
    } catch {
      case e: IOException ⇒
        log.error("Could not close response stream, probably the request has either timed out or the client has " +
          "disconnected\nRequest: {}\nError: {}", requestStringForLog, e)
        error = Some(e)
      case NonFatal(e) ⇒
        log.error("Could not close response stream\nRequest: {}\nError: {}", requestStringForLog, e)
        error = Some(e)
    }
    postProcess(error, PostProcessMessage(completeContext = false, ack, close = true, sender))
  }

  private def writeResponse(response: HttpMessageStart with HttpResponsePart,
                    postProcessMessage: Option[PostProcessMessage] = None) {

    val resp = response.message.asInstanceOf[HttpResponse]
    hsResponse.setStatus(resp.status.intValue)
    resp.headers.foreach {
      header ⇒
        header.lowercaseName match {
          case "content-type" ⇒ // we never render these headers here, because their production is the
          case "content-length" ⇒ // responsibility of the spray-servlet layer, not the user
          case _ ⇒ hsResponse.addHeader(header.name, header.value)
        }
    }
      resp.entity match {
        case EmptyEntity ⇒ {
          if (postProcessMessage.isDefined) {
            if (postProcessMessage.get.completeContext) asyncContext.complete()
            postProcess(None, postProcessMessage.get)
          }
        }
        case HttpBody(contentType, buffer) ⇒ {
          hsResponse.addHeader("Content-Type", contentType.value)
          if (response.isInstanceOf[HttpResponse]) hsResponse.addHeader("Content-Length", buffer.length.toString)
          queue.add((new ByteArrayInputStream(buffer), postProcessMessage, response.toString))
          tryWriteFromQueue()
        }
      }
  }

  def callTimeout(timeoutHandler: ActorRef) {
    val timeOutResponder = new UnregisteredActorRef(system) {
      def handle(message: Any)(implicit sender: ActorRef) {
        message match {
          case x: HttpResponse ⇒ writeResponse(x, None)
          case x ⇒ system.eventStream.publish(UnhandledMessage(x, sender, this))
        }
      }
    }

    writeResponse(timeoutResponse(), None)
    if (timeoutTimeout.isFinite() && theRequest.isDefined) {
      timeoutHandler.tell(Timedout(theRequest.get), timeOutResponder)
    }

  }

  private def timeoutResponse(): HttpResponse = HttpResponse(
    status = 500,
    entity = "Ooops! The server was not able to produce a timely response to your request.\n" +
      "Please try again in a short while!")


}