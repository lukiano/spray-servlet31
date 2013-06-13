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

  private val queue: ConcurrentLinkedQueue[(ByteArrayInputStream, PostProcessMessage, String)] =
    new ConcurrentLinkedQueue[(ByteArrayInputStream, PostProcessMessage, String)]


  /**
   * The first time data arrives and the queue is filled with something,
   * we hook the listener so it can start moving data from the queue to the OutputStream
   * whenever the Servlet Container wants.
   */
  private val writeListenerSet = new AtomicBoolean(false)

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
      tryWriteFromQueue()
    }
  }

  futureRequest.onComplete(processRequestFromFuture)(system.dispatcher)

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

  private def processRequestFromFuture(successOrFailure: Try[HttpRequest]) {
    successOrFailure match {
      case Success(request: HttpRequest) ⇒ {
        theRequest = Some(request)
        serviceActor.tell(request, this)
      }
      case Failure(t: Throwable) ⇒ t match {
        case e: IllegalRequestException ⇒ {
          log.warning("Illegal request {}\n\t{}\n\tCompleting with '{}' response",
            requestStringForLog, e.info.formatPretty, e.status)
          writeResponse(HttpResponse(e.status, e.info.format(settings.verboseErrorMessages)), PostProcessMessage(close = true))
        }
        case e: RequestProcessingException ⇒ {
          log.warning("Request {} could not be handled normally\n\t{}\n\tCompleting with '{}' response",
            requestStringForLog, e.info.formatPretty, e.status)
          writeResponse(HttpResponse(e.status, e.info.format(settings.verboseErrorMessages)), PostProcessMessage(close = true))
        }
        case NonFatal(e) ⇒ {
          log.error(e, "Error during processing of request {}", requestStringForLog)
          writeResponse(HttpResponse(500, entity = "The request could not be handled"), PostProcessMessage(close = true))
        }
      }

    }
  }

  /**
   * If there is data in the queue, try to write it to the response OutputStream, while it's ready.
   */
  private def tryWriteFromQueue() {
    if (!queue.isEmpty) {
      val (byteInputStream, postProcessMessage, responseAsStringForLog) = queue.peek()
      val tryToWrite: Try[Unit] = Try {
        while (hsResponse.getOutputStream.isReady && byteInputStream.available > 0) {
          hsResponse.getOutputStream.write(byteInputStream.read())
        }
      }
      tryToWrite match {
        case Failure(e) ⇒ e match {
          case ioe: IOException ⇒
            log.error("Could not write response body, probably the request has either timed out or the client has " +
              "disconnected\nRequest: {}\nResponse: {}\nError: {}",
              requestStringForLog, responseAsStringForLog, ioe)
          case another ⇒
            log.error("Could not complete request\nRequest: {}\nResponse: {}\nError: {}",
              requestStringForLog, responseAsStringForLog, another)
        }
      }
      //val error: Option[Throwable] = (tryToWrite map {case _ => None} recover { case t => Some(t)}).toOption.flatten

      if (tryToWrite.isFailure || byteInputStream.available() == 0) {
        queue.poll()
        postProcess(tryToWrite, postProcessMessage)
      }
    }
  }

  /**
   * Defines what to do after some data has been written to the stream.
   * @param ack if something, send it back to the sender.
   * @param close if true, close the stream (complete) and tell the sender that we are closed.
   * @param sender the actor that sent us the data.
   */
  private case class PostProcessMessage(close: Boolean, sender: Option[ActorRef] = None, ack: Option[Any] = None)

  private def postProcess(error: Try[_], postProcessMessage: PostProcessMessage) {
    error match {
      case Success(_) ⇒ {
        postProcessMessage.ack.foreach(ack => postProcessMessage.sender.foreach(sender => sender.tell(ack, this)))
        if (postProcessMessage.close) {
          asyncContext.complete()
          if (postProcessMessage.sender.isDefined) {
            postProcessMessage.sender.get.tell(Tcp.Closed, this)
          }
        }
      }
      case Failure(e) ⇒ {
        asyncContext.complete()
        if (postProcessMessage.sender.isDefined) {
          postProcessMessage.sender.get.tell(Tcp.ErrorClosed(nullAsEmpty(e.getMessage)), this)
        }
      }
    }
  }

  /**
   * Method to handle messages that this Actor receives.
   * @param message an actor message.
   * @param sender the actor that sent the message.
   */
  def handle(message: Any)(implicit sender: ActorRef) {
    val trueSender = sender
    message match {
      case wrapper: HttpMessagePartWrapper if wrapper.messagePart.isInstanceOf[HttpResponsePart] ⇒
        wrapper.messagePart.asInstanceOf[HttpResponsePart] match {
          case response: HttpResponse ⇒
            if (state.compareAndSet(OPEN, COMPLETED)) {
              writeResponse(response, PostProcessMessage(close = true, Some(trueSender), wrapper.ack))
            } else state.get match {
              case STARTED ⇒
                log.warning("Received an HttpResponse after a ChunkedResponseStart, dropping ...\nRequest: {}\nResponse: {}", requestStringForLog, response)
              case COMPLETED ⇒
                log.warning("Received a second response for a request that was already completed, dropping ...\nRequest: {}\nResponse: {}", requestStringForLog, response)
            }

          case response: ChunkedResponseStart ⇒
            if (state.compareAndSet(OPEN, STARTED)) {
              writeResponse(response, PostProcessMessage(close = false, Some(trueSender), wrapper.ack))
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
              writeChunk(body, PostProcessMessage(close = false, Some(trueSender), wrapper.ack))
            case COMPLETED ⇒
              log.warning("Received a MessageChunk for a request that was already completed, dropping ...\nRequest: {}\nChunk: {} bytes", requestStringForLog, body.length)
          }

          case _: ChunkedMessageEnd ⇒
            if (state.compareAndSet(STARTED, COMPLETED)) {
              postProcess(Success(), PostProcessMessage(close = true, Some(trueSender), wrapper.ack))
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

  /**
   * Log that a message came but the response was already committed.
   * @param msg the unwanted message.
   */
  private def notCompleted(msg: Any) {
    log.warning("Received a {} for a request that was already completed, dropping ...\nRequest: {}",
      msg, requestStringForLog)
  }

  /**
   * Write a chunk of data to the queue, and maybe to the OutputStream if it's ready.
   * @param buffer data
   * @param postProcessMessage defines what to do after the data is sent to the stream.
   */
  private def writeChunk(buffer: Array[Byte], postProcessMessage: PostProcessMessage) {
    queue.add((new ByteArrayInputStream(buffer), postProcessMessage, hsResponse.toString))
    tryWriteFromQueue()
  }

  private def writeResponse(response: HttpMessageStart with HttpResponsePart,
                    postProcessMessage: PostProcessMessage) {

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
        postProcess(Success(), postProcessMessage)
      }
      case HttpBody(contentType, buffer) ⇒ {
        hsResponse.addHeader("Content-Type", contentType.value)
        if (response.isInstanceOf[HttpResponse]) hsResponse.addHeader("Content-Length", buffer.length.toString)
        queue.add((new ByteArrayInputStream(buffer), postProcessMessage, response.toString))

        if (!writeListenerSet.get()) {
          writeListenerSet.set(true)
          hsResponse.getOutputStream.setWriteListener(writeListener)
        } else {
          tryWriteFromQueue()
        }


      }
    }
  }

  /**
   * public method to be called by AsyncListener.
   * @param timeoutHandler actor to tell that a timeOut happened.
   */
  def callTimeout(timeoutHandler: ActorRef) {
    val timeOutResponder = new UnregisteredActorRef(system) {
      def handle(message: Any)(implicit sender: ActorRef) {
        message match {
          case x: HttpResponse ⇒ writeResponse(x, PostProcessMessage(close = false))
          case x ⇒ system.eventStream.publish(UnhandledMessage(x, sender, this))
        }
      }
    }

    writeResponse(timeoutResponse(), PostProcessMessage(close = false))
    if (timeoutTimeout.isFinite() && theRequest.isDefined) {
      timeoutHandler.tell(Timedout(theRequest.get), timeOutResponder)
    }

  }

  /**
   * @return a Timeout Response to send to the client.
   */
  private def timeoutResponse(): HttpResponse = HttpResponse(
    status = 500,
    entity = "Ooops! The server was not able to produce a timely response to your request.\n" +
      "Please try again in a short while!")


}