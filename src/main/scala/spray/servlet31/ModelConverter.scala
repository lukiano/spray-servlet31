package spray.servlet31

import java.io.{ByteArrayOutputStream, IOException}
import javax.servlet.http.HttpServletRequest
import scala.collection.JavaConverters._
import akka.event.LoggingAdapter
import spray.http.parser.HttpParser
import spray.http._
import HttpHeaders._
import StatusCodes._
import javax.servlet.ReadListener
import spray.servlet.ConnectorSettings
import scala.concurrent.{Promise, Future}
import java.util.concurrent.atomic.AtomicInteger

/**
 * Similar to the one from Spray Servlet 3.0, but using {@link javax.servlet.ReadListener}.
 * If content-length is known, will build an Array[Byte]. Else, a ByteArrayOutputStream.
 */
private[servlet31] object ModelConverter {

  /**
   * Parse request headers and hooks to read listener.
   * @param hsRequest servlet request.
   * @param settings spray servlet connection settings.
   * @param log facade to log events.
   * @return a Future that will be complete once the request body has been read.
   */
  def toHttpRequest(hsRequest: HttpServletRequest)(implicit settings: ConnectorSettings, log: LoggingAdapter):
  Future[HttpRequest] = {
    val rawHeaders = hsRequest.getHeaderNames.asScala.map { name ⇒
      RawHeader(name, hsRequest.getHeaders(name).asScala mkString ", ")
    }.toList
    val (errors, parsedHeaders) = HttpParser.parseHeaders(rawHeaders)
    if (!errors.isEmpty) errors.foreach(e ⇒ log.warning(e.formatPretty))
    val (contentType, contentLength) = parsedHeaders.foldLeft[(Option[ContentType], Option[Int])](None -> None) {
      case ((None, cl), `Content-Type`(ct))   ⇒ Some(ct) -> cl
      case ((ct, None), `Content-Length`(cl)) ⇒ ct -> Some(cl)
      case (result, _)                        ⇒ result
    }

    if (contentLength.isDefined && contentLength.get > settings.maxContentLength)
      throw new IllegalRequestException(RequestEntityTooLarge, ErrorInfo("HTTP message Content-Length " +
      contentLength.get + " exceeds the configured limit of " + settings.maxContentLength))


    val promise = Promise[HttpRequest]()
    val inputStream = hsRequest.getInputStream
    val readListener = new ReadListener {

      val buf:Either[Array[Byte], ByteArrayOutputStream] = if (contentLength.isDefined && contentLength.get > 0)
        Left(new Array[Byte](contentLength.get))
      else
        Right(new ByteArrayOutputStream())

      var bytesRead: AtomicInteger = new AtomicInteger(0)

      def fillLeftBuffer() {
        while (inputStream.isReady) {
          buf.left.get(bytesRead.get) = inputStream.read().toByte
          bytesRead.incrementAndGet()
          if (bytesRead.get > contentLength.get) {
            val e  = new RequestProcessingException(InternalServerError, "Illegal Servlet request entity, " +
              "expected length " + contentLength + " but only has length " + bytesRead)
            log.error(e, "Could not read request entity")
            promise.complete(new scala.util.Failure[HttpRequest](e))
            inputStream.close()
          }
        }
      }

      def fillRightBuffer() {
        while (inputStream.isReady) {
          buf.right.get.write(inputStream.read())
        }
      }

      def onDataAvailable() {
        try {
          if (buf.isLeft) fillLeftBuffer() else fillRightBuffer()
        } catch {
          case e: IOException ⇒
            log.error(e, "Could not read request entity")
            promise.complete(new scala.util.Failure[HttpRequest](new RequestProcessingException(InternalServerError, "Could not read request entity")))
        }
      }

      def getBuffer = if (buf.isLeft) buf.left.get else buf.right.get.toByteArray

      def onAllDataRead() {
        try {
          val request = HttpRequest(
            method = toHttpMethod(hsRequest.getMethod),
            uri = rebuildUri(hsRequest),
            headers = addRemoteAddressHeader(hsRequest, rawHeaders),
            entity = toHttpEntity(getBuffer, contentType),
            protocol = toHttpProtocol(hsRequest.getProtocol))
            promise.complete(new scala.util.Success(request))
        } catch {
          case t: Throwable => promise.complete(new scala.util.Failure[HttpRequest](t))
        }
      }

      def onError(t: Throwable) {
        promise.complete(new scala.util.Failure[HttpRequest](t))
      }
    }
    inputStream.setReadListener(readListener)
    promise.future
  }

  def toHttpMethod(name: String) =
    HttpMethods.getForKey(name)
      .getOrElse(throw new IllegalRequestException(MethodNotAllowed, ErrorInfo("Illegal HTTP method", name)))

  def rebuildUri(hsRequest: HttpServletRequest)(implicit settings: ConnectorSettings, log: LoggingAdapter) = {
    val requestUri = hsRequest.getRequestURI
    val uri = settings.rootPath match {
      case ""                                         ⇒ requestUri
      case rootPath if requestUri startsWith rootPath ⇒ requestUri substring rootPath.length
      case rootPath ⇒
        log.warning("Received request outside of configured root-path, request uri '{}', configured root path '{}'",
          requestUri, rootPath)
        requestUri
    }
    val queryString = hsRequest.getQueryString
    try Uri(if (queryString != null && queryString.length > 0) uri + '?' + queryString else uri)
    catch {
      case e: IllegalUriException ⇒
        throw new IllegalRequestException(BadRequest, ErrorInfo("Illegal request URI", e.getMessage))
    }
  }

  def addRemoteAddressHeader(hsr: HttpServletRequest, headers: List[HttpHeader])(implicit settings: ConnectorSettings): List[HttpHeader] =
    if (settings.remoteAddressHeader) `Remote-Address`(hsr.getRemoteAddr) :: headers
    else headers

  def toHttpProtocol(name: String) =
    HttpProtocols.getForKey(name)
      .getOrElse(throw new IllegalRequestException(BadRequest, ErrorInfo("Illegal HTTP protocol", name)))

  def toHttpEntity(buf: Array[Byte], contentType: Option[ContentType]): HttpEntity = {
    if (contentType.isEmpty) HttpEntity(buf) else HttpEntity(contentType.get, buf)
  }

}