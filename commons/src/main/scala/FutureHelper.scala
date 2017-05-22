package com.mfglabs.commons.aws

import com.amazonaws.AmazonWebServiceRequest
import com.amazonaws.ClientConfiguration
import com.amazonaws.handlers.AsyncHandler

import scala.concurrent.{Future, Promise}
import java.util.concurrent.{Future => JFuture, LinkedBlockingQueue, ThreadPoolExecutor, TimeUnit}

package object FutureHelper {

  def promiseToAsyncHandler[Request <: AmazonWebServiceRequest, Result](p: Promise[Result]) =
    new AsyncHandler[Request, Result] {
      override def onError(exception: Exception): Unit = { p.failure(exception); () }
      override def onSuccess(request: Request, result: Result): Unit = { p.success(result); () }
    }

  @inline
  def wrapAsyncMethod[Request <: AmazonWebServiceRequest, Result](
    f:       (Request, AsyncHandler[Request, Result]) => JFuture[Result],
    request: Request
  ): Future[Result] = {
    val p = Promise[Result]
    f(request, promiseToAsyncHandler(p))
    p.future
  }

  def defaultExecutorService(clientConfiguration: ClientConfiguration, factoryName: String) = new ThreadPoolExecutor(
    0, clientConfiguration.getMaxConnections,
    60L, TimeUnit.SECONDS,
    new LinkedBlockingQueue[Runnable],
    new AWSThreadFactory(factoryName)
  )

}
