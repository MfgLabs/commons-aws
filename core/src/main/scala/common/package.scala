package com.mfglabs.commons

import com.amazonaws.AmazonWebServiceRequest
import com.amazonaws.handlers.AsyncHandler

import scala.concurrent.{Future, Promise}
import java.util.concurrent.{Executors, ThreadFactory, Future => JFuture}
import java.util.concurrent.atomic.AtomicLong

package object aws {

  class AWSThreadFactory(name: String) extends ThreadFactory {
    private val count = new AtomicLong(0L)
    private val backingThreadFactory: ThreadFactory = Executors.defaultThreadFactory()
    override def newThread(r: Runnable): Thread = {
      val thread = backingThreadFactory.newThread(r)
      thread.setName(s"$name-${count.getAndIncrement()}")
      thread
    }
  }

  def promiseToAsyncHandler[Request <: AmazonWebServiceRequest, Result](p: Promise[Result]) =
    new AsyncHandler[Request, Result] {
      override def onError(exception: Exception): Unit = { p.failure(exception); () }
      override def onSuccess(request: Request, result: Result): Unit = { p.success(result); () }
    }

  def promiseToVoidAsyncHandler[Request <: AmazonWebServiceRequest](p: Promise[Unit]) =
    new AsyncHandler[Request, Void] {
      override def onError(exception: Exception): Unit = { p.failure(exception); () }
      override def onSuccess(request: Request, result: Void): Unit = { p.success(()); () }
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

  @inline
  def wrapVoidAsyncMethod[Request <: AmazonWebServiceRequest](
    f:       (Request, AsyncHandler[Request, Void]) => JFuture[Void],
    request: Request
  ): Future[Unit] = {
    val p = Promise[Unit]
    f(request, promiseToVoidAsyncHandler(p))
    p.future
  }
}
