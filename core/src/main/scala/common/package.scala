package com.mfglabs.commons

import java.util.concurrent.{Executors, ExecutorService, LinkedBlockingQueue, ThreadFactory, ThreadPoolExecutor, TimeUnit}
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

}