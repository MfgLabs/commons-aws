package com.mfglabs.commons.aws

import java.util.concurrent.{Executors, ThreadFactory}
import java.util.concurrent.atomic.AtomicLong

class AWSThreadFactory(name: String) extends ThreadFactory {
  private val count = new AtomicLong(0L)
  private val backingThreadFactory: ThreadFactory = Executors.defaultThreadFactory()
  override def newThread(r: Runnable): Thread = {
    val thread = backingThreadFactory.newThread(r)
    thread.setName(s"$name-${count.getAndIncrement()}")
    thread
  }
}
