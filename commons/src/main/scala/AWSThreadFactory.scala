package com.mfglabs.commons.aws

import java.util.concurrent._

class AWSThreadFactory(name: String) extends ForkJoinPool.ForkJoinWorkerThreadFactory {
  private val backingThreadFactory = ForkJoinPool.defaultForkJoinWorkerThreadFactory

  override def newThread(pool: ForkJoinPool): ForkJoinWorkerThread = {
    val thread = backingThreadFactory.newThread(pool)
    thread.setName(s"$name-${thread.getPoolIndex()}")
    thread
  }
}
