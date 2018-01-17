/*
 * Copyright 2012-2015 Pellucid Analytics
 * Copyright 2015 Daniel W. H. James
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mfglabs.commons.aws
package s3

import scala.concurrent.{Future, Promise}

import com.amazonaws.event.{ProgressListener, ProgressEvent}
import com.amazonaws.services.s3.transfer.Transfer

import org.slf4j.{Logger, LoggerFactory}


/**
  * A helper object providing a Scala Future interface for S3 Transfers.
  *
  * Transfers to and from S3 using the TransferManager provider a listener
  * interface, and [[FutureTransfer.listenFor]] adapts this interface to
  * Scala futures.
  *
  * @see [[com.amazonaws.services.s3.transfer.TransferManager TransferManager]]
  * @see [[com.amazonaws.services.s3.transfer.Transfer Transfer]]
  */
object FutureTransfer {

  private val logger: Logger = LoggerFactory.getLogger("com.mfglabs.commons.aws.s3.FutureTransfer")

  /**
    * Attach a listener to an S3 Transfer and return it as a Future.
    *
    * This helper method attaches a progress and state change listeners to the given
    * Transfer object. The returned future is completed with the
    * same transfer when the transfer is ‘done’ (canceled, completed,
    * or failed). The future will always been completed successfully
    * even if the transfer itself has failed. It is up to the caller
    * to extract the result of the transfer and perform any error
    * handling.
    *
    * In essence, this helper just gives back the transfer when it is done.
    *
    * The detailed progress of the transfer is logged at debug level to the
    * `com.github.dwhjames.awswrap.s3.FutureTransfer` logger.
    *
    * @tparam T
    *     a subtype of Transfer.
    * @param transfer
    *     an S3 Transfer to listen for progress.
    * @return the transfer in a future.
    * @see [[com.amazonaws.services.s3.transfer.Transfer Transfer]]
    * @see [[com.amazonaws.event.ProgressListener ProgressListener]]
    */
  def listenFor[T <: Transfer](transfer: T): Future[transfer.type] = {
    import com.amazonaws.services.s3.transfer.internal.{ AbstractTransfer, TransferStateChangeListener }
    val transferDescription = transfer.getDescription
    def debugLog(eventType: String): Unit = {
      logger.debug(s"$eventType : $transferDescription")
    }
    def logTransferState(state: Transfer.TransferState): Unit = {
      if (logger.isDebugEnabled) {
        state match {
          case Transfer.TransferState.Waiting =>
            debugLog("Waiting")
          case Transfer.TransferState.InProgress =>
            debugLog("InProgress")
          case Transfer.TransferState.Completed =>
            debugLog("Completed")
          case Transfer.TransferState.Canceled =>
            debugLog("Canceled")
          case Transfer.TransferState.Failed =>
            debugLog("Failed")
          case _ =>
            logger.warn(s"unrecognized transfer state for transfer $transferDescription")
        }
      }
    }

    def logProgressEvent(progressEvent: ProgressEvent): Unit = {
      if (logger.isDebugEnabled) {
        progressEvent.getEventCode match {
          case ProgressEvent.CANCELED_EVENT_CODE        => debugLog("CANCELED_EVENT_CODE")
          case ProgressEvent.COMPLETED_EVENT_CODE       => debugLog("COMPLETED_EVENT_CODE")
          case ProgressEvent.FAILED_EVENT_CODE          => debugLog("FAILED_EVENT_CODE")
          case ProgressEvent.PART_COMPLETED_EVENT_CODE  => debugLog("PART_COMPLETED_EVENT_CODE")
          case ProgressEvent.PART_FAILED_EVENT_CODE     => debugLog("PART_FAILED_EVENT_CODE")
          case ProgressEvent.PART_STARTED_EVENT_CODE    => debugLog("PART_STARTED_EVENT_CODE")
          case ProgressEvent.PREPARING_EVENT_CODE       => debugLog("PREPARING_EVENT_CODE")
          case ProgressEvent.RESET_EVENT_CODE           => debugLog("RESET_EVENT_CODE")
          case ProgressEvent.STARTED_EVENT_CODE         => debugLog("STARTED_EVENT_CODE")
          case _ =>
            logger.warn(s"unrecognized progress event code for transfer $transferDescription")
        }
      }
    }

    val p = Promise[transfer.type]

    if (transfer.isInstanceOf[AbstractTransfer]) {
      /* Attach a state change listener to the transfer.
       * At this point, the transfer is already in progress
       * and may even have already completed. We will have
       * missed any state change events that have already been
       * fired, including the completion event!
       */
      transfer.asInstanceOf[AbstractTransfer].addStateChangeListener(new TransferStateChangeListener {
        /* Note that the transferStateChanged will be called in the Java SDK’s
         * special thread for callbacks, so any blocking calls here have
         * the potential to induce deadlock.
         */
        override def transferStateChanged(t: Transfer, state: Transfer.TransferState): Unit = {
          logTransferState(state)

          if (state == Transfer.TransferState.Completed ||
              state == Transfer.TransferState.Canceled ||
              state == Transfer.TransferState.Failed) {
            val success = p trySuccess transfer
            if (logger.isDebugEnabled) {
              if (success) {
                logger.debug(s"promise successfully completed from transfer state change listener for $transferDescription")
              }
            }
          }
        }
      })
    }

    /* Attach a progress listener to the transfer.
     * At this point, the transfer is already in progress
     * and may even have already completed. We will have
     * missed any progress events that have already been
     * fired, including the completion event!
     */
    transfer.addProgressListener(new ProgressListener {
      /* Note that the progressChanged will be called in the Java SDK’s
       * `java-sdk-progress-listener-callback-thread` special thread
       * for progress event callbacks, so any blocking calls here have
       * the potential to induce deadlock.
       */
      override def progressChanged(progressEvent: ProgressEvent): Unit = {
        logProgressEvent(progressEvent)

        val code = progressEvent.getEventCode()
        if (code == ProgressEvent.CANCELED_EVENT_CODE  ||
            code == ProgressEvent.COMPLETED_EVENT_CODE  ||
            code == ProgressEvent.FAILED_EVENT_CODE ) {
          val success = p trySuccess transfer
          if (logger.isDebugEnabled) {
            if (success) {
              logger.debug(s"promise successfully completed from progress listener for $transferDescription")
            }
          }
        }
      }
    })

    /* In case the progress listener never fires due to the
     * transfer already being done, poll the transfer once.
     */
    if (transfer.isDone) {
      val success = p trySuccess transfer
      if (logger.isDebugEnabled) {
        if (success) {
          logger.debug(s"promise successfully completed from outside of callbacks for $transferDescription")
        }
      }
    }

    p.future
  }
}
