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

import com.amazonaws.event.{ProgressListener, ProgressEvent, ProgressEventType}
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

  private val logger: Logger = LoggerFactory.getLogger("com.github.dwhjames.awswrap.s3.FutureTransfer")

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
        progressEvent.getEventType match {
          case ProgressEventType.CLIENT_REQUEST_FAILED_EVENT =>
            debugLog("CLIENT_REQUEST_FAILED_EVENT")
          case ProgressEventType.CLIENT_REQUEST_RETRY_EVENT =>
            debugLog("CLIENT_REQUEST_RETRY_EVENT")
          case ProgressEventType.CLIENT_REQUEST_STARTED_EVENT =>
            debugLog("CLIENT_REQUEST_STARTED_EVENT")
          case ProgressEventType.CLIENT_REQUEST_SUCCESS_EVENT =>
            debugLog("CLIENT_REQUEST_SUCCESS_EVENT")
          case ProgressEventType.HTTP_REQUEST_COMPLETED_EVENT =>
            debugLog("HTTP_REQUEST_COMPLETED_EVENT")
          case ProgressEventType.HTTP_REQUEST_CONTENT_RESET_EVENT =>
            debugLog("HTTP_REQUEST_CONTENT_RESET_EVENT")
          case ProgressEventType.HTTP_REQUEST_STARTED_EVENT =>
            debugLog("HTTP_REQUEST_STARTED_EVENT")
          case ProgressEventType.HTTP_RESPONSE_COMPLETED_EVENT =>
            debugLog("HTTP_RESPONSE_COMPLETED_EVENT")
          case ProgressEventType.HTTP_RESPONSE_CONTENT_RESET_EVENT =>
            debugLog("HTTP_RESPONSE_CONTENT_RESET_EVENT")
          case ProgressEventType.HTTP_RESPONSE_STARTED_EVENT =>
            debugLog("HTTP_RESPONSE_STARTED_EVENT")
          case ProgressEventType.REQUEST_BYTE_TRANSFER_EVENT =>
            debugLog("REQUEST_BYTE_TRANSFER_EVENT")
          case ProgressEventType.REQUEST_CONTENT_LENGTH_EVENT =>
            debugLog("REQUEST_CONTENT_LENGTH_EVENT")
          case ProgressEventType.RESPONSE_BYTE_DISCARD_EVENT =>
            debugLog("RESPONSE_BYTE_DISCARD_EVENT")
          case ProgressEventType.RESPONSE_BYTE_TRANSFER_EVENT =>
            debugLog("RESPONSE_BYTE_TRANSFER_EVENT")
          case ProgressEventType.RESPONSE_CONTENT_LENGTH_EVENT =>
            debugLog("RESPONSE_CONTENT_LENGTH_EVENT")
          case ProgressEventType.TRANSFER_CANCELED_EVENT =>
            debugLog("TRANSFER_CANCELED_EVENT")
          case ProgressEventType.TRANSFER_COMPLETED_EVENT =>
            debugLog("TRANSFER_COMPLETED_EVENT")
          case ProgressEventType.TRANSFER_FAILED_EVENT =>
            debugLog("TRANSFER_FAILED_EVENT")
          case ProgressEventType.TRANSFER_PART_COMPLETED_EVENT =>
            debugLog("TRANSFER_PART_COMPLETED_EVENT")
          case ProgressEventType.TRANSFER_PART_FAILED_EVENT =>
            debugLog("TRANSFER_PART_FAILED_EVENT")
          case ProgressEventType.TRANSFER_PART_STARTED_EVENT =>
            debugLog("TRANSFER_PART_STARTED_EVENT")
          case ProgressEventType.TRANSFER_PREPARING_EVENT =>
            debugLog("TRANSFER_PREPARING_EVENT")
          case ProgressEventType.TRANSFER_STARTED_EVENT =>
            debugLog("TRANSFER_STARTED_EVENT")
          case _ =>
            logger.warn(s"unrecognized progress event type for transfer $transferDescription")
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

        val code = progressEvent.getEventType()
        if (code == ProgressEventType.TRANSFER_CANCELED_EVENT ||
            code == ProgressEventType.TRANSFER_COMPLETED_EVENT ||
            code == ProgressEventType.TRANSFER_FAILED_EVENT) {
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
