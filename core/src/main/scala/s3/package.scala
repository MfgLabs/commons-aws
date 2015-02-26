package com.mfglabs.commons.aws

/**
 * Asynchronous Scala client for S3 with facilities using Akka Stream.
 * On top of Pellucid Scala wrapper S3 client.
 *
 * Usage:
 * {{{
 *   import com.mfglabs.commons.aws.s3._
 *
 *   val builder = S3StreamBuilder(new AmazonS3AsyncClient()) // contains un-materialized composable Source / Flow / Sink
 *
 *   val fileStream: Source[ByteString] = builder.getFileAsStream(bucket, key)
 *
 *   val ops = new builder.Ops() // contains materialized methods on top of S3Stream.
 *                                 // you can optionnaly provide your own FlowMaterializer in Ops() constructor
 *
 *   val file: Future[ByteString] = ops.getFile(bucket, key)
 *   val deletedFile: Future[Unit] = ops.deleteFile(bucket, key)
 * }}}
 *
 * Please remark that you don't need any implicit [[scala.concurrent.ExecutionContext]] as it's directly provided
 * and managed by [[com.mfglabs.commons.aws.s3.AmazonS3AsyncClient]] itself.
 * There are smart [[com.mfglabs.commons.aws.s3.AmazonS3AsyncClient]] constructors that can be provided with custom.
 * [[java.util.concurrent.ExecutorService]] if you want to manage your pools of threads.
 */

package object s3 {

}
