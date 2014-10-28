## MFG Commons AWS Scala API

## S3

It enhances default AWS `AmazonS3Client` for Scala :
  - asynchronous/non-blocking wrapper using an external pool of threads managed internally.
  - a few file management/streaming facilities.

> It is based on [Opensource Pellucid wrapper](https://github.com/pellucidanalytics/aws-wrap).

To use rich MFGLabs AWS S3 wrapper, you just have to add the following:

```scala
import com.mfglabs.commons.aws.s3
import s3._ // brings implicit extensions

// Create the client
val S3 = new AmazonS3Client()
// Use it
for {
  _   <- S3.uploadStream(bucket, "big.txt", Enumerator.fromFile(new java.io.File(s"big.txt")))
  l   <- S3.listFiles(bucket)
  _   <- S3.deleteFile(bucket, "big.txt")
  l2  <- S3.listFiles(bucket)
} yield (l, l2)
```

Please remark that you don't need any implicit `scala.concurrent.ExecutionContext` as it's directly provided
and managed by [[AmazonS3Client]] itself.

There are also smart `AmazonS3Client` constructors that can be provided with custom.
`java.util.concurrent.ExecutorService` if you want to manage your pools of threads.
