# Changelog

This file summarizes the main changes for each release.

## <a name="0.12.1"></a>Version 0.12.1

 - Add the region as a mandatory parameter for all clients.
 - Add the `apply` and `from` constructor for `AmazonCloudwatchClient`.

### <a name="0.12.0"></a>Version 0.12.0

 - Rework the class hierarchy, for `S3` and `SQS`
   - The `Wrapper` wrap the amazon Java sdk function with no additional logic.
   - The `Client` extend it
   - For `ClientMaterialized` extend the `Client` and need an additional `Materializer` to transform a stream to `Future`.
 - Move `deleteObjects(bucket, prefix)` out of `AmazonS3Wrapper` to be able to use `AmazonS3ClientMaterialized.listFiles`
 - Switch to `ForkJoinPool` with daemon thread as default.


### <a name="0.11.0"></a>Version 0.11.0

 - Remove dependency to the unmaintained `dwhjames/aws-wrap` by including the necessary code
 - Split the project in three : `commons-aws-cloudwatch`, `commons-aws-s3`  and `commons-aws-sqs`
 - Upgrade dependencies including `aws-java-sdk` and `akka-stream-extensions`
 - Remove `CloudwatchAkkaHeartbeat`
 - Replace `AmazonCloudwatchClient` and `AmazonS3AsyncClient` constructors by an `apply` with default values.
 - The followind duplicated methods from`AmazonS3AsyncClient` were deleted :
   - `uploadFile` use `putObject` with `new PutObjectRequest().withCannedAcl(CannedAccessControlList.PublicReadWrite)`
   - `deleteFile` use `deleteObject`
   - `deleteFiles` use `deleteObjects`
