package com.mfglabs.commons.aws

import java.sql.Connection

import com.mfglabs.commons.aws.commons.DockerEnv
import com.mfglabs.commons.aws.extensions.postgres.PostgresExtensions
import com.mfglabs.commons.aws.extensions.postgres.PostgresExtensions.{Table, PGCopyable}
import com.mfglabs.commons.aws.s3.AmazonS3Client
import org.postgresql.PGConnection
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, Matchers, FlatSpec}
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.io.Source

/**
 * Created by damien on 14/11/14.
 */
class CopyToS3Spec extends FlatSpec with Matchers with ScalaFutures with DockerEnv {

  val bucket = "mfg-commons-aws"

  val keyPrefix = "test/extensions/postgres_copy2s3/"
  val s3c = new AmazonS3Client()
  val pgExt = new PostgresExtensions(s3c)

  it should "copy a table to S3 as flat file" in {
    val stmt = conn.createStatement()
    stmt.execute("CREATE TABLE test_copy_to_s3(id bigint, mot text);")
    stmt.execute("INSERT INTO test_copy_to_s3 (mot) VALUES (1, 'veau'),(2, 'vache'),(3, 'cochon');")

    pgExt.copyToS3AsFlatFile(Table("public","test_copy_to_s3") , ";", bucket, keyPrefix + "flat.tsv")(conn.asInstanceOf[PGConnection])

    val flatS3Obj = Await.result(s3c.getObject(bucket, keyPrefix + "flat.tsv"),1 minute)
    Source.fromInputStream(flatS3Obj.getObjectContent)
    pgExt.copyToS3AsFlatFile(Table("public","test_copy_to_s3") , "\t", bucket, keyPrefix + "flat.tsv")(conn.asInstanceOf[PGConnection])


    stmt.execute("CREATE SCHEMA monschema;")
    stmt.execute("CREATE TABLE monschema.test_copy_to_s3 AS (SELECT * FROM test_copy_to_s3);")


  }
}
