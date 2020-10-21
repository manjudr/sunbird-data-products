package org.sunbird.analytics.job.report

import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.{ArrayType, MapType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.{FrameworkContext, JobConfig, StorageConfig}
import org.scalamock.scalatest.MockFactory
import org.sunbird.analytics.util.UserData

import java.io.File
import java.time.{ZoneOffset, ZonedDateTime}

import cats.syntax.either._
import ing.wbaa.druid._
import ing.wbaa.druid.client.DruidClient
import io.circe._
import io.circe.parser._
import org.apache.spark.sql.functions._
import scala.collection.JavaConverters._

import scala.collection.mutable

class TestCollectionSummaryJob extends BaseReportSpec with MockFactory {


  var spark: SparkSession = _

  var courseBatchDF: DataFrame = _
  var userEnrolments: DataFrame = _
  var userDF: DataFrame = _
  var organisationDF: DataFrame = _
  var reporterMock: BaseReportsJob = mock[BaseReportsJob]
  val sunbirdCoursesKeyspace = "sunbird_courses"
  val sunbirdKeyspace = "sunbird"
  val esIndexName = "composite"

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = getSparkSession();

    courseBatchDF = spark
      .read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .load("src/test/resources/collection-summary/course_batch_data.csv")
      .cache()

    userEnrolments = spark
      .read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .load("src/test/resources/collection-summary/user_courses_data.csv")
      .cache()

    organisationDF = spark
      .read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .load("src/test/resources/collection-summary/orgTable.csv")
      .cache()

    userDF = spark.read.json("src/test/resources/collection-summary/user_data.json")
      .cache()

  }

  val convertMethod = udf((value: mutable.WrappedArray[String]) => {
    if (null != value && value.nonEmpty)
      value.toList.map(str => JSONUtils.deserialize(str)(manifest[Map[String, String]])).toArray
    else null
  }, new ArrayType(MapType(StringType, StringType), true))

  it should "generate the report for all the batches" in {

    (reporterMock.fetchData _)
      .expects(spark, Map("table" -> "course_batch", "keyspace" -> sunbirdCoursesKeyspace), "org.apache.spark.sql.cassandra", new StructType())
      .returning(courseBatchDF)

    (reporterMock.fetchData _)
      .expects(spark, Map("table" -> "user_enrolments", "keyspace" -> sunbirdCoursesKeyspace, "cluster" -> "LMSCluster"), "org.apache.spark.sql.cassandra", new StructType())
      .returning(userEnrolments.withColumn("certificates", convertMethod(split(userEnrolments.col("certificates"), ",").cast("array<string>"))))
      .anyNumberOfTimes()

    (reporterMock.fetchData _)
      .expects(spark, Map("table" -> "organisation", "keyspace" -> sunbirdKeyspace, "cluster" -> "LMSCluster"), "org.apache.spark.sql.cassandra", new StructType())
      .returning(organisationDF)
      .anyNumberOfTimes()

    val schema = Encoders.product[UserData].schema
    (reporterMock.fetchData _)
      .expects(spark, Map("table" -> "user", "infer.schema" -> "true", "key.column" -> "userid"), "org.apache.spark.sql.redis", schema)
      .anyNumberOfTimes()
      .returning(userDF)


    implicit val mockFc: FrameworkContext = mock[FrameworkContext]
    val strConfig = """{"search": {"type": "none"},"model": "org.sunbird.analytics.job.report.CourseMetricsJob","modelParams": {"batchFilters": ["TPD"],"fromDate": "$(date --date yesterday '+%Y-%m-%d')","toDate": "$(date --date yesterday '+%Y-%m-%d')","sparkCassandraConnectionHost": "127.0.0.0","sparkElasticsearchConnectionHost": "'$sunbirdPlatformElasticsearchHost'","sparkRedisConnectionHost": "'$sparkRedisConnectionHost'","sparkUserDbRedisIndex": "4","contentFilters": {"request": {"filters": {"framework": "TPD"},"sort_by": {"createdOn": "desc"},"limit": 10000,"fields": ["framework", "identifier", "name", "channel"]}},"reportPath": "course-reports/"},"output": [{"to": "console","params": {"printEvent": false}}],"parallelization": 8,"appName": "Course Dashboard Metrics","deviceMapping": false}""".stripMargin
    implicit val jobConfig = JSONUtils.deserialize[JobConfig](strConfig)
    val storageConfig = StorageConfig("local", "", "/tmp/course-metrics")
    CollectionSummaryJob.prepareReport(spark, reporterMock.fetchData, List())
  }


}
