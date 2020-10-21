package org.sunbird.analytics.job.report

import java.util.concurrent.CompletableFuture
import java.util.concurrent.atomic.AtomicInteger

import com.datastax.spark.connector.cql.CassandraConnectorConf
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.cassandra.CassandraSparkSessionFunctions
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataTypes, LongType, StructType}
import org.apache.spark.storage.StorageLevel
import org.ekstep.analytics.framework.Level.INFO
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.util.DatasetUtil.extensions
import org.ekstep.analytics.framework.util.{CommonUtil, JSONUtils, JobLogger}
import org.ekstep.analytics.framework.{FrameworkContext, IJob, JobConfig}
import org.sunbird.analytics.util.{CourseUtils, DecryptUtil, UserData}

import scala.collection.immutable.List


case class Metrics(totalRequests: Option[Int], failedRequests: Option[Int], successRequests: Option[Int])

case class CollectionBatch(batchId: String, courseId: String, startDate: String, endDate: String)

case class CourseMetrics()

case class CollectionBatchResponses(batchId: String, status: String, execTime: Long)

object CollectionSummaryJob extends optional.Application with IJob with BaseReportsJob {
  val cassandraUrl = "org.apache.spark.sql.cassandra"
  private val userCacheDBSettings = Map("table" -> "user", "infer.schema" -> "true", "key.column" -> "userid")
  private val userEnrolmentDBSettings = Map("table" -> "user_enrolments", "keyspace" -> AppConf.getConfig("sunbird.courses.keyspace"), "cluster" -> "LMSCluster")
  private val organisationDBSettings = Map("table" -> "organisation", "keyspace" -> "sunbird", "cluster" -> "LMSCluster")
  implicit val className: String = "org.sunbird.analytics.job.report.CollectionSummaryJob"
  val jobName = "CollectionSummaryJob"

  private val columnsOrder = List("Published by", "Collection id", "Collection name", "Batch start date", "Batch end date", "Total Enrolments", "Total Completion", "Total Enrolment from same org", "Total Completion from same org",
    "Is certified course", "Total Certificate issues", "Average elapsed time to complete the course", "Generated on");

  private val columnMapping = Map("publishedBy" -> "Published by", "collectionId" -> "Collection id", "collectionName" -> "Collection name", "batchStartDate" -> "Batch start date",
    "batchEndDate" -> "Batch end date", "enrolmentCount" -> "Total Enrolments", "completionCount" -> "Total Completion",
    "enrolCountBySameOrg" -> "Total Enrolment from same org", "completionCountBySameOrg" -> "Total Completion from same org",
    "isCertified" -> "Is certified course",
    "certificateIssuesCount" -> "Total Certificate issues",
    "avgElapsedTime" -> "Average elapsed time to complete the course", "generatedOn" -> "Generated on")


  override def main(config: String)(implicit sc: Option[SparkContext] = None, fc: Option[FrameworkContext] = None) {

    JobLogger.init(jobName)
    JobLogger.start(s"$jobName started executing", Option(Map("config" -> config, "model" -> jobName)))
    implicit val jobConfig: JobConfig = JSONUtils.deserialize[JobConfig](config)
    implicit val spark: SparkSession = openSparkSession(jobConfig)
    implicit val frameworkContext: FrameworkContext = getReportingFrameworkContext()
    init()
    val conf = config.split(";")
    val batchIds = if (conf.length > 1) {
      conf(1).split(",").toList
    } else List()

    try {
      val res = CommonUtil.time(execute(batchIds))
      JobLogger.end(s"$jobName completed execution", "SUCCESS", Option(Map("timeTaken" -> res._1)))
    } finally {
      frameworkContext.closeContext()
      spark.close()
    }

  }

  def init()(implicit spark: SparkSession, fc: FrameworkContext, config: JobConfig) {
    DecryptUtil.initialise()
    spark.setCassandraConf("UserCluster", CassandraConnectorConf.ConnectionHostParam.option(AppConf.getConfig("sunbird.user.cluster.host")))
    spark.setCassandraConf("LMSCluster", CassandraConnectorConf.ConnectionHostParam.option(AppConf.getConfig("sunbird.courses.cluster.host")))
    spark.setCassandraConf("ContentCluster", CassandraConnectorConf.ConnectionHostParam.option(AppConf.getConfig("sunbird.content.cluster.host")))
  }


  def execute(batchList: List[String])(implicit spark: SparkSession, fc: FrameworkContext, config: JobConfig) = {

    val time = CommonUtil.time({
      prepareReport(spark, fetchData, batchList)
    });
  }

  def loadData(spark: SparkSession, settings: Map[String, String], url: String, schema: StructType): DataFrame = {
    if (schema.nonEmpty) {
      spark.read.schema(schema).format(url).options(settings).load()
    }
    else {
      spark.read.format(url).options(settings).load()
    }
  }

  def getUserData(spark: SparkSession, fetchData: (SparkSession, Map[String, String], String, StructType) => DataFrame): DataFrame = {
    val schema = Encoders.product[UserData].schema
    fetchData(spark, userCacheDBSettings, "org.apache.spark.sql.redis", schema)
      .withColumn("username", concat_ws(" ", col("firstname"), col("lastname"))).persist(StorageLevel.MEMORY_ONLY)
  }

  def getUserEnrollment(spark: SparkSession, courseId: String, batchId: String, fetchData: (SparkSession, Map[String, String], String, StructType) => DataFrame): DataFrame = {
    fetchData(spark, userEnrolmentDBSettings, cassandraUrl, new StructType())
      .where(col("courseid") === courseId && col("batchid") === batchId && lower(col("active")).equalTo("true") && col("enrolleddate").isNotNull)
      .select(col("batchid"), col("userid"), col("courseid"), col("active")
        , col("completionpercentage"), col("enrolleddate"), col("completedon"), col("status"), col("certificates"))
  }

  def getOrganisationDetails(spark: SparkSession, channel: String, fetchData: (SparkSession, Map[String, String], String, StructType) => DataFrame): DataFrame = {
    fetchData(spark, organisationDBSettings, cassandraUrl, new StructType()).select("channel", "orgname", "id")
      .where(col("channel") === channel)
  }


  def prepareReport(spark: SparkSession, fetchData: (SparkSession, Map[String, String], String, StructType) => DataFrame, batchList: List[String])(implicit fc: FrameworkContext, config: JobConfig): CourseMetrics = {
    implicit val sparkSession: SparkSession = spark
    implicit val sqlContext: SQLContext = spark.sqlContext
    val res = CommonUtil.time({
      val userDF = getUserData(spark, fetchData = fetchData).select("userid", "orgname", "userchannel")
      (userDF.count(), userDF)
    })
    JobLogger.log("Time to fetch user details", Some(Map("timeTaken" -> res._1, "count" -> res._2._1)), INFO)
    val userCachedDF = res._2._2;

    val encoder = Encoders.product[CollectionBatch];
    val activeBatches: List[CollectionBatch] = CourseUtils.getActiveBatches(fetchData, batchList, AppConf.getConfig("sunbird.courses.keyspace"))
      .as[CollectionBatch](encoder).collect().toList
    val activeBatchesCount = new AtomicInteger(activeBatches.length)
    val result: List[CollectionBatchResponses] = for (collectionBatch <- activeBatches) yield {
      val response = processBatch(userCachedDF, collectionBatch, fetchData)
      JobLogger.log("Batch is processed", Some(Map("batchId" -> response.batchId, "execTime" -> response.execTime, "filePath" -> response.file, "remainingBatch" -> activeBatchesCount.getAndDecrement())), INFO)
      response
    }
    CompletableFuture.allOf(result: _*) // Wait for all the async tasks to complete
    val completedResult = result.map(f => f.join()); // Get the completed job requests
    CourseMetrics()
  }

  def processBatch(userCacheDF: DataFrame, collectionBatch: CollectionBatch, fetchData: (SparkSession, Map[String, String], String, StructType) => DataFrame)
                  (implicit spark: SparkSession, config: JobConfig): CollectionBatchResponses = {
    implicit val sparkSession: SparkSession = spark
    implicit val sqlContext: SQLContext = spark.sqlContext
    val filteredContents = CourseUtils.filterContents(spark, JSONUtils.serialize(Map("request" -> Map("filters" -> Map("identifier" -> collectionBatch.courseId, "status" -> Array("Live", "Unlisted", "Retired")), "fields" -> Array("channel", "identifier", "name")))))
    val channel = filteredContents.map(res => res.channel).headOption.getOrElse("")
    val collectionName = filteredContents.map(res => res.name).headOption.getOrElse("")
    val organisationDF = getOrganisationDetails(spark, channel, fetchData).select("orgname", "channel").collect() // Filter by channel and returns channel, orgname, id fields
    val userEnrolment = getUserEnrollment(spark, collectionBatch.courseId, collectionBatch.batchId, fetchData).join(userCacheDF, Seq("userid"), "inner")
    val publisherName = organisationDF.headOption.getOrElse(Row()).getString(0)
    val completedUsers = userEnrolment.where(col("status") === 2)
    val avgElapsedTime = getAvgElapsedTime(completedUsers)
    val userInfo = userEnrolment.withColumn("publishedBy", lit(publisherName))
      .withColumn("collectionName", lit(collectionName))
      .withColumn("collectionId", lit(collectionBatch.courseId))
      .withColumn("batchStartDate", lit(collectionBatch.startDate))
      .withColumn("batchEndDate", lit(collectionBatch.endDate))
      .withColumn("enrolmentCount", lit(userEnrolment.select("userid").distinct().count()))
      .withColumn("completionCount", lit(completedUsers.select("userid").distinct().count()))
      .withColumn("generatedOn", date_format(from_utc_timestamp(current_timestamp.cast(DataTypes.TimestampType), "Asia/Kolkata"), "yyyy-MM-dd'T'HH:mm:ss'Z'"))
      .withColumn("isCertified", when(col("certificates").isNotNull && size(col("certificates").cast("array<map<string, string>>")) > 0, "Y").otherwise("N"))
      .withColumn("completionCountBySameOrg", lit(completedUsers.where(col("userchannel") === channel).count()))
      .withColumn("enrolCountBySameOrg", lit(userEnrolment.where(col("userchannel") === channel).select("userid").distinct().count()))
      .withColumn("avgElapsedTime", lit(avgElapsedTime))
      .withColumn("certificateIssuesCount", lit(10)) // TODO: Compute this logic
      .drop("userid", "batchid", "active", "courseid", "completionpercentage", "enrolleddate", "completedon", "status", "certificates", "orgname", "userchannel")
    saveToBlob(userInfo, collectionBatch.batchId)
    CollectionBatchResponses(null, null, null)
  }

  def getAvgElapsedTime(usersEnrolmentDF: DataFrame): Long = {
    import org.apache.spark.sql.functions._
    val updatedUser = usersEnrolmentDF
      .withColumn("enrolDate", to_timestamp(col("enrolleddate"), fmt = "yyyy-MM-dd HH:mm:ss"))
      .withColumn("completedDate", to_timestamp(col("completedon"), fmt = "yyyy-MM-dd HH:mm:ss"))
      .withColumn("diffInMinutes", round(col("completedDate").cast(LongType) - col("enrolDate").cast(LongType)) / 60) // Converting to mins
    println("updatedUser" + updatedUser.show(false))
    updatedUser.agg(sum("diffInMinutes").cast("long")).first.getLong(0) / updatedUser.select("userid").distinct().count()
  }

  def saveToBlob(reportData: DataFrame, batchId: String): Unit = {
    val container = AppConf.getConfig("cloud.container.reports")
    val objectKey = AppConf.getConfig("course.metrics.cloud.objectKey")
    val storageConfig = getStorageConfig(container, objectKey)
    val reportPath = "collection-summary-reports/"
    val fields = reportData.schema.fieldNames
    val colNames = for (e <- fields) yield columnMapping.getOrElse(e, e)
    val dynamicColumns = fields.toList.filter(e => !columnMapping.keySet.contains(e))
    val columnWithOrder = (columnsOrder ::: dynamicColumns).distinct
    reportData.toDF(colNames: _*).select(columnWithOrder.head, columnWithOrder.tail: _*)
      .saveToBlobStore(storageConfig, "csv", reportPath + "report-" + batchId, Option(Map("header" -> "true")), None)
  }
}

