// BB card HUDI upsert for EMR Studio Workspace (Spark kernel).
// Prerequisites:
//   1. Workbook attached to EMR Serverless app with role:
//      sms-card-enrichment-serverless-emr-runtime-role-prod
//   2. cards2replay.csv already uploaded to S3 (step 1 of replay_bb_cards.py)
//
// Paste into a notebook cell after Spark is Idle, or run via:
//   python3 replay_bb_cards.py print-scala

%%configure -f
{
    "conf": {
        "spark.jars": "/usr/lib/hudi/hudi-spark-bundle.jar",
        "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
        "spark.hadoop.hive.metastore.client.factory.class": "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
    }
}

import org.apache.spark.sql.functions._

val csvPath = "s3://dtv-prod-bigdatadl-330572541641-sms/tmp/cards2replay.csv"
val csvDf = spark.read.option("header", "true").csv(csvPath).select(col("card10").as("cardid")).distinct()

val hudiBasePath = "s3://aeg-prod-bigdatadl-integration-sms/broadband/bb-card-status/"
val hudiDf = spark.read.format("hudi").load(hudiBasePath)
val hudiSchema = hudiDf.schema
val baseDf = spark.read.format("hudi").schema(hudiSchema).load(hudiBasePath)

val matchedDf = baseDf.join(broadcast(csvDf), Seq("cardid"), "inner")
val updatedDf = matchedDf
  .withColumn("requestType", lit("addCards"))
  .withColumn("requestStatus", lit("NOT_SENT"))
  .withColumn("requestDate", current_timestamp())

println(s"Cards matched for upsert: ${matchedDf.count()}")

(updatedDf.write
  .format("hudi")
  .option("hoodie.datasource.write.operation", "upsert")
  .option("hoodie.datasource.write.recordkey.field", "cardid")
  .option("hoodie.datasource.write.partitionpath.field", "_hoodie_partition_path")
  .option("hoodie.datasource.write.precombine.field", "requestDate")
  .mode("append")
  .save(hudiBasePath)
)

// Optional verification after job completes:
(spark.read
  .format("hudi")
  .load(hudiBasePath)
  .orderBy(desc("_hoodie_commit_time"))
  .select("cardid", "requesttype", "requeststatus", "_hoodie_commit_time")
  .show(50, false)
)
