// EMR Studio Workspace (Spark kernel) HUDI upsert for BB card replay.
// Attach EMR Serverless app with role:
//   sms-card-enrichment-serverless-emr-runtime-role-prod
// Wait for Spark | Idle, then run.
// Prints HUDI BEFORE/AFTER views and [COUNTER] lines for change tracking.

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
val csvCount = csvDf.count()
println(s"[COUNTER] csv_distinct_cards=$csvCount")

val hudiBasePath = "s3://aeg-prod-bigdatadl-integration-sms/broadband/bb-card-status/"
val hudiDf = spark.read.format("hudi").load(hudiBasePath)
val hudiSchema = hudiDf.schema
val baseDf = spark.read.format("hudi").schema(hudiSchema).load(hudiBasePath)

val viewCols = Seq("cardid", "requesttype", "requeststatus", "requestdate", "_hoodie_commit_time")

// ---- BEFORE upsert ----
val beforeDf = baseDf.join(broadcast(csvDf), Seq("cardid"), "inner")
val beforeCount = beforeDf.count()
val notInHudiCount = csvCount - beforeCount
println(s"[COUNTER] hudi_matched_before=$beforeCount")
println(s"[COUNTER] cards_not_in_hudi=$notInHudiCount")
println("=== HUDI BEFORE (matched cards) ===")
beforeDf.select(viewCols.map(col): _*).orderBy(col("cardid")).show(200, false)

val matchedDf = beforeDf
val updatedDf = matchedDf
  .withColumn("requestType", lit("addCards"))
  .withColumn("requestStatus", lit("NOT_SENT"))
  .withColumn("requestDate", current_timestamp())
val upsertCount = updatedDf.count()
println(s"[COUNTER] rows_to_upsert=$upsertCount")

(updatedDf.write
  .format("hudi")
  .option("hoodie.datasource.write.operation", "upsert")
  .option("hoodie.datasource.write.recordkey.field", "cardid")
  .option("hoodie.datasource.write.partitionpath.field", "_hoodie_partition_path")
  .option("hoodie.datasource.write.precombine.field", "requestDate")
  .mode("append")
  .save(hudiBasePath)
)
println("[COUNTER] hudi_upsert_write=done")

// ---- AFTER upsert ----
val afterBase = spark.read.format("hudi").load(hudiBasePath)
val afterDf = afterBase.join(broadcast(csvDf), Seq("cardid"), "inner")
val afterCount = afterDf.count()
val afterReady = afterDf.filter(
  lower(col("requesttype")) === "addcards" &&
  upper(col("requeststatus")) === "NOT_SENT"
).count()
println(s"[COUNTER] hudi_matched_after=$afterCount")
println(s"[COUNTER] hudi_ready_addCards_NOT_SENT=$afterReady")
println(s"[COUNTER] hudi_changed_or_ready=$afterReady")
println("=== HUDI AFTER (matched cards) ===")
afterDf.select(viewCols.map(col): _*).orderBy(col("cardid")).show(200, false)

println("=== SUMMARY ===")
println(s"[COUNTER] csv_distinct_cards=$csvCount")
println(s"[COUNTER] hudi_matched_before=$beforeCount")
println(s"[COUNTER] rows_to_upsert=$upsertCount")
println(s"[COUNTER] hudi_matched_after=$afterCount")
println(s"[COUNTER] hudi_ready_addCards_NOT_SENT=$afterReady")
println(s"[COUNTER] cards_not_in_hudi=$notInHudiCount")
