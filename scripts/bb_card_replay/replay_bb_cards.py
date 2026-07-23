#!/usr/bin/env python3
"""
BB card AMS replay helper.

Steps:
  1) Athena: find eligible cards (default sampleid <> '05'), save CSV, upload to S3
  2) Print Scala for EMR Studio HUDI upsert
  3) Copy CSV to replay/YYYYMMDD/ and create Athena table
  4) Print day-after verification SQLs

Examples:
  python3 replay_bb_cards.py discover \\
      --profile <aws-profile-1641> \\
      --athena-output s3://dtv-prod-bigdatadl-330572541641-sms/tmp/athena-results/

  python3 replay_bb_cards.py print-scala
  python3 replay_bb_cards.py register-table --profile <aws-profile-1641> \\
      --athena-output s3://dtv-prod-bigdatadl-330572541641-sms/tmp/athena-results/
  python3 replay_bb_cards.py check --check-date 2026-05-01
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

try:
    import boto3
    from botocore.exceptions import BotoCoreError, ClientError
except ImportError:  # pragma: no cover
    boto3 = None
    BotoCoreError = ClientError = Exception  # type: ignore


REGION = "us-east-1"
BUCKET = "dtv-prod-bigdatadl-330572541641-sms"
CSV_KEY = "tmp/cards2replay.csv"
CSV_S3 = f"s3://{BUCKET}/{CSV_KEY}"
HUDI_BASE = "s3://aeg-prod-bigdatadl-integration-sms/broadband/bb-card-status/"
EMR_ROLE = "sms-card-enrichment-serverless-emr-runtime-role-prod"
DATABASE = "integration"
WORKGROUP = "primary"
SAMPLE_ID = "05"
LOCAL_CSV = "cards2replay.csv"

STB_MODELS = (
    "C31-700", "C41-100", "C41-500", "C41-700", "C41W-100", "C41W-500",
    "C51-100", "C61K-700", "D10", "D10-100", "D10-200", "D10-300", "D11",
    "D11-100", "D11-300", "D11-500", "D11-800", "D11I-100", "D12-100",
    "D12-300", "D12-500", "D12-700", "H20", "H20-100", "H20-600", "H21-100",
    "H21-200", "H23-600", "H24-100", "H24-200", "H24-700", "H25-100",
    "H25-500", "H25-700", "H44-100", "H44-500", "HR20-100", "HR20-700",
    "HR20I-100", "HR21-100", "HR21-200", "HR21-700", "HR21P-200", "HR22-100",
    "HR23-700", "HR24-100", "HR24-200", "HR24-500", "HR34-700", "HR44-200",
    "HR44-500", "HR44-700", "HR54-200", "HR54-500", "HR54-700", "R15",
    "R15-100", "R15-300", "R15-500", "R15-700", "R16-100", "R16-300",
    "R16-500", "R16-700", "R22-100", "R22-200", "HR54R1-700", "HR54R1-500",
    "HS17-500", "HS17-100",
)


class Error(RuntimeError):
    pass


def log(msg: str) -> None:
    print(msg, flush=True)


def session(profile: Optional[str], region: str):
    if boto3 is None:
        raise Error("boto3 required: pip install -r requirements.txt")
    kwargs: Dict[str, Any] = {"region_name": region}
    if profile:
        kwargs["profile_name"] = profile
    return boto3.Session(**kwargs)


def today() -> dt.date:
    return dt.datetime.now(dt.timezone.utc).date()


def parse_date(value: str) -> dt.date:
    return dt.date.fromisoformat(value)


def table_name(d: dt.date) -> str:
    return f"ams_replay_{d.strftime('%m%d%y')}"


def replay_prefix(d: dt.date) -> str:
    return f"s3://{BUCKET}/replay/{d.strftime('%Y%m%d')}/"


def replay_key(d: dt.date) -> str:
    return f"replay/{d.strftime('%Y%m%d')}/cards2replay.csv"


def eligible_sql(sample_eq: bool = False) -> str:
    op = "=" if sample_eq else "<>"
    models = ",".join(f"'{m}'" for m in STB_MODELS)
    return f"""
SELECT
    t1.card10,
    t1.stb_model,
    t1.callback_date,
    t2.last_event_time,
    to_hex(substr(s.subregions, 9, 1)) AS sampleid
FROM integration.sms_card_enrichment t1
LEFT JOIN integration.sms_card_enrichment_with_ams_last_event t2
    ON t2.card_id = t1.card10
LEFT JOIN rawlanding.subscrib_table s
    ON CAST(substr(t1.card10, 1, 9) AS bigint) = s.cardid
WHERE t1.subscriber_id IS NOT NULL
  AND t1.subscriber_id <> 0
  AND t1.stb_model IN ({models})
  AND t1.privacy = '0'
  AND UPPER(t1.connection_type) = 'BROADBAND'
  AND TRY_CAST(t1.callback_date AS date) > date_add('day', -5, current_date)
  AND s.year  = CAST(year(current_date) AS varchar)
  AND s.month = lpad(CAST(month(current_date) AS varchar), 2, '0')
  AND s.day   = lpad(CAST(day(current_date) AS varchar), 2, '0')
  AND to_hex(substr(s.subregions, 9, 1)) {op} '{SAMPLE_ID}'
  AND (
      t2.card_id IS NULL
      OR CAST(t2.last_event_time AS timestamp) < CAST(date_add('day', -14, current_date) AS timestamp)
  )
ORDER BY CAST(t2.last_event_time AS timestamp) DESC
""".strip()


def create_table_sql(name: str, location: str) -> str:
    return f"""
CREATE EXTERNAL TABLE IF NOT EXISTS {DATABASE}.{name} (
  card10 string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  "separatorChar" = ",",
  "quoteChar" = "\\"",
  "escapeChar" = "\\\\"
)
STORED AS TEXTFILE
LOCATION '{location}'
TBLPROPERTIES ("skip.header.line.count"="1")
""".strip()


def callback_sql(name: str, check: dt.date) -> str:
    y, m, d = check.strftime("%Y"), check.strftime("%m"), check.strftime("%d")
    return f"""
SELECT *
FROM (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY card_id
            ORDER BY event_time DESC
        ) AS rn
    FROM "rawlanding"."bigdata_amslogevents"
    WHERE year = '{y}' AND month = '{m}' AND day = '{d}'
      AND card_id IN (SELECT card10 FROM "{DATABASE}"."{name}")
) t
WHERE rn = 1
""".strip()


def missing_sql(name: str, check: dt.date) -> str:
    y, m, d = check.strftime("%Y"), check.strftime("%m"), check.strftime("%d")
    return f"""
SELECT r.*
FROM "{DATABASE}"."{name}" r
WHERE NOT EXISTS (
    SELECT 1
    FROM "rawlanding"."bigdata_amslogevents" e
    WHERE e.card_id = r.card10
      AND e.year = '{y}' AND e.month = '{m}' AND e.day = '{d}'
)
""".strip()


def scala_script() -> str:
    return f"""%%configure -f
{{
    "conf": {{
        "spark.jars": "/usr/lib/hudi/hudi-spark-bundle.jar",
        "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
        "spark.hadoop.hive.metastore.client.factory.class": "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
    }}
}}

import org.apache.spark.sql.functions._

val csvPath = "{CSV_S3}"
val csvDf = spark.read.option("header", "true").csv(csvPath).select(col("card10").as("cardid")).distinct()

val hudiBasePath = "{HUDI_BASE}"
val hudiDf = spark.read.format("hudi").load(hudiBasePath)
val hudiSchema = hudiDf.schema
val baseDf = spark.read.format("hudi").schema(hudiSchema).load(hudiBasePath)

val matchedDf = baseDf.join(broadcast(csvDf), Seq("cardid"), "inner")
val updatedDf = matchedDf
  .withColumn("requestType", lit("addCards"))
  .withColumn("requestStatus", lit("NOT_SENT"))
  .withColumn("requestDate", current_timestamp())

(updatedDf.write
  .format("hudi")
  .option("hoodie.datasource.write.operation", "upsert")
  .option("hoodie.datasource.write.recordkey.field", "cardid")
  .option("hoodie.datasource.write.partitionpath.field", "_hoodie_partition_path")
  .option("hoodie.datasource.write.precombine.field", "requestDate")
  .mode("append")
  .save(hudiBasePath)
)

(spark.read
  .format("hudi")
  .load(hudiBasePath)
  .orderBy(desc("_hoodie_commit_time"))
  .select("cardid", "requesttype", "requeststatus", "_hoodie_commit_time")
  .show(50, false)
)
"""


def run_athena(athena, sql: str, output: str) -> str:
    qid = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": DATABASE},
        ResultConfiguration={"OutputLocation": output},
        WorkGroup=WORKGROUP,
    )["QueryExecutionId"]
    deadline = time.time() + 1800
    while time.time() < deadline:
        status = athena.get_query_execution(QueryExecutionId=qid)["QueryExecution"]["Status"]
        state = status["State"]
        if state == "SUCCEEDED":
            return qid
        if state in {"FAILED", "CANCELLED"}:
            raise Error(f"Athena {qid} {state}: {status.get('StateChangeReason', '')}")
        time.sleep(2)
    raise Error(f"Athena {qid} timed out")


def fetch_rows(athena, qid: str) -> Tuple[List[str], List[List[str]]]:
    headers: List[str] = []
    rows: List[List[str]] = []
    first = True
    for page in athena.get_paginator("get_query_results").paginate(QueryExecutionId=qid):
        for raw in page["ResultSet"]["Rows"]:
            vals = [c.get("VarCharValue", "") for c in raw["Data"]]
            if first:
                headers = vals
                first = False
            else:
                rows.append(vals)
    return headers, rows


def write_csv(path: Path, headers: Sequence[str], rows: Sequence[Sequence[str]]) -> int:
    with path.open("w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(headers)
        w.writerows(rows)
    return len(rows)


def cmd_discover(args: argparse.Namespace) -> int:
    sql = eligible_sql(sample_eq=args.sample_eq)
    op = "=" if args.sample_eq else "<>"
    log(f"Sample filter: sampleid {op} '{SAMPLE_ID}'")

    if args.dry_run:
        print(sql)
        return 0
    if not args.athena_output:
        raise Error("--athena-output is required")

    sess = session(args.profile, args.region)
    athena, s3 = sess.client("athena"), sess.client("s3")

    log("Running Athena query...")
    qid = run_athena(athena, sql, args.athena_output)
    log(f"QueryExecutionId: {qid}")
    headers, rows = fetch_rows(athena, qid)

    local = Path(args.local_csv)
    write_csv(local, headers, rows)
    log(f"Wrote {len(rows)} rows -> {local}")
    if not rows:
        return 0

    s3.upload_file(str(local), BUCKET, CSV_KEY)
    log(f"Uploaded -> {CSV_S3}")

    replay_date = parse_date(args.replay_date) if args.replay_date else today()
    s3.copy_object(
        Bucket=BUCKET,
        CopySource={"Bucket": BUCKET, "Key": CSV_KEY},
        Key=replay_key(replay_date),
    )
    log(f"Copied -> {replay_prefix(replay_date)}cards2replay.csv")
    return 0


def cmd_print_scala(_: argparse.Namespace) -> int:
    log(f"# EMR Studio: Quick Launch, attach role {EMR_ROLE}, Spark | Idle, then run:")
    print(scala_script())
    return 0


def cmd_register_table(args: argparse.Namespace) -> int:
    replay_date = parse_date(args.replay_date) if args.replay_date else today()
    name = args.table_name or table_name(replay_date)
    location = replay_prefix(replay_date)
    sql = create_table_sql(name, location)

    log(f"Table: {DATABASE}.{name}")
    log(f"Location: {location}")
    if args.dry_run:
        print(sql)
        return 0
    if not args.athena_output:
        raise Error("--athena-output is required")

    sess = session(args.profile, args.region)
    athena, s3 = sess.client("athena"), sess.client("s3")
    s3.copy_object(
        Bucket=BUCKET,
        CopySource={"Bucket": BUCKET, "Key": CSV_KEY},
        Key=replay_key(replay_date),
    )
    qid = run_athena(athena, sql, args.athena_output)
    log(f"Created table ({qid})")
    return 0


def cmd_check(args: argparse.Namespace) -> int:
    check = parse_date(args.check_date)
    replay_date = parse_date(args.replay_date) if args.replay_date else (check - dt.timedelta(days=1))
    name = args.table_name or table_name(replay_date)

    print("-- cards that started callback")
    print(callback_sql(name, check))
    print()
    print("-- cards still missing callback")
    print(missing_sql(name, check))
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="BB card AMS replay helper")
    sub = p.add_subparsers(dest="command", required=True)

    d = sub.add_parser("discover", help="Athena query -> cards2replay.csv -> S3 (+ replay/ copy)")
    d.add_argument("--profile")
    d.add_argument("--region", default=REGION)
    d.add_argument("--athena-output", help="s3://... Athena results location")
    d.add_argument("--local-csv", default=LOCAL_CSV)
    d.add_argument("--replay-date", help="YYYY-MM-DD for replay/ folder (default: today UTC)")
    d.add_argument("--sample-eq", action="store_true", help="Use sampleid = '05' instead of <>")
    d.add_argument("--dry-run", action="store_true")
    d.set_defaults(func=cmd_discover)

    s = sub.add_parser("print-scala", help="Print EMR Studio HUDI upsert Scala")
    s.set_defaults(func=cmd_print_scala)

    r = sub.add_parser("register-table", help="Copy CSV to replay/ and CREATE EXTERNAL TABLE")
    r.add_argument("--profile")
    r.add_argument("--region", default=REGION)
    r.add_argument("--athena-output")
    r.add_argument("--replay-date", help="YYYY-MM-DD (default: today UTC)")
    r.add_argument("--table-name")
    r.add_argument("--dry-run", action="store_true")
    r.set_defaults(func=cmd_register_table)

    c = sub.add_parser("check", help="Print day-after callback / missing SQLs")
    c.add_argument("--check-date", required=True, help="AMS log partition YYYY-MM-DD")
    c.add_argument("--replay-date", help="Replay table date (default: check-date - 1 day)")
    c.add_argument("--table-name")
    c.set_defaults(func=cmd_check)

    return p


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return int(args.func(args))
    except (Error, BotoCoreError, ClientError, ValueError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
