#!/usr/bin/env python3
"""
Self-contained BB card replay orchestration script.

Automates (or prepares) the AMS broadband card replay flow:
  1) Athena: find eligible cards not calling back AMS
  2) Export cards2replay.csv and upload to self-service S3 (*1641)
  3) HUDI upsert via EMR Studio Scala (or print script for Workspace)
  4) Optionally start Step Function to send requests immediately
  5) Copy CSV to dated /replay folder and create Athena table
  6/7) Day-after callback / still-missing verification queries

Usage examples:
  # Dry-run discovery (sample != 05 by default)
  python3 replay_bb_cards.py discover --dry-run

  # Full prep: Athena -> local CSV -> S3 upload -> copy to replay/ -> create table
  python3 replay_bb_cards.py discover \\
      --profile sms-self-service \\
      --region us-east-1 \\
      --athena-output s3://dtv-prod-bigdatadl-330572541641-sms/tmp/athena-results/

  # Print EMR Studio Scala for HUDI upsert
  python3 replay_bb_cards.py print-scala

  # Start Step Function immediately after HUDI upsert
  python3 replay_bb_cards.py trigger-sfn --profile sms-self-service

  # Register Athena external table for today's replay folder
  python3 replay_bb_cards.py register-table --profile sms-self-service \\
      --athena-output s3://dtv-prod-bigdatadl-330572541641-sms/tmp/athena-results/

  # Day-after checks (default: yesterday partition vs today's table name)
  python3 replay_bb_cards.py check-callbacks --check-date 2026-05-01
  python3 replay_bb_cards.py check-missing  --check-date 2026-05-01

  # End-to-end prep (does NOT run EMR Studio notebook interactively)
  python3 replay_bb_cards.py run-prep --profile sms-self-service \\
      --athena-output s3://dtv-prod-bigdatadl-330572541641-sms/tmp/athena-results/
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import os
import sys
import time
import uuid
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

try:
    import boto3
    from botocore.exceptions import BotoCoreError, ClientError
except ImportError:  # pragma: no cover
    boto3 = None
    BotoCoreError = ClientError = Exception  # type: ignore


# ---------------------------------------------------------------------------
# Defaults (prod / self-service *1641)
# ---------------------------------------------------------------------------
DEFAULT_REGION = "us-east-1"
DEFAULT_S3_BUCKET = "dtv-prod-bigdatadl-330572541641-sms"
DEFAULT_CSV_KEY = "tmp/cards2replay.csv"
DEFAULT_CSV_S3 = f"s3://{DEFAULT_S3_BUCKET}/{DEFAULT_CSV_KEY}"
DEFAULT_REPLAY_PREFIX = "replay"
DEFAULT_HUDI_BASE = "s3://aeg-prod-bigdatadl-integration-sms/broadband/bb-card-status/"
DEFAULT_SFN_NAME = "bigdata-godfathers-sms-broadbanddelta-prod"
DEFAULT_SFN_PAYLOAD = {
    "TerminateCluster": True,
    "CreateCluster": True,
    "ConfigSubDir": "",
}
DEFAULT_EMR_RUNTIME_ROLE = "sms-card-enrichment-serverless-emr-runtime-role-prod"
DEFAULT_ATHENA_DATABASE = "integration"
DEFAULT_ATHENA_WORKGROUP = "primary"
DEFAULT_SAMPLE_FILTER = "<>"  # cards NOT in sample 05
DEFAULT_SAMPLE_ID = "05"
DEFAULT_LOCAL_CSV = "cards2replay.csv"

STB_MODELS = (
    "C31-700",
    "C41-100",
    "C41-500",
    "C41-700",
    "C41W-100",
    "C41W-500",
    "C51-100",
    "C61K-700",
    "D10",
    "D10-100",
    "D10-200",
    "D10-300",
    "D11",
    "D11-100",
    "D11-300",
    "D11-500",
    "D11-800",
    "D11I-100",
    "D12-100",
    "D12-300",
    "D12-500",
    "D12-700",
    "H20",
    "H20-100",
    "H20-600",
    "H21-100",
    "H21-200",
    "H23-600",
    "H24-100",
    "H24-200",
    "H24-700",
    "H25-100",
    "H25-500",
    "H25-700",
    "H44-100",
    "H44-500",
    "HR20-100",
    "HR20-700",
    "HR20I-100",
    "HR21-100",
    "HR21-200",
    "HR21-700",
    "HR21P-200",
    "HR22-100",
    "HR23-700",
    "HR24-100",
    "HR24-200",
    "HR24-500",
    "HR34-700",
    "HR44-200",
    "HR44-500",
    "HR44-700",
    "HR54-200",
    "HR54-500",
    "HR54-700",
    "R15",
    "R15-100",
    "R15-300",
    "R15-500",
    "R15-700",
    "R16-100",
    "R16-300",
    "R16-500",
    "R16-700",
    "R22-100",
    "R22-200",
    "HR54R1-700",
    "HR54R1-500",
    "HS17-500",
    "HS17-100",
)

SCRIPT_DIR = Path(__file__).resolve().parent
SCALA_PATH = SCRIPT_DIR / "hudi_bb_card_upsert.scala"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
class ReplayError(RuntimeError):
    pass


def log(msg: str) -> None:
    print(msg, flush=True)


def require_boto3() -> None:
    if boto3 is None:
        raise ReplayError(
            "boto3 is required. Install with: pip install -r requirements.txt"
        )


def session_from_args(args: argparse.Namespace):
    require_boto3()
    kwargs: Dict[str, Any] = {}
    if getattr(args, "profile", None):
        kwargs["profile_name"] = args.profile
    if getattr(args, "region", None):
        kwargs["region_name"] = args.region
    return boto3.Session(**kwargs)


def client(session, service: str):
    return session.client(service)


def today(tz: Optional[dt.tzinfo] = None) -> dt.date:
    return dt.datetime.now(tz or dt.timezone.utc).date()


def parse_date(value: str) -> dt.date:
    return dt.date.fromisoformat(value)


def table_name_for_date(d: dt.date) -> str:
    # Matches example ams_replay_043026 (MMDDYY)
    return f"ams_replay_{d.strftime('%m%d%y')}"


def replay_s3_prefix_for_date(d: dt.date, bucket: str = DEFAULT_S3_BUCKET) -> str:
    # Targeted to folder, not file: s3://.../replay/YYYYMMDD/
    return f"s3://{bucket}/{DEFAULT_REPLAY_PREFIX}/{d.strftime('%Y%m%d')}/"


def replay_s3_key_for_date(d: dt.date) -> str:
    return f"{DEFAULT_REPLAY_PREFIX}/{d.strftime('%Y%m%d')}/cards2replay.csv"


def stb_model_sql_list() -> str:
    return ",".join(f"'{m}'" for m in STB_MODELS)


def build_eligible_cards_sql(sample_op: str = DEFAULT_SAMPLE_FILTER, sample_id: str = DEFAULT_SAMPLE_ID) -> str:
    """
    Eligible BB cards that are not getting AMS callbacks.

    sample_op:
      '<>'  -> cards NOT in sample (default; not in sample 05)
      '='   -> cards IN sample (original runbook sample 05 filter)
    """
    if sample_op not in {"=", "<>"}:
        raise ReplayError("sample_op must be '=' or '<>'")
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
  AND t1.stb_model IN ({stb_model_sql_list()})
  AND t1.privacy = '0'
  AND UPPER(t1.connection_type) = 'BROADBAND'
  AND TRY_CAST(t1.callback_date AS date) > date_add('day', -5, current_date)
  AND s.year  = CAST(year(current_date) AS varchar)
  AND s.month = lpad(CAST(month(current_date) AS varchar), 2, '0')
  AND s.day   = lpad(CAST(day(current_date) AS varchar), 2, '0')
  AND to_hex(substr(s.subregions, 9, 1)) {sample_op} '{sample_id}'
  AND (
      t2.card_id IS NULL
      OR CAST(t2.last_event_time AS timestamp) < CAST(date_add('day', -14, current_date) AS timestamp)
  )
ORDER BY CAST(t2.last_event_time AS timestamp) DESC
""".strip()


def build_create_table_sql(
    table_name: str,
    location: str,
    database: str = DEFAULT_ATHENA_DATABASE,
) -> str:
    return f"""
CREATE EXTERNAL TABLE IF NOT EXISTS {database}.{table_name} (
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


def build_callback_check_sql(
    table_name: str,
    check_date: dt.date,
    database: str = DEFAULT_ATHENA_DATABASE,
) -> str:
    y, m, d = check_date.strftime("%Y"), check_date.strftime("%m"), check_date.strftime("%d")
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
    WHERE year = '{y}'
      AND month = '{m}'
      AND day = '{d}'
      AND card_id IN (SELECT card10 FROM "{database}"."{table_name}")
) t
WHERE rn = 1
""".strip()


def build_missing_callback_sql(
    table_name: str,
    check_date: dt.date,
    database: str = DEFAULT_ATHENA_DATABASE,
) -> str:
    y, m, d = check_date.strftime("%Y"), check_date.strftime("%m"), check_date.strftime("%d")
    return f"""
SELECT r.*
FROM "{database}"."{table_name}" r
WHERE NOT EXISTS (
    SELECT 1
    FROM "rawlanding"."bigdata_amslogevents" e
    WHERE e.card_id = r.card10
      AND e.year = '{y}'
      AND e.month = '{m}'
      AND e.day = '{d}'
)
""".strip()


def scala_script(csv_s3: str = DEFAULT_CSV_S3, hudi_base: str = DEFAULT_HUDI_BASE) -> str:
    return f"""%%configure -f
{{
    "conf": {{
        "spark.jars": "/usr/lib/hudi/hudi-spark-bundle.jar",
        "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
        "spark.hadoop.hive.metastore.client.factory.class": "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
    }}
}}

import org.apache.spark.sql.functions._

val csvPath = "{csv_s3}"
val csvDf = spark.read.option("header", "true").csv(csvPath).select(col("card10").as("cardid")).distinct()

val hudiBasePath = "{hudi_base}"
val hudiDf = spark.read.format("hudi").load(hudiBasePath)
val hudiSchema = hudiDf.schema
val baseDf = spark.read.format("hudi").schema(hudiSchema).load(hudiBasePath)

val matchedDf = baseDf.join(broadcast(csvDf), Seq("cardid"), "inner")
val updatedDf = matchedDf
  .withColumn("requestType", lit("addCards"))
  .withColumn("requestStatus", lit("NOT_SENT"))
  .withColumn("requestDate", current_timestamp())

println(s"Cards matched for upsert: ${{matchedDf.count()}}")

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


# ---------------------------------------------------------------------------
# Athena / S3 / SFN
# ---------------------------------------------------------------------------
def start_athena_query(
    athena,
    sql: str,
    database: str,
    output: str,
    workgroup: str,
) -> str:
    resp = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": database},
        ResultConfiguration={"OutputLocation": output},
        WorkGroup=workgroup,
    )
    return resp["QueryExecutionId"]


def wait_athena(athena, qid: str, poll_seconds: float = 2.0, timeout_seconds: int = 1800) -> Dict[str, Any]:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        resp = athena.get_query_execution(QueryExecutionId=qid)
        status = resp["QueryExecution"]["Status"]
        state = status["State"]
        if state in {"SUCCEEDED", "FAILED", "CANCELLED"}:
            if state != "SUCCEEDED":
                reason = status.get("StateChangeReason", "unknown")
                raise ReplayError(f"Athena query {qid} ended with {state}: {reason}")
            return resp
        time.sleep(poll_seconds)
    raise ReplayError(f"Athena query {qid} timed out after {timeout_seconds}s")


def fetch_athena_rows(athena, qid: str) -> Tuple[List[str], List[List[str]]]:
    """Return (headers, rows) for the query result set."""
    paginator = athena.get_paginator("get_query_results")
    headers: List[str] = []
    rows: List[List[str]] = []
    first = True
    for page in paginator.paginate(QueryExecutionId=qid):
        for raw in page["ResultSet"]["Rows"]:
            vals = [c.get("VarCharValue", "") for c in raw["Data"]]
            if first:
                headers = vals
                first = False
                continue
            rows.append(vals)
    return headers, rows


def write_csv(path: Path, headers: Sequence[str], rows: Iterable[Sequence[str]]) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(headers)
        for row in rows:
            writer.writerow(row)
            count += 1
    return count


def upload_file(s3, local_path: Path, bucket: str, key: str) -> str:
    s3.upload_file(str(local_path), bucket, key)
    return f"s3://{bucket}/{key}"


def copy_s3_object(s3, bucket: str, src_key: str, dst_key: str) -> str:
    s3.copy_object(
        Bucket=bucket,
        CopySource={"Bucket": bucket, "Key": src_key},
        Key=dst_key,
    )
    return f"s3://{bucket}/{dst_key}"


def resolve_sfn_arn(sfn, name: str) -> str:
    if name.startswith("arn:"):
        return name
    paginator = sfn.get_paginator("list_state_machines")
    for page in paginator.paginate():
        for sm in page.get("stateMachines", []):
            if sm["name"] == name:
                return sm["stateMachineArn"]
    raise ReplayError(f"Step Function not found: {name}")


def start_step_function(sfn, name: str, payload: Dict[str, Any], execution_name: Optional[str] = None) -> str:
    arn = resolve_sfn_arn(sfn, name)
    exec_name = execution_name or f"bb-card-replay-{dt.datetime.utcnow().strftime('%Y%m%d%H%M%S')}-{uuid.uuid4().hex[:8]}"
    resp = sfn.start_execution(
        stateMachineArn=arn,
        name=exec_name,
        input=json.dumps(payload),
    )
    return resp["executionArn"]


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------
def cmd_print_sql(args: argparse.Namespace) -> int:
    sample_op = "=" if args.sample_eq else "<>"
    print(build_eligible_cards_sql(sample_op=sample_op, sample_id=args.sample_id))
    return 0


def cmd_print_scala(args: argparse.Namespace) -> int:
    print(scala_script(csv_s3=args.csv_s3, hudi_base=args.hudi_base))
    if SCALA_PATH.exists():
        log(f"\n# Also available on disk: {SCALA_PATH}")
    log("\n# EMR Studio checklist:")
    log("#  1) Open Workspaces -> your workspace -> Quick Launch")
    log(f"#  2) Attach EMR Serverless app with role {DEFAULT_EMR_RUNTIME_ROLE}")
    log("#  3) Select Spark kernel, wait for Spark | Idle")
    log("#  4) Paste/run the configure cell, then the upsert cells")
    return 0


def cmd_discover(args: argparse.Namespace) -> int:
    sample_op = "=" if args.sample_eq else "<>"
    sql = build_eligible_cards_sql(sample_op=sample_op, sample_id=args.sample_id)
    local_csv = Path(args.local_csv).resolve()
    bucket = args.bucket
    csv_key = args.csv_key

    log("=== Step 1: Discover eligible cards (Athena) ===")
    log(f"Sample filter: to_hex(substr(s.subregions, 9, 1)) {sample_op} '{args.sample_id}'")
    if args.dry_run:
        log("--- SQL (dry-run) ---")
        print(sql)
        log(f"Would write local CSV: {local_csv}")
        log(f"Would upload to: s3://{bucket}/{csv_key}")
        return 0

    if not args.athena_output:
        raise ReplayError("--athena-output is required (s3://... Athena results location)")

    session = session_from_args(args)
    athena = client(session, "athena")
    s3 = client(session, "s3")

    log("Starting Athena query...")
    qid = start_athena_query(
        athena,
        sql=sql,
        database=args.database,
        output=args.athena_output,
        workgroup=args.workgroup,
    )
    log(f"QueryExecutionId: {qid}")
    wait_athena(athena, qid)
    headers, rows = fetch_athena_rows(athena, qid)
    count = write_csv(local_csv, headers, rows)
    log(f"Wrote {count} rows -> {local_csv}")

    if count == 0:
        log("WARNING: zero eligible cards; skipping S3 upload.")
        return 0

    uri = upload_file(s3, local_csv, bucket, csv_key)
    log(f"Uploaded -> {uri}")

    if args.copy_to_replay:
        replay_date = parse_date(args.replay_date) if args.replay_date else today()
        dst_key = replay_s3_key_for_date(replay_date)
        replay_uri = copy_s3_object(s3, bucket, csv_key, dst_key)
        log(f"Copied to replay folder -> {replay_uri}")
        log(f"Replay LOCATION for Athena table: {replay_s3_prefix_for_date(replay_date, bucket)}")

    return 0


def cmd_register_table(args: argparse.Namespace) -> int:
    replay_date = parse_date(args.replay_date) if args.replay_date else today()
    table = args.table_name or table_name_for_date(replay_date)
    location = args.location or replay_s3_prefix_for_date(replay_date, args.bucket)
    sql = build_create_table_sql(table, location, database=args.database)

    log("=== Step 5: Register Athena replay table ===")
    log(f"Table: {args.database}.{table}")
    log(f"Location: {location}")

    if args.dry_run:
        print(sql)
        return 0

    if not args.athena_output:
        raise ReplayError("--athena-output is required")

    session = session_from_args(args)
    athena = client(session, "athena")
    s3 = client(session, "s3")

    # Ensure CSV is present under the folder LOCATION (folder, not bare file)
    if args.ensure_copy:
        src_key = args.csv_key
        dst_key = replay_s3_key_for_date(replay_date)
        log(f"Copying s3://{args.bucket}/{src_key} -> s3://{args.bucket}/{dst_key}")
        copy_s3_object(s3, args.bucket, src_key, dst_key)

    qid = start_athena_query(
        athena,
        sql=sql,
        database=args.database,
        output=args.athena_output,
        workgroup=args.workgroup,
    )
    log(f"QueryExecutionId: {qid}")
    wait_athena(athena, qid)
    log("CREATE EXTERNAL TABLE succeeded.")
    return 0


def cmd_trigger_sfn(args: argparse.Namespace) -> int:
    payload = dict(DEFAULT_SFN_PAYLOAD)
    if args.payload_json:
        payload.update(json.loads(args.payload_json))

    log("=== Step 4: Start Step Function (immediate send) ===")
    log(f"State machine: {args.sfn_name}")
    log(f"Payload: {json.dumps(payload)}")

    if args.dry_run:
        return 0

    session = session_from_args(args)
    sfn = client(session, "stepfunctions")
    arn = start_step_function(sfn, args.sfn_name, payload, execution_name=args.execution_name)
    log(f"Started execution: {arn}")
    return 0


def cmd_check_callbacks(args: argparse.Namespace) -> int:
    check_date = parse_date(args.check_date)
    # Replay table is typically from the day before the check partition
    replay_date = parse_date(args.replay_date) if args.replay_date else (check_date - dt.timedelta(days=1))
    table = args.table_name or table_name_for_date(replay_date)
    sql = build_callback_check_sql(table, check_date, database=args.database)

    log("=== Step 6: Cards that started callback after replay ===")
    log(f"Replay table: {args.database}.{table}")
    log(f"AMS log partition: {check_date.isoformat()}")

    if args.dry_run or args.print_only:
        print(sql)
        if args.print_only or args.dry_run:
            return 0

    if not args.athena_output:
        raise ReplayError("--athena-output is required unless --print-only/--dry-run")

    session = session_from_args(args)
    athena = client(session, "athena")
    qid = start_athena_query(
        athena,
        sql=sql,
        database=args.database,
        output=args.athena_output,
        workgroup=args.workgroup,
    )
    log(f"QueryExecutionId: {qid}")
    wait_athena(athena, qid)
    headers, rows = fetch_athena_rows(athena, qid)
    log(f"Rows: {len(rows)}")
    if args.local_csv:
        write_csv(Path(args.local_csv), headers, rows)
        log(f"Wrote {args.local_csv}")
    else:
        # Print a compact preview
        print(",".join(headers))
        for row in rows[:50]:
            print(",".join(row))
        if len(rows) > 50:
            log(f"... truncated, showing 50/{len(rows)}")
    return 0


def cmd_check_missing(args: argparse.Namespace) -> int:
    check_date = parse_date(args.check_date)
    replay_date = parse_date(args.replay_date) if args.replay_date else (check_date - dt.timedelta(days=1))
    table = args.table_name or table_name_for_date(replay_date)
    sql = build_missing_callback_sql(table, check_date, database=args.database)

    log("=== Step 7: Cards still not calling back after replay ===")
    log(f"Replay table: {args.database}.{table}")
    log(f"AMS log partition: {check_date.isoformat()}")

    if args.dry_run or args.print_only:
        print(sql)
        if args.print_only or args.dry_run:
            return 0

    if not args.athena_output:
        raise ReplayError("--athena-output is required unless --print-only/--dry-run")

    session = session_from_args(args)
    athena = client(session, "athena")
    qid = start_athena_query(
        athena,
        sql=sql,
        database=args.database,
        output=args.athena_output,
        workgroup=args.workgroup,
    )
    log(f"QueryExecutionId: {qid}")
    wait_athena(athena, qid)
    headers, rows = fetch_athena_rows(athena, qid)
    log(f"Rows still missing callback: {len(rows)}")
    if args.local_csv:
        write_csv(Path(args.local_csv), headers, rows)
        log(f"Wrote {args.local_csv}")
    else:
        print(",".join(headers))
        for row in rows[:50]:
            print(",".join(row))
        if len(rows) > 50:
            log(f"... truncated, showing 50/{len(rows)}")
    return 0


def cmd_emr_instructions(args: argparse.Namespace) -> int:
    log("=== Step 2/3: EMR Studio Workspace + HUDI upsert ===")
    log("This step is interactive in EMR Studio. Automation prepares the Scala for you.")
    log("")
    log("Manual steps:")
    log("  1. Connect to AWS EMR Studio (self-service / *1641 as needed).")
    log("  2. Open Workspaces -> pick yours -> Quick Launch.")
    log(f"  3. For Workbook application, attach EMR Serverless with role:")
    log(f"       {DEFAULT_EMR_RUNTIME_ROLE}")
    log("  4. Select Spark kernel and start; wait until status is Spark | Idle.")
    log("  5. Run the Scala below (also: python3 replay_bb_cards.py print-scala).")
    log("")
    print(scala_script(csv_s3=args.csv_s3, hudi_base=args.hudi_base))
    return 0


def cmd_run_prep(args: argparse.Namespace) -> int:
    """
    Automated prep path:
      discover (+ upload) -> copy to replay/ -> register Athena table
      then print EMR Scala + optional SFN trigger.
    """
    log("=== run-prep: discover + S3 + Athena table (+ optional SFN) ===")
    rc = cmd_discover(args)
    if rc != 0:
        return rc

    # Force register with ensure_copy after discover
    args.ensure_copy = True
    rc = cmd_register_table(args)
    if rc != 0:
        return rc

    log("")
    cmd_emr_instructions(args)

    if args.trigger_sfn:
        log("")
        log("NOTE: Triggering Step Function now. Prefer waiting until HUDI upsert finishes.")
        rc = cmd_trigger_sfn(args)
        if rc != 0:
            return rc
    else:
        log("")
        log("After HUDI upsert completes, optionally send immediately:")
        log(
            f"  python3 {Path(__file__).name} trigger-sfn"
            + (f" --profile {args.profile}" if args.profile else "")
        )
    return 0


def cmd_runbook(args: argparse.Namespace) -> int:
    replay_date = parse_date(args.replay_date) if args.replay_date else today()
    check_date = parse_date(args.check_date) if args.check_date else (replay_date + dt.timedelta(days=1))
    table = table_name_for_date(replay_date)
    location = replay_s3_prefix_for_date(replay_date)

    print(
        f"""
BB Card Replay Runbook (self-contained helper)
=============================================

Defaults
  CSV S3:        {DEFAULT_CSV_S3}
  HUDI path:     {DEFAULT_HUDI_BASE}
  EMR role:      {DEFAULT_EMR_RUNTIME_ROLE}
  Step Function: {DEFAULT_SFN_NAME}
  Sample filter: sampleid <> '05'  (cards NOT in sample 5)
  Replay table:  integration.{table}
  Replay loc:    {location}
  Check date:    {check_date.isoformat()}

1) Discover + export + upload
   python3 replay_bb_cards.py discover \\
     --profile <aws-profile-1641> --region {DEFAULT_REGION} \\
     --athena-output s3://{DEFAULT_S3_BUCKET}/tmp/athena-results/ \\
     --copy-to-replay

2/3) EMR Studio Workspace -> Spark Idle -> run HUDI upsert
   python3 replay_bb_cards.py print-scala
   # paste into notebook (configure cell first)

4) Optional: send immediately via Step Function
   python3 replay_bb_cards.py trigger-sfn --profile <aws-profile-1641>

5) Register Athena table (folder LOCATION)
   python3 replay_bb_cards.py register-table \\
     --profile <aws-profile-1641> \\
     --athena-output s3://{DEFAULT_S3_BUCKET}/tmp/athena-results/ \\
     --ensure-copy

6) Day after: cards that started callback
   python3 replay_bb_cards.py check-callbacks --check-date {check_date.isoformat()} --print-only

7) Day after: cards still missing callback
   python3 replay_bb_cards.py check-missing --check-date {check_date.isoformat()} --print-only

One-shot prep (steps 1+5, then prints Scala for 2/3):
   python3 replay_bb_cards.py run-prep \\
     --profile <aws-profile-1641> \\
     --athena-output s3://{DEFAULT_S3_BUCKET}/tmp/athena-results/
""".strip()
    )
    return 0


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def add_aws_args(p: argparse.ArgumentParser) -> None:
    p.add_argument("--profile", help="AWS named profile (self-service *1641)")
    p.add_argument("--region", default=DEFAULT_REGION, help=f"AWS region (default: {DEFAULT_REGION})")
    p.add_argument("--dry-run", action="store_true", help="Print actions/SQL without calling AWS")


def add_athena_args(p: argparse.ArgumentParser) -> None:
    p.add_argument("--database", default=DEFAULT_ATHENA_DATABASE)
    p.add_argument("--workgroup", default=DEFAULT_ATHENA_WORKGROUP)
    p.add_argument(
        "--athena-output",
        help="s3:// path for Athena query results (required unless dry-run/print-only)",
    )


def add_s3_args(p: argparse.ArgumentParser) -> None:
    p.add_argument("--bucket", default=DEFAULT_S3_BUCKET)
    p.add_argument("--csv-key", default=DEFAULT_CSV_KEY)
    p.add_argument("--csv-s3", default=DEFAULT_CSV_S3)
    p.add_argument("--hudi-base", default=DEFAULT_HUDI_BASE)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Self-contained BB card replay via Athena + HUDI upsert helpers",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("runbook", help="Print end-to-end runbook with today's dates")
    p.add_argument("--replay-date", help="YYYY-MM-DD for replay folder/table (default: today UTC)")
    p.add_argument("--check-date", help="YYYY-MM-DD AMS log partition for day-after checks")
    p.set_defaults(func=cmd_runbook)

    p = sub.add_parser("print-sql", help="Print eligible-cards Athena SQL")
    p.add_argument("--sample-id", default=DEFAULT_SAMPLE_ID)
    p.add_argument(
        "--sample-eq",
        action="store_true",
        help="Use sampleid = '05' (original). Default is sampleid <> '05'.",
    )
    p.set_defaults(func=cmd_print_sql)

    p = sub.add_parser("print-scala", help="Print EMR Studio Spark/Scala HUDI upsert script")
    add_s3_args(p)
    p.set_defaults(func=cmd_print_scala)

    p = sub.add_parser("emr-instructions", help="Print EMR Studio steps + Scala")
    add_s3_args(p)
    p.set_defaults(func=cmd_emr_instructions)

    p = sub.add_parser("discover", help="Run Athena query, save CSV, upload to S3")
    add_aws_args(p)
    add_athena_args(p)
    add_s3_args(p)
    p.add_argument("--local-csv", default=DEFAULT_LOCAL_CSV)
    p.add_argument("--sample-id", default=DEFAULT_SAMPLE_ID)
    p.add_argument(
        "--sample-eq",
        action="store_true",
        help="Use sampleid = '05'. Default is sampleid <> '05' (not in sample 5).",
    )
    p.add_argument(
        "--copy-to-replay",
        action="store_true",
        help="Also copy CSV under s3://.../replay/YYYYMMDD/",
    )
    p.add_argument("--replay-date", help="YYYY-MM-DD for replay folder (default: today UTC)")
    p.set_defaults(func=cmd_discover)

    p = sub.add_parser("register-table", help="CREATE EXTERNAL TABLE for replayed cards folder")
    add_aws_args(p)
    add_athena_args(p)
    add_s3_args(p)
    p.add_argument("--replay-date", help="YYYY-MM-DD (default: today UTC)")
    p.add_argument("--table-name", help="Override table name (default: ams_replay_MMDDYY)")
    p.add_argument("--location", help="Override S3 LOCATION folder (must end with /)")
    p.add_argument(
        "--ensure-copy",
        action="store_true",
        help="Copy tmp/cards2replay.csv into the replay/YYYYMMDD/ folder first",
    )
    p.set_defaults(func=cmd_register_table)

    p = sub.add_parser("trigger-sfn", help="Start broadbanddelta Step Function")
    add_aws_args(p)
    p.add_argument("--sfn-name", default=DEFAULT_SFN_NAME)
    p.add_argument(
        "--payload-json",
        help='JSON object merged into default payload, e.g. \'{"ConfigSubDir":"x"}\'',
    )
    p.add_argument("--execution-name", help="Optional Step Functions execution name")
    p.set_defaults(func=cmd_trigger_sfn)

    p = sub.add_parser("check-callbacks", help="Day-after: cards that started AMS callback")
    add_aws_args(p)
    add_athena_args(p)
    p.add_argument("--check-date", required=True, help="AMS log partition YYYY-MM-DD")
    p.add_argument("--replay-date", help="Replay table date YYYY-MM-DD (default: check-date - 1 day)")
    p.add_argument("--table-name", help="Override ams_replay_* table name")
    p.add_argument("--local-csv", help="Optional path to save results")
    p.add_argument("--print-only", action="store_true", help="Only print SQL")
    p.set_defaults(func=cmd_check_callbacks)

    p = sub.add_parser("check-missing", help="Day-after: cards still without AMS callback")
    add_aws_args(p)
    add_athena_args(p)
    p.add_argument("--check-date", required=True, help="AMS log partition YYYY-MM-DD")
    p.add_argument("--replay-date", help="Replay table date YYYY-MM-DD (default: check-date - 1 day)")
    p.add_argument("--table-name", help="Override ams_replay_* table name")
    p.add_argument("--local-csv", help="Optional path to save results")
    p.add_argument("--print-only", action="store_true", help="Only print SQL")
    p.set_defaults(func=cmd_check_missing)

    p = sub.add_parser(
        "run-prep",
        help="Discover+upload+register table, then print EMR Scala (optional --trigger-sfn)",
    )
    add_aws_args(p)
    add_athena_args(p)
    add_s3_args(p)
    p.add_argument("--local-csv", default=DEFAULT_LOCAL_CSV)
    p.add_argument("--sample-id", default=DEFAULT_SAMPLE_ID)
    p.add_argument("--sample-eq", action="store_true")
    p.add_argument("--copy-to-replay", action="store_true", default=True)
    p.add_argument("--replay-date", help="YYYY-MM-DD (default: today UTC)")
    p.add_argument("--table-name")
    p.add_argument("--location")
    p.add_argument("--ensure-copy", action="store_true", default=True)
    p.add_argument("--sfn-name", default=DEFAULT_SFN_NAME)
    p.add_argument("--payload-json")
    p.add_argument("--execution-name")
    p.add_argument(
        "--trigger-sfn",
        action="store_true",
        help="Also start Step Function (only after HUDI upsert is done)",
    )
    p.set_defaults(func=cmd_run_prep)

    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return int(args.func(args))
    except (ReplayError, BotoCoreError, ClientError, ValueError, json.JSONDecodeError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
