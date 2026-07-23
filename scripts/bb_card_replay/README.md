# BB Card AMS Replay

Self-contained helper to prepare broadband cards for AMS replay via HUDI upsert.

There are **two prep paths**. Both upload `cards2replay.csv` to S3, optionally create an Athena replay table, then print the EMR Studio Scala for the HUDI update.

| Path | Command | Card source |
|------|---------|-------------|
| 1 | `from-query` | Athena discovery query (impacted cards) |
| 2 | `from-list` | Static list (`--cards` and/or `--cards-file`) |

## Setup

```bash
cd scripts/bb_card_replay
pip install -r requirements.txt
```

Use an AWS profile that can access the self-service account (`*1641`) for Athena/S3.

## Path 1 — discover via Athena query

Finds eligible BB cards not calling back AMS (default: `sampleid <> '05'`), writes CSV, uploads to S3, creates the Athena table, and prints HUDI Scala.

```bash
python3 replay_bb_cards.py from-query \
  --profile <aws-profile-1641> \
  --athena-output s3://dtv-prod-bigdatadl-330572541641-sms/tmp/athena-results/
```

Useful flags:

- `--sample-eq` — use `sampleid = '05'` instead of `<>`
- `--dry-run` — print SQL only
- `--skip-table` — skip Athena `CREATE EXTERNAL TABLE`
- `--skip-scala` — do not print HUDI Scala at the end
- `--replay-date YYYY-MM-DD` — set replay folder/table date (default: today UTC)

## Path 2 — static card list

Skips the discovery query. Builds `cards2replay.csv` from the cards you provide, then same upload / table / HUDI steps.

```bash
# comma-separated
python3 replay_bb_cards.py from-list \
  --cards 0123456789,0987654321 \
  --profile <aws-profile-1641> \
  --athena-output s3://dtv-prod-bigdatadl-330572541641-sms/tmp/athena-results/

# from file (one card per line, or CSV with card10 / cardid / card_id header)
python3 replay_bb_cards.py from-list \
  --cards-file my_cards.txt \
  --profile <aws-profile-1641> \
  --athena-output s3://dtv-prod-bigdatadl-330572541641-sms/tmp/athena-results/
```

`--cards` and `--cards-file` can be combined. Duplicates are removed.

Dry-run (writes local CSV, no S3/Athena):

```bash
python3 replay_bb_cards.py from-list --cards-file my_cards.txt --dry-run
```

## HUDI update (EMR Studio)

After either path finishes, run the printed Scala in EMR Studio:

1. Open Workspaces → your workspace → **Quick Launch**
2. Attach EMR Serverless app with role: `sms-card-enrichment-serverless-emr-runtime-role-prod`
3. Select **Spark** kernel and wait for **Spark \| Idle**
4. Paste/run the configure cell, then the upsert cells

The Scala job prints:

- **HUDI BEFORE** — matched cards’ `requesttype` / `requeststatus` / `requestdate` / commit time
- **HUDI AFTER** — same view after upsert (expect `addCards` / `NOT_SENT`)
- **`[COUNTER]` lines** — change counts (see below)

Or reprint the script anytime:

```bash
python3 replay_bb_cards.py print-scala
```

Companion file: `hudi_bb_card_upsert.scala`

## Counters

Python prep (`from-query` / `from-list`) logs:

| Counter | Meaning |
|---------|---------|
| `cards_in` | Distinct cards from query or static list |
| `cards_uploaded` | Cards written to S3 CSV |
| `athena_table_created` | Whether replay table was created |

Scala HUDI job logs:

| Counter | Meaning |
|---------|---------|
| `csv_distinct_cards` | Cards read from S3 CSV |
| `hudi_matched_before` | Cards found in HUDI before upsert |
| `cards_not_in_hudi` | CSV cards with no HUDI row (not updated) |
| `rows_to_upsert` | Rows written in the upsert |
| `hudi_matched_after` | Cards found in HUDI after upsert |
| `hudi_ready_addCards_NOT_SENT` | Cards now set to `addCards` / `NOT_SENT` |

## Day-after checks

Print verification SQLs (callbacks vs still missing):

```bash
python3 replay_bb_cards.py check --check-date YYYY-MM-DD
```

By default the replay table is assumed to be from `check-date - 1 day` (`ams_replay_MMDDYY`). Override with `--replay-date` or `--table-name`.

## Defaults

| Item | Value |
|------|--------|
| CSV S3 | `s3://dtv-prod-bigdatadl-330572541641-sms/tmp/cards2replay.csv` |
| Replay folder | `s3://dtv-prod-bigdatadl-330572541641-sms/replay/YYYYMMDD/` |
| HUDI path | `s3://aeg-prod-bigdatadl-integration-sms/broadband/bb-card-status/` |
| Athena DB | `integration` |
| Replay table | `ams_replay_MMDDYY` |
| Sample filter (query path) | `sampleid <> '05'` |
| Region | `us-east-1` |

## Files

- `replay_bb_cards.py` — CLI
- `hudi_bb_card_upsert.scala` — EMR Studio HUDI upsert
- `requirements.txt` — Python deps (`boto3`)
