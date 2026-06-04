# Tushare Mirror Phase 1 Runbook

Last reviewed: 2026-05-31

Phase 1 started as a minimal file-lake closed loop for `daily`. Phase 1.2 extends the same closed loop to a small set of low-volume stable endpoints. It is not a full Tushare mirror, does not backfill history, and does not support minute/tick/financial expansion.

## Scope

Implemented:

- SQLite catalog initialization.
- Endpoint config loading for `daily`, `stock_basic`, `trade_cal`, `adj_factor`, and `daily_basic`.
- Permission probe for enabled low-volume endpoints.
- Staged write under `_tmp/run_id=<run_id>/`.
- Raw `JSONL.zst` archive.
- Lake Parquet writer with standard metadata columns.
- Basic schema registry and schema evolution checks.
- Active table/API snapshot commit after validation.
- Checksum, size, record-count, raw-event-count, and Parquet footer validation.
- LakeReader reads the latest active snapshot.
- Catalog observability commands for runs/jobs/snapshots/validations/permissions.
- Fetch dry-run job plan.
- SQLite catalog schema version and backup command.

Not implemented in Phase 1:

- Full mirror endpoint coverage.
- Historical backfill loops.
- Minute/tick large-volume sync.
- Financial PIT derived loaders.
- PostgreSQL derived loader.
- Compaction executor.
- Full data backup/restore executor beyond SQLite catalog backup.
- Iceberg/Delta metadata writer.
- Multi-writer concurrency.
- Remote object storage.

## Token Setup

Set a Tushare token in the environment or `.env`:

```bash
export TUSHARE_TOKEN='<your-token>'
```

The catalog stores only an HMAC token hash for probe results. Raw files and
catalog rows must not store the plaintext token.

## Initialize Catalog

```bash
python -m tushare_mirror --root /tmp/tushare-mirror-real init-catalog
```

This creates:

```text
/tmp/tushare-mirror-real/_catalog/catalog.sqlite
/tmp/tushare-mirror-real/_catalog/endpoints/stock.yaml
```

## Probe Daily

```bash
python -m tushare_mirror --root /tmp/tushare-mirror-real probe --api daily
```

Expected result for an accessible token:

```text
daily: accessible
```

An empty but permission-valid response is recorded as `empty_but_accessible` only
when endpoint config allows it.

## Fetch Dry Run

Use dry-run to inspect the job plan without sending a Tushare request and without
writing raw, lake, job, checkpoint, or snapshot records:

```bash
python -m tushare_mirror --root /tmp/tushare-mirror-real fetch --api daily --params '{"trade_date":"20250102"}' --dry-run
python -m tushare_mirror --root /tmp/tushare-mirror-real fetch --api daily --params '{"trade_date":"20250102"}' --dry-run --json
```

The plan includes `api_name`, `params_hash`, `job_key`, volume class, partition
values, expected raw path, expected lake path prefix, latest permission status,
whether the permission probe is expired, whether active data already covers the
job, and planned actions.

## Fetch One Trade Date

```bash
python -m tushare_mirror --root /tmp/tushare-mirror-real fetch --api daily --params '{"trade_date":"20250102"}'
```

Expected output includes:

```text
run_id=<run_id>
job_key=<job_key>
snapshot_id=<snapshot_id>
record_count=<n>
```

The command writes raw and lake files through staging first. Checkpoint advances
only after validation and snapshot commit succeed.

## Validate Latest Snapshot

```bash
python -m tushare_mirror --root /tmp/tushare-mirror-real validate --snapshot latest
```

Validation checks:

- File exists.
- SHA256 matches catalog.
- Size matches catalog.
- Parquet footer is readable.
- Lake `record_count` matches Parquet rows.
- Raw `raw_event_count` matches JSONL events.
- `schema_id` exists for lake files.

## List Files

```bash
python -m tushare_mirror --root /tmp/tushare-mirror-real list-files --api daily --snapshot latest
python -m tushare_mirror --root /tmp/tushare-mirror-real list-files --api daily --snapshot latest --json
```

The command lists active lake files for the latest `daily` snapshot.

## Inspect Catalog

```bash
python -m tushare_mirror --root /tmp/tushare-mirror-real catalog-inspect
python -m tushare_mirror --root /tmp/tushare-mirror-real show-runs --api daily --limit 20
python -m tushare_mirror --root /tmp/tushare-mirror-real show-jobs --api daily --limit 20
python -m tushare_mirror --root /tmp/tushare-mirror-real show-snapshots --api daily --limit 20
python -m tushare_mirror --root /tmp/tushare-mirror-real show-validations --api daily --limit 20
python -m tushare_mirror --root /tmp/tushare-mirror-real show-permissions --api daily --limit 20
```

Each command supports `--json` for machine-readable output. These commands read
from the local SQLite catalog and do not require an external database client.

## Catalog Version And Backup

```bash
python -m tushare_mirror --root /tmp/tushare-mirror-real catalog-version
python -m tushare_mirror --root /tmp/tushare-mirror-real catalog-backup --output /tmp/tushare-catalog-backup.sqlite
```

The MVP SQLite catalog is a local single-writer catalog. Catalog mutations use
SQLite transactions. The backup command uses the SQLite backup API rather than
copying a live WAL database file directly.

## Snapshot Semantics

Phase 1.3 uses per-API table snapshots. Every successful fetch for one endpoint
creates a snapshot for that endpoint with `api_name`, `table_id`, `snapshot_id`,
`sequence_number`, `created_at`, `parent_snapshot_id`, and `status`.

`latest` has these meanings:

- `--api <api> --snapshot latest` resolves to that API/table's latest `current`
  snapshot.
- `validate --snapshot latest` without `--api` validates all APIs' latest
  `current` snapshots. It is equivalent to `validate --latest-all`.
- `list-files --api <api> --snapshot latest` lists that API's latest lake files.
- `list-files --snapshot latest` without `--api` lists lake files from every
  API's latest snapshot.
- `show-snapshots --latest` shows each API's latest snapshot.
- `show-snapshots --api <api> --latest` shows only that API's latest snapshot.

Global snapshots are not implemented in Phase 1.3. The catalog has
`snapshot_refs` as a compatibility placeholder for a future full-mirror or
multi-endpoint batch snapshot, but current commands operate on per-API snapshots.

Useful commands:

```bash
python3 -m tushare_mirror --root /tmp/tushare-mirror-real-12 validate --snapshot latest
python3 -m tushare_mirror --root /tmp/tushare-mirror-real-12 validate --latest-all
python3 -m tushare_mirror --root /tmp/tushare-mirror-real-12 validate --api daily --snapshot latest
python3 -m tushare_mirror --root /tmp/tushare-mirror-real-12 list-files --snapshot latest
python3 -m tushare_mirror --root /tmp/tushare-mirror-real-12 list-files --api daily --snapshot latest
python3 -m tushare_mirror --root /tmp/tushare-mirror-real-12 show-snapshots --latest
python3 -m tushare_mirror --root /tmp/tushare-mirror-real-12 show-snapshots --api daily --latest
```

## Read Latest Snapshot

Python example:

```python
from tushare_mirror.catalog import CatalogStore
from tushare_mirror.reader import LakeReader

root = "/tmp/tushare-mirror-real"
catalog = CatalogStore(root)
table = LakeReader(root, catalog).scan_api(
    "daily",
    filters={"trade_date": "20250102"},
    columns=["ts_code", "trade_date", "close", "_job_key"],
)
print(table)
```

## Phase 1.2 Low-volume Endpoint Smoke Tests

Phase 1.2 supports only these endpoints:

- `daily`
- `stock_basic`
- `trade_cal`
- `adj_factor`
- `daily_basic`

The following commands send real Tushare requests. Run them only when you intend
to spend quota on a single minimal request per endpoint. Do not loop dates, do
not iterate stocks, and do not run a full mirror. A `permission_denied` result for
any endpoint is a valid observable state; it means the endpoint is not currently
accessible for the token and is not by itself a system failure.

Initialize an isolated root:

```bash
python3 -m tushare_mirror --root /tmp/tushare-mirror-real-12 init-catalog
```

`stock_basic` minimal smoke:

```bash
python3 -m tushare_mirror --root /tmp/tushare-mirror-real-12 probe --api stock_basic
python3 -m tushare_mirror --root /tmp/tushare-mirror-real-12 fetch --api stock_basic --params '{"list_status":"L"}'
python3 -m tushare_mirror --root /tmp/tushare-mirror-real-12 validate --snapshot latest --api stock_basic
```

`trade_cal` minimal smoke:

```bash
python3 -m tushare_mirror --root /tmp/tushare-mirror-real-12 probe --api trade_cal
python3 -m tushare_mirror --root /tmp/tushare-mirror-real-12 fetch --api trade_cal --params '{"exchange":"SSE","start_date":"20250101","end_date":"20250131"}'
python3 -m tushare_mirror --root /tmp/tushare-mirror-real-12 validate --snapshot latest --api trade_cal
```

`adj_factor` minimal smoke:

```bash
python3 -m tushare_mirror --root /tmp/tushare-mirror-real-12 probe --api adj_factor
python3 -m tushare_mirror --root /tmp/tushare-mirror-real-12 fetch --api adj_factor --params '{"trade_date":"20250102"}'
python3 -m tushare_mirror --root /tmp/tushare-mirror-real-12 validate --snapshot latest --api adj_factor
```

`daily_basic` minimal smoke:

```bash
python3 -m tushare_mirror --root /tmp/tushare-mirror-real-12 probe --api daily_basic
python3 -m tushare_mirror --root /tmp/tushare-mirror-real-12 fetch --api daily_basic --params '{"trade_date":"20250102"}'
python3 -m tushare_mirror --root /tmp/tushare-mirror-real-12 validate --snapshot latest --api daily_basic
```

Use catalog observability after each minimal smoke or after the sequence:

```bash
python3 -m tushare_mirror --root /tmp/tushare-mirror-real-12 catalog-inspect
python3 -m tushare_mirror --root /tmp/tushare-mirror-real-12 show-permissions
python3 -m tushare_mirror --root /tmp/tushare-mirror-real-12 show-runs --limit 20
python3 -m tushare_mirror --root /tmp/tushare-mirror-real-12 show-jobs --limit 20
python3 -m tushare_mirror --root /tmp/tushare-mirror-real-12 show-snapshots --limit 20
python3 -m tushare_mirror --root /tmp/tushare-mirror-real-12 show-validations --limit 20
```

For dry-run only, append `--dry-run` to `fetch`. Dry-run does not request
Tushare and does not create raw, lake, job, file, checkpoint, or snapshot rows.

## Clean Test Root

```bash
rm -rf /tmp/tushare-mirror-real
```

Only clean explicit temporary roots. Do not delete `data/tushare/` unless you
intend to remove local mirror data.

## Phase 2 Low-risk Endpoint Expansion

Phase 2 adds low-risk A-share base/event endpoints without turning the project
into a full mirror. Supported Phase 2 endpoints are:

- `weekly`
- `monthly`
- `suspend_d`
- `namechange`
- `hs_const`
- `stk_managers`
- `stk_rewards`

Minimal dry-run commands, which do not send Tushare requests and do not write
raw/lake/job/snapshot rows:

```bash
python3 -m tushare_mirror --root /tmp/tushare-mirror-phase2 fetch --api weekly --params '{"trade_date":"20250103"}' --dry-run
python3 -m tushare_mirror --root /tmp/tushare-mirror-phase2 fetch --api monthly --params '{"trade_date":"20250127"}' --dry-run
python3 -m tushare_mirror --root /tmp/tushare-mirror-phase2 fetch --api suspend_d --params '{"trade_date":"20250102"}' --dry-run
python3 -m tushare_mirror --root /tmp/tushare-mirror-phase2 fetch --api namechange --params '{"ts_code":"000001.SZ"}' --dry-run
python3 -m tushare_mirror --root /tmp/tushare-mirror-phase2 fetch --api hs_const --params '{"hs_type":"SH","is_new":"1"}' --dry-run
python3 -m tushare_mirror --root /tmp/tushare-mirror-phase2 fetch --api stk_managers --params '{"ts_code":"000001.SZ"}' --dry-run
python3 -m tushare_mirror --root /tmp/tushare-mirror-phase2 fetch --api stk_rewards --params '{"ts_code":"000001.SZ"}' --dry-run
```

Minimal real smoke commands for one endpoint follow the same probe/fetch/validate
shape. These commands send real requests and consume quota; run only one minimal
request per endpoint and do not loop dates or stocks:

```bash
python3 -m tushare_mirror --root /tmp/tushare-mirror-phase2 init-catalog
python3 -m tushare_mirror --root /tmp/tushare-mirror-phase2 probe --api weekly
python3 -m tushare_mirror --root /tmp/tushare-mirror-phase2 fetch --api weekly --params '{"trade_date":"20250103"}'
python3 -m tushare_mirror --root /tmp/tushare-mirror-phase2 validate --api weekly --snapshot latest
python3 -m tushare_mirror --root /tmp/tushare-mirror-phase2 list-files --api weekly --snapshot latest
```

The opt-in smoke script can run the whole Phase 2 low-risk set once:

```bash
python3 scripts/tushare_real_smoke.py --phase-2-low-volume --root /tmp/tushare-mirror-real-phase2 --reset-root
```

`permission_denied`, `rate_limited`, and `empty_but_accessible` are observable
endpoint states. They should be reported rather than treated as framework bugs.
Response-shape, writer, validation, and reader failures are system failures.
Use snapshot observability after a smoke run:

```bash
python3 -m tushare_mirror --root /tmp/tushare-mirror-phase2 show-snapshots --latest
python3 -m tushare_mirror --root /tmp/tushare-mirror-phase2 validate --snapshot latest
python3 -m tushare_mirror --root /tmp/tushare-mirror-phase2 show-jobs --limit 50
```


## Phase 2.1 Scoped Backfill Planner

Phase 2.1 adds controlled scoped backfill planning for existing low-risk date
series endpoints. It is not a full mirror and it does not infer full history,
all stocks, all endpoints, or trading calendars from the network.

Supported scoped date backfill endpoints:

- `daily`
- `adj_factor`
- `daily_basic`
- `weekly`
- `monthly`
- `suspend_d`

Unsupported in Phase 2.1: `stock_basic`, `trade_cal`, `namechange`, `hs_const`,
`stk_managers`, and `stk_rewards`. These require snapshot, calendar, code, or
period-specific planning and should not be routed through date-only backfill.

`backfill-plan` is always dry-run. It never sends Tushare requests and does not
write raw/lake/job/file/snapshot rows:

```bash
python3 -m tushare_mirror --root /tmp/tushare-backfill backfill-plan \
  --api daily \
  --dates 20250102,20250103 \
  --max-jobs 2

python3 -m tushare_mirror --root /tmp/tushare-backfill backfill-plan \
  --api weekly \
  --start-date 20250102 \
  --end-date 20250106 \
  --max-jobs 3
```

`backfill` also defaults to dry-run. It uses the same output shape as
`backfill-plan` unless `--execute` is present:

```bash
python3 -m tushare_mirror --root /tmp/tushare-backfill backfill \
  --api daily \
  --dates 20250102,20250103 \
  --max-jobs 2
```

Real execution requires both `--execute` and an explicit `--max-jobs`. Phase 2.1
refuses to execute more than 20 jobs in one command and provides no `--force`:

```bash
python3 -m tushare_mirror --root /tmp/tushare-backfill backfill \
  --api daily \
  --dates 20250102,20250103 \
  --max-jobs 2 \
  --execute
```

Execution is serial. Each date becomes one `trade_date` job, and each job uses
the existing fetch/staged write/schema/snapshot path. Existing active data is
skipped. Failed jobs can be retried. Quarantined jobs are blocked by default and
must be handled manually.

Optional validation checks only the target API latest snapshot, not all API
latest snapshots:

```bash
python3 -m tushare_mirror --root /tmp/tushare-backfill backfill \
  --api daily \
  --dates 20250102,20250103 \
  --max-jobs 2 \
  --execute \
  --validate-latest
```

### Phase 2.4 Calendar-aware Scoped Backfill

`--trading-days-only` reads local `trade_cal` lake data. It does not request
Tushare. If no local `trade_cal` latest snapshot exists, the planner fails with
`trading-days-only requires local trade_cal latest snapshot; fetch trade_cal first`.
Phase 2.4 supports `--calendar-exchange SSE` only. Unsupported exchanges fail
without falling back to network access.

Fetch a small calendar range first, then plan from local calendar data:

```bash
python3 -m tushare_mirror --root /tmp/tushare-backfill fetch \
  --api trade_cal \
  --params '{"exchange":"SSE","start_date":"20250101","end_date":"20250110"}'

python3 -m tushare_mirror --root /tmp/tushare-backfill backfill-plan \
  --api daily \
  --start-date 20250101 \
  --end-date 20250110 \
  --trading-days-only \
  --calendar-exchange SSE \
  --max-jobs 3
```

Calendar-aware plan output includes `calendar_source`, `exchange`,
`requested_start_date`, `requested_end_date`, `natural_days`, `trading_days`,
`filtered_non_trading_days`, `filtered_non_trading_dates`, and
`truncated_by_max_jobs`. This makes max-job truncation and weekend/holiday
filtering visible before execution. In Phase 2.4, trading-calendar filtering is
for daily-like endpoints: `daily`, `adj_factor`, `daily_basic`, and
`suspend_d`. `weekly` and `monthly` reject `--trading-days-only`; their dates
represent week/month bar anchors rather than ordinary daily trading sessions.

Execute a small daily calendar-aware smoke only after reviewing the plan:

```bash
python3 -m tushare_mirror --root /tmp/tushare-backfill backfill \
  --api daily \
  --start-date 20250101 \
  --end-date 20250110 \
  --trading-days-only \
  --calendar-exchange SSE \
  --max-jobs 3 \
  --execute \
  --validate-latest
```

The opt-in real smoke script performs exactly that small sequence: fetch a
local `trade_cal` window, validate it, plan daily with `--trading-days-only`,
and execute at most 3 daily jobs. It is not run by tests or CI by default:

```bash
python3 scripts/tushare_real_smoke.py \
  --calendar-backfill \
  --root /tmp/tushare-mirror-calendar-backfill-smoke \
  --reset-root
```

Do not use this as a full historical calendar or market backfill. It is a
small scoped validation path, and it never implicitly fetches `trade_cal` from
inside the planner.

Use observability commands after an executed scoped backfill:

```bash
python3 -m tushare_mirror --root /tmp/tushare-backfill show-runs --api daily --limit 20
python3 -m tushare_mirror --root /tmp/tushare-backfill show-jobs --api daily --limit 20
python3 -m tushare_mirror --root /tmp/tushare-backfill show-snapshots --api daily --latest
python3 -m tushare_mirror --root /tmp/tushare-backfill validate --api daily --snapshot latest
python3 -m tushare_mirror --root /tmp/tushare-backfill list-files --api daily --snapshot latest
```

Opt-in real smoke commands are intentionally small and should only be run after
explicit confirmation:

```bash
python3 -m tushare_mirror --root /tmp/tushare-mirror-backfill-smoke init-catalog
python3 -m tushare_mirror --root /tmp/tushare-mirror-backfill-smoke backfill-plan \
  --api daily \
  --dates 20250102,20250103 \
  --max-jobs 2
python3 -m tushare_mirror --root /tmp/tushare-mirror-backfill-smoke backfill \
  --api daily \
  --dates 20250102,20250103 \
  --max-jobs 2 \
  --execute
```

Do not use Phase 2.1 commands to run a full mirror, loop all historical dates,
loop all stocks, or run all endpoints.


### Backfill Observability

Use `show-runs` to inspect batch-level backfill counters. Backfill runs expose
planning and result counters directly, so a skip-only idempotent rerun does not
look like an empty run:

```bash
python3 -m tushare_mirror --root /tmp/tushare-backfill show-runs --api daily --limit 20
python3 -m tushare_mirror --root /tmp/tushare-backfill show-runs --api daily --json --limit 20
```

Important fields:

- `planned_jobs`: jobs included in the scoped plan.
- `executed_jobs`: jobs that actually called fetch/write paths.
- `skipped_jobs`: jobs skipped because active data already exists.
- `succeeded_jobs`: jobs newly fetched and committed successfully.
- `failed_jobs`: jobs that failed during execution.
- `blocked_jobs`: jobs blocked before execution, such as quarantined jobs.
- `quarantined_jobs`: jobs that entered or matched quarantine state.

A skip-only rerun can have `job_count=0` because existing jobs are not rebound to
that run. That is expected. Check `planned_jobs` and `skipped_jobs`; they should
show what the run considered and skipped without creating duplicate jobs, raw
files, lake files, or snapshots.

Use `show-run` for item-level detail:

```bash
python3 -m tushare_mirror --root /tmp/tushare-backfill show-run --run-id <run_id>
python3 -m tushare_mirror --root /tmp/tushare-backfill show-run --run-id <run_id> --json
```

For backfill runs, each item shows:

- `date`
- `job_key`
- `existing_status`
- `planned_action`
- `result_status`
- `snapshot_id`
- `record_count`
- `raw_event_count`
- `error_type`

To verify an idempotent rerun, compare catalog counters before and after the
rerun. `run_count` may increase, but `job_count`, `file_count`, and
`snapshot_count` should not grow when all jobs are `skip_existing`.

Failed, blocked, or quarantined jobs should be investigated through `show-run`,
`show-jobs`, `show-validations`, and the `_quarantine/` catalog records before
rerunning. Do not override quarantine automatically.

## Real Smoke Script

The repository includes an opt-in real smoke script. It is not run by unittest or
CI by default because it sends real Tushare requests and consumes quota.

```bash
python3 scripts/tushare_real_smoke.py --help
python3 scripts/tushare_real_smoke.py --root /tmp/tushare-mirror-real-smoke --reset-root --all-phase-1
python3 scripts/tushare_real_smoke.py --root /tmp/tushare-mirror-real-smoke --reset-root --phase-2-low-volume
python3 scripts/tushare_real_smoke.py --root /tmp/tushare-mirror-real-smoke --reset-root --endpoint daily
```

The script requires `TUSHARE_TOKEN` from the environment or `.env`, but never
prints the token. `--all-phase-1` runs `daily`, `stock_basic`, `trade_cal`,
`adj_factor`, and `daily_basic`. `--phase-2-low-volume` runs `weekly`,
`monthly`, `suspend_d`, `namechange`, `hs_const`, `stk_managers`, and
`stk_rewards`. Each endpoint uses one minimal request; the script does not loop
dates or stocks and does not perform a full mirror.

`permission_denied` and `rate_limited` are reported as observable endpoint
states. Response-shape errors, writer errors, validation failures, and reader
failures should be treated as system failures. Clean temporary roots explicitly:

```bash
rm -rf /tmp/tushare-mirror-real-smoke
```

## Real Tushare Smoke Test

This smoke test sends real requests to Tushare. Run it only after confirming the
token and quota are intended for a single `daily` trade-date check. Do not loop
dates, do not add endpoints, and do not run a full mirror.

Set the token first:

```bash
export TUSHARE_TOKEN='<your-token>'
```

Run only this single-date sequence:

```bash
python -m tushare_mirror --root /tmp/tushare-mirror-real init-catalog
python -m tushare_mirror --root /tmp/tushare-mirror-real probe --api daily
python -m tushare_mirror --root /tmp/tushare-mirror-real fetch --api daily --params '{"trade_date":"20250102"}'
python -m tushare_mirror --root /tmp/tushare-mirror-real validate --snapshot latest --api daily
python -m tushare_mirror --root /tmp/tushare-mirror-real list-files --api daily --snapshot latest
python -m tushare_mirror --root /tmp/tushare-mirror-real catalog-inspect
python -m tushare_mirror --root /tmp/tushare-mirror-real show-jobs --api daily
python -m tushare_mirror --root /tmp/tushare-mirror-real show-snapshots --api daily
python -m tushare_mirror --root /tmp/tushare-mirror-real show-validations --api daily
```

Expected directories and files include:

- `_catalog/catalog.sqlite`
- `raw/api=daily/ingest_date=<YYYYMMDD>/job=<job_key>.jsonl.zst`
- `lake/market=a/domain=stock/api=daily/year=2025/month=01/part-<suffix>.parquet`

If the token is missing, commands that require Tushare access fail before making
requests with `TUSHARE_TOKEN is required`. If the token lacks permission,
`probe` records `permission_denied` in `permission_probes`, and `show-permissions`
shows the status without exposing the token. Rate limiting should appear as
`rate_limited`. Invalid parameters should appear as `invalid_params`.

Clean the smoke root only when the result is no longer needed:

```bash
rm -rf /tmp/tushare-mirror-real
```

Real Tushare probe/fetch was not executed by the unit test suite.

## Phase 2.6 Coverage and Gap Report

`coverage` is a read-only report for date-based endpoint coverage. It answers
which dates already have active data, which dates are missing, which dates had a
failed job, and which dates are blocked by quarantine before you run a backfill.
It does not send Tushare requests and does not create runs, jobs, files,
snapshots, or validation records.

Use it with explicit dates:

```bash
python3 -m tushare_mirror --root /tmp/tushare-backfill coverage \
  --api daily \
  --dates 20250102,20250103,20250106
```

Or use a bounded date range:

```bash
python3 -m tushare_mirror --root /tmp/tushare-backfill coverage \
  --api daily \
  --start-date 20250101 \
  --end-date 20250110
```

Calendar-aware coverage uses the local `trade_cal` latest snapshot and never
fetches the calendar implicitly:

```bash
python3 -m tushare_mirror --root /tmp/tushare-backfill coverage \
  --api daily_basic \
  --start-date 20250101 \
  --end-date 20250110 \
  --trading-days-only \
  --calendar-exchange SSE
```

If local `trade_cal` is missing, the command fails with the same safety error as
`backfill-plan`: `trading-days-only requires local trade_cal latest snapshot;
fetch trade_cal first`. In Phase 2.6, calendar filtering is only for daily-like
endpoints: `daily`, `adj_factor`, `daily_basic`, and `suspend_d`. `weekly` and
`monthly` can be inspected by explicit dates or natural date ranges, but they do
not support `--trading-days-only`.

Coverage and `backfill-plan` reuse the same planner, so for the same input their
`existing_status` and `planned_action` should match. The difference is intent:
`coverage` is an inventory and gap report; `backfill-plan` is a pre-execution
plan. Typical statuses are:

- `active_exists` / `skip_existing`: the date is already covered by the API's latest snapshot.
- `missing` / `fetch`: no active data exists for that date.
- `failed_exists` / `retry_failed`: a previous job failed and can be retried.
- `quarantined_exists` / `blocked_quarantined`: the job is quarantined and should be reviewed manually.
- `staged_exists` / `retry_failed`: staged leftovers exist and should be handled through retry or cleanup.

Use `--json` when you want to compare coverage in scripts:

```bash
python3 -m tushare_mirror --root /tmp/tushare-backfill coverage \
  --api daily \
  --start-date 20250101 \
  --end-date 20250110 \
  --trading-days-only \
  --calendar-exchange SSE \
  --json
```

The JSON summary includes `total_dates`, `covered_dates`, `missing_dates`,
`failed_dates`, `quarantined_dates`, and `coverage_ratio`, plus one item per
planned date with `job_key`, `snapshot_id`, `record_count`, `raw_event_count`,
`file_count`, and the previous job status. Use this report to decide whether a
small scoped backfill is needed; do not use it as an excuse to start a full
mirror or broad historical loop.


## Phase 2.7 Coverage-driven Missing Backfill

`backfill-missing` turns a coverage report into a missing-only scoped backfill.
It first computes coverage for the requested dates, then executes only dates with
`existing_status=missing`. Dates already covered by active snapshots remain
`skip_existing` and are not fetched again.

Start with coverage:

```bash
python3 -m tushare_mirror --root /tmp/tushare-backfill coverage   --api daily_basic   --start-date 20250101   --end-date 20250110   --trading-days-only   --calendar-exchange SSE
```

Then inspect the missing-only dry-run plan. This is still read-only and does not
send Tushare requests:

```bash
python3 -m tushare_mirror --root /tmp/tushare-backfill backfill-missing   --api daily_basic   --start-date 20250101   --end-date 20250110   --trading-days-only   --calendar-exchange SSE   --max-jobs 3
```

Execute only after reviewing the plan:

```bash
python3 -m tushare_mirror --root /tmp/tushare-backfill backfill-missing   --api daily_basic   --start-date 20250101   --end-date 20250110   --trading-days-only   --calendar-exchange SSE   --max-jobs 3   --execute   --validate-latest
```

`--max-jobs` is required for `backfill-missing`. Execution still has the Phase
2.x hard cap of 20 jobs. If no missing jobs remain, the command prints `No
missing jobs to backfill.` and does not create a backfill run.

Failed jobs are visible in the report, but they are not executed by default. Use
`--retry-failed` when you explicitly want `failed_exists` dates to join the
candidate set:

```bash
python3 -m tushare_mirror --root /tmp/tushare-backfill backfill-missing   --api daily   --dates 20250102,20250103   --max-jobs 2   --retry-failed
```

Quarantined and staged dates remain blocked. They are shown in the plan so you
can see the gap, but `backfill-missing` will not overwrite them automatically.
Manual review is required before those dates can be retried safely.

After execution, rerun `coverage` for the same range. The expected sign of a
successful scoped run is that `coverage_ratio` increases and the executed dates
move from `missing` to `active_exists`.

`backfill-missing` does not infer full history, does not iterate stocks, does not
iterate endpoints, and does not fetch `trade_cal` implicitly. Calendar-aware mode
requires a local `trade_cal` latest snapshot, the same as `coverage` and
`backfill-plan`.


## Phase 2.8 Local Backup and Restore-check

Phase 2.8 adds a local backup manifest so a file lake snapshot can be copied and
checked without contacting Tushare. The goal is local durability verification,
not remote disaster recovery.

`catalog-backup` still exists and only copies the SQLite catalog:

```bash
python3 -m tushare_mirror --root /tmp/tushare-backfill catalog-backup   --output /tmp/catalog-backup.sqlite
```

`backup` is broader: it copies the SQLite catalog via SQLite's backup API,
endpoint config YAML files, and the raw/lake files referenced by current active
snapshots. It writes `manifest.json` into the backup root.

Preview the scope first:

```bash
python3 -m tushare_mirror --root /tmp/tushare-backfill backup-plan   --target /tmp/tushare-mirror-backup
```

Machine-readable preview:

```bash
python3 -m tushare_mirror --root /tmp/tushare-backfill backup-plan   --target /tmp/tushare-mirror-backup   --json
```

Run the local backup:

```bash
python3 -m tushare_mirror --root /tmp/tushare-backfill backup   --target /tmp/tushare-mirror-backup
```

If the target exists, backup refuses to overwrite it unless `--overwrite` is
explicitly passed:

```bash
python3 -m tushare_mirror --root /tmp/tushare-backfill backup   --target /tmp/tushare-mirror-backup   --overwrite
```

The backup target must not be the source root or inside the source root. Backup
uses a same-directory staging path and only moves it into place after
`restore-check` succeeds.

Check a backup without relying on the original source root:

```bash
python3 -m tushare_mirror restore-check   --backup /tmp/tushare-mirror-backup
```

`restore-check` verifies:

- `manifest.json` exists and has a supported version.
- Catalog backup exists, checksum matches, and SQLite can open it.
- Endpoint config files exist and match size/checksum.
- Every manifest data file exists.
- File size and sha256 match the manifest.
- Raw JSONL.zst files can be read and their raw event counts match.
- Lake Parquet footers can be read and record counts match.
- Manifest `file_count` matches the listed file entries.

Phase 2.8 backs up current active raw/lake files only. It does not copy `_tmp/`,
`_quarantine/`, inactive/superseded files, compacted/deleted/missing files,
remote object storage, PostgreSQL derived tables, or Iceberg/Delta metadata.
The manifest schema has a `storage_layer` field so future object-index files can
be represented, but this phase does not add object storage support.

A successful local backup looks like:

```text
/tmp/tushare-mirror-backup/
  manifest.json
  _catalog/catalog.sqlite
  _catalog/endpoints/*.yaml
  raw/...
  lake/...
```

This is a local backup and restore-check MVP. It is not encrypted, compressed,
incremental, or remote. Do not treat it as full disaster recovery until a remote
backup policy is designed and tested separately.

### Backup root as read-only/restored root

A successful `backup` directory is self-contained for local read checks. After
`restore-check` succeeds, you can point the normal CLI at the backup root:

```bash
python3 -m tushare_mirror --root /tmp/tushare-mirror-backup catalog-inspect
python3 -m tushare_mirror --root /tmp/tushare-mirror-backup show-snapshots --latest
python3 -m tushare_mirror --root /tmp/tushare-mirror-backup list-files --api daily_basic --snapshot latest
python3 -m tushare_mirror --root /tmp/tushare-mirror-backup coverage \
  --api daily_basic \
  --start-date 20250101 \
  --end-date 20250110 \
  --trading-days-only \
  --calendar-exchange SSE
```

`restore-check` and `validate` are different operations:

- `restore-check --backup <path>` validates the backup artifact against
  `manifest.json`. It does not write `validation_runs`, does not restore into a
  source root, and does not depend on the original source root.
- `validate --snapshot latest --no-record` with `--root <backup>` validates
  the backup as a normal root without writing `validation_runs`. Use this when
  you want the backup artifact to remain immutable.
- `validate --snapshot latest` without `--no-record` writes new
  `validation_runs` into the backup catalog. This changes the catalog file and
  can make `restore-check` report a catalog checksum mismatch.

The manifest `source_root` field is provenance only. `restore-check`,
`list-files`, `LakeReader`, and `coverage` must resolve data files from the
backup root using relative paths such as `backup_relative_path`; they must not
read files from `source_root`. Tokens are not stored in `manifest.json`, raw
archive files, or the copied catalog.

Phase 2.9 still does not implement `restore`, `restore-copy`, or restore into an
active source root. It also does not add remote disaster recovery.

### Backup manifest compatibility and inspection

Manifest v1 is the compatibility contract for local backup artifacts. Required
top-level fields are:

```text
manifest_version
backup_id
created_at
catalog_schema_version
snapshot_scope
api_names
snapshot_ids
file_count
total_size_bytes
catalog
endpoint_configs
files
```

`source_root` is allowed and is kept only as provenance. `backup-inspect`,
`restore-check`, `LakeReader`, `list-files`, and `coverage` must resolve backup
files relative to the backup root; they must not read from `source_root`.

The required `catalog` fields are:

```text
relative_path
size_bytes
sha256
```

Each `endpoint_configs[]` entry requires:

```text
relative_path
size_bytes
sha256
```

Each `files[]` entry requires:

```text
file_id
api_name
storage_layer
source_relative_path
backup_relative_path
snapshot_ids
size_bytes
sha256
```

`record_count` and `raw_event_count` may be null, but should remain present when
known because `restore-check` uses them to validate raw event counts and lake
Parquet row counts. Unknown extra fields are accepted for v1 compatibility and
reported as warnings, not failures.

Use `backup-inspect` for a lightweight, read-only manifest and catalog summary:

```bash
python3 -m tushare_mirror backup-inspect --backup /tmp/tushare-mirror-backup
python3 -m tushare_mirror backup-inspect --backup /tmp/tushare-mirror-backup --json
```

`backup-inspect` validates the manifest schema and may open the backup catalog to
show counts, but it does not calculate file checksums, read Parquet footers, read
raw JSONL.zst payloads, create `validation_runs`, or modify the backup artifact.
It returns a non-zero exit code for missing manifests, malformed JSON,
unsupported `manifest_version`, or missing required fields.

Use `restore-check` for the full artifact check:

```bash
python3 -m tushare_mirror restore-check --backup /tmp/tushare-mirror-backup
```

`restore-check` first runs the manifest schema validation. If the manifest is
invalid, it fails before doing checksum or file-content checks. If the manifest is
valid, it verifies catalog and endpoint config checksums, raw JSONL.zst
readability and event counts, lake Parquet footer readability and row counts, and
all listed file sizes and checksums. `restore-check` also does not write
`validation_runs`.

A backup root can be inspected like any other root:

```bash
python3 -m tushare_mirror --root /tmp/tushare-mirror-backup catalog-version
python3 -m tushare_mirror --root /tmp/tushare-mirror-backup catalog-inspect
python3 -m tushare_mirror --root /tmp/tushare-mirror-backup coverage \
  --api daily_basic \
  --start-date 20250101 \
  --end-date 20250110 \
  --trading-days-only \
  --calendar-exchange SSE
```

Only `validate --root <backup>` without `--no-record` intentionally writes new
`validation_runs` into the backup catalog. `backup-inspect`, `restore-check`,
`coverage`, and `validate --no-record` are read-only.

### Immutable backup artifact policy

A backup directory should be treated as an immutable artifact. The manifest stores
checksums for the copied catalog and active data files. Commands that write to the
backup catalog change `_catalog/catalog.sqlite`, so the catalog checksum in
`manifest.json` will no longer match.

Read-only commands for a backup artifact:

```bash
python3 -m tushare_mirror restore-check --backup /tmp/tushare-mirror-backup-smoke
python3 -m tushare_mirror backup-inspect --backup /tmp/tushare-mirror-backup-smoke
python3 -m tushare_mirror --root /tmp/tushare-mirror-backup-smoke validate --snapshot latest --no-record
python3 -m tushare_mirror --root /tmp/tushare-mirror-backup-smoke coverage \
  --api daily_basic \
  --start-date 20250101 \
  --end-date 20250110 \
  --trading-days-only \
  --calendar-exchange SSE
```

These commands do not create `validation_runs`, do not create
`validation_failures`, and do not update the backup manifest.

Writable command to avoid on the original backup artifact:

```bash
python3 -m tushare_mirror --root /tmp/tushare-mirror-backup-smoke validate --snapshot latest
```

Plain `validate` records `validation_runs` by design. On a source root this is the
normal behavior. On a backup artifact it mutates the copied catalog and
`restore-check` will fail with a catalog checksum mismatch. The JSON output
includes `catalog_checksum_status: "mismatch"` and `possible_mutation: true` to
make this diagnosis explicit.

Do not edit `manifest.json` to hide the mutation. If you want a writable working
copy, copy the backup directory to a new location and use that copy as a normal
root. Keep the original backup artifact untouched.


## Phase 3.0 Controlled Mirror Orchestrator

`mirror-plan` and `mirror-run` provide a bounded orchestration layer for the first low-risk A-share local mirror. They do not scan all Tushare APIs, do not infer full history, and do not iterate over all stocks.

Supported scope:

```bash
--scope low-risk-a-share
```

This scope is limited to:

- Snapshot/reference: `stock_basic`, `trade_cal`, `hs_const`
- Date-based low-risk: `daily`, `adj_factor`, `daily_basic`, `weekly`, `monthly`, `suspend_d`
- Event/company snapshot fetches only: `namechange`, `stk_managers`, `stk_rewards`

Explicitly out of scope: minute, tick, order-level, realtime, announcements/PDF, news, research reports, financial statements, PIT derived layers, PostgreSQL loaders, remote backup, and restore-into.

### mirror-plan

`mirror-plan` is read-only. It does not send Tushare requests, write raw/lake files, create snapshots, create validation runs, or create mirror runs.

```bash
python3 -m tushare_mirror --root /tmp/tushare-mirror-local mirror-plan \
  --scope low-risk-a-share \
  --mode smoke \
  --max-jobs-per-api 3
```

Use JSON for machine-readable planning:

```bash
python3 -m tushare_mirror --root /tmp/tushare-mirror-local mirror-plan \
  --scope low-risk-a-share \
  --mode smoke \
  --max-jobs-per-api 3 \
  --json
```

The plan shows endpoint category, whether `trade_cal` is required, planned jobs, missing jobs, blocked reason, and whether the endpoint would execute. If local `trade_cal` is missing, calendar-aware endpoints are blocked in the plan rather than silently switching to natural-day backfill.

### mirror-run

`mirror-run` defaults to dry-run behavior unless `--execute` is passed:

```bash
python3 -m tushare_mirror --root /tmp/tushare-mirror-local mirror-run \
  --scope low-risk-a-share \
  --mode smoke \
  --max-jobs-per-api 3
```

A real run requires `--execute` and an explicit `--max-jobs-per-api`:

```bash
python3 -m tushare_mirror --root /tmp/tushare-mirror-local mirror-run \
  --scope low-risk-a-share \
  --mode smoke \
  --max-jobs-per-api 3 \
  --backup-target /tmp/tushare-mirror-local-backup \
  --execute
```

Safety limits:

- `smoke`: `--max-jobs-per-api <= 3`
- `pilot`: `--max-jobs-per-api <= 20`
- no `--force` override
- no stock loop
- no endpoint discovery loop
- no hidden `trade_cal` request from the planner

Execution order:

1. Probe the fixed low-risk endpoint set once.
2. Fetch `trade_cal` dependency.
3. Fetch snapshot/reference endpoints.
4. Run calendar-aware backfill for daily-like endpoints only after local `trade_cal` exists.
5. Run explicit-date backfill for `weekly` and `monthly`.
6. Fetch event/company endpoints once with fixed `000001.SZ` params.
7. Validate all latest snapshots.
8. If `--backup-target` is provided, run local `backup` and `restore-check`.
9. Store a `run_type=mirror` summary that can be viewed with `show-run`.

Permission failures are recorded per endpoint. A non-dependency endpoint with `permission_denied` is blocked without forcing the whole mirror run to fail. If `trade_cal` fails, calendar-aware endpoints are blocked and the run is marked as a dependency failure.

### Smoke Mode

Smoke mode is the smallest real-request mirror check:

- `stock_basic`: one `list_status=L` fetch
- `trade_cal`: one SSE `20250101` to `20250110` fetch
- `hs_const`: one `hs_type=SH,is_new=1` fetch
- `daily`, `adj_factor`, `daily_basic`, `suspend_d`: at most 3 trading-day jobs each
- `weekly`: explicit dates `20250103,20250110`
- `monthly`: explicit dates `20250127,20250228`
- `namechange`, `stk_managers`, `stk_rewards`: one `ts_code=000001.SZ` fetch each

### Pilot Mode

Pilot mode is prepared but should only be executed after review. It is still not full mirror.

Plan only:

```bash
MIRROR_ROOT=/path/to/local/tushare
MIRROR_BACKUP=/path/to/local/tushare-backup

python3 -m tushare_mirror --root "$MIRROR_ROOT" mirror-plan \
  --scope low-risk-a-share \
  --mode pilot \
  --start-date 20250101 \
  --end-date 20250131 \
  --max-jobs-per-api 20 \
  --json
```

Do not execute pilot until the plan is reviewed.

### Observing Mirror Runs

```bash
python3 -m tushare_mirror --root /tmp/tushare-mirror-local show-runs --limit 50
python3 -m tushare_mirror --root /tmp/tushare-mirror-local show-run --run-id <mirror_run_id>
python3 -m tushare_mirror --root /tmp/tushare-mirror-local validate --snapshot latest --no-record
python3 -m tushare_mirror restore-check --backup /tmp/tushare-mirror-local-backup
```

`show-run` displays the mirror endpoint items, including endpoint status, planned jobs, executed jobs, skipped jobs, record counts, snapshot IDs, and blocked reasons.


## Phase 3.1 Pilot Plan Readiness

Phase 3.1 is a dry-run readiness review for one-month pilot planning. It must not execute `mirror-run --execute`, send Tushare requests, create raw/lake files, create snapshots, or create validation runs.

Recommended readiness command:

```bash
PILOT_PLAN_ROOT=/tmp/tushare-mirror-pilot-plan

python3 -m tushare_mirror --root "$PILOT_PLAN_ROOT" init-catalog

python3 -m tushare_mirror --root "$PILOT_PLAN_ROOT" mirror-plan \
  --scope low-risk-a-share \
  --mode pilot \
  --start-date 20250101 \
  --end-date 20250131 \
  --max-jobs-per-api 20 \
  --json
```

Pilot plan output is expected to include:

- top-level `scope`, `mode`, `start_date`, `end_date`, `max_jobs_per_api`, endpoint counts, planned job counts, and `dry_run=true`
- per-endpoint `endpoint`, `category`, `requires_trade_cal`, `plan_status`, `planned_jobs`, `max_jobs`, `existing_coverage`, `missing_jobs`, `blocked_reason`, `will_execute`, `planned_action`, `required_by`, and `notes`

Pilot plan semantics:

- `trade_cal` is the calendar dependency. It is shown as `category=calendar_dependency`, `planned_action=fetch_calendar`, with `exchange=SSE` and the requested date range.
- `daily`, `adj_factor`, `daily_basic`, and `suspend_d` are daily-like calendar-aware endpoints. In an empty root they show `plan_status=blocked_until_trade_cal` and do not fall back to natural-day planning.
- `weekly` and `monthly` do not use `--trading-days-only` in Phase 3.1. They use explicit endpoint-specific date lists for the pilot month.
- `stock_basic` and `hs_const` are snapshot/reference fetches with one planned job each.
- `namechange`, `stk_managers`, and `stk_rewards` are marked `excluded_from_pilot_execution`; pilot mode does not run stock loops.

To confirm no side effects after planning:

```bash
python3 -m tushare_mirror --root "$PILOT_PLAN_ROOT" catalog-inspect
python3 -m tushare_mirror --root "$PILOT_PLAN_ROOT" show-runs --limit 20
python3 -m tushare_mirror --root "$PILOT_PLAN_ROOT" show-jobs --limit 20
python3 -m tushare_mirror --root "$PILOT_PLAN_ROOT" show-snapshots --latest
```

Expected empty-root counts after `mirror-plan`:

- `run_count=0`
- `job_count=0`
- `file_count=0`
- `snapshot_count=0`
- `validation_count=0`

If reviewing an existing smoke root, pilot plan can show `active_exists`, missing jobs, and satisfied dependencies based on the current local lake. It still remains dry-run and does not execute requests.

Pilot execute is a separate user-confirmed step. Do not run it during readiness review. The command to review for a future execution is:

```bash
MIRROR_ROOT=/path/to/local/tushare
MIRROR_BACKUP=/path/to/local/tushare-backup

python3 -m tushare_mirror --root "$MIRROR_ROOT" mirror-run \
  --scope low-risk-a-share \
  --mode pilot \
  --start-date 20250101 \
  --end-date 20250131 \
  --max-jobs-per-api 20 \
  --backup-target "$MIRROR_BACKUP" \
  --execute
```

This command is intentionally not executed as part of Phase 3.1.


## Phase 3.4 Durable Mirror Preflight

Before moving a pilot mirror into durable local storage, run `mirror-preflight` against the intended source root and backup target. Preflight is read-only: it does not create a catalog, does not write raw/lake files, does not create runs/jobs/snapshots/validations, and does not send Tushare requests.

Choose durable paths outside `/tmp`:

```bash
MIRROR_ROOT=/path/to/local/tushare-mirror
MIRROR_BACKUP=/path/to/local/tushare-mirror-backup
```

Path rules:

- `MIRROR_ROOT` and `MIRROR_BACKUP` must be different paths.
- `MIRROR_BACKUP` must not be inside `MIRROR_ROOT`.
- `MIRROR_ROOT` must not be inside `MIRROR_BACKUP`.
- Empty or missing target directories are acceptable after review.
- An existing mirror root with `_catalog/catalog.sqlite` is detected and summarized without mutation.
- A backup target with `manifest.json` is detected as an existing backup artifact; choose a new target or clear it deliberately before executing a new backup.
- Unknown non-empty directories are blocked to avoid mixing mirror data with unrelated files.

Run preflight first:

```bash
python3 -m tushare_mirror mirror-preflight \
  --mirror-root "$MIRROR_ROOT" \
  --backup-target "$MIRROR_BACKUP" \
  --scope low-risk-a-share \
  --mode pilot \
  --start-date 20250101 \
  --end-date 20250131 \
  --max-jobs-per-api 20
```

JSON output is available for automation:

```bash
python3 -m tushare_mirror mirror-preflight \
  --mirror-root "$MIRROR_ROOT" \
  --backup-target "$MIRROR_BACKUP" \
  --scope low-risk-a-share \
  --mode pilot \
  --start-date 20250101 \
  --end-date 20250131 \
  --max-jobs-per-api 20 \
  --json
```

Preflight checks:

- path relationship and unknown non-empty directories
- existing catalog summary when a mirror root already exists
- existing backup manifest summary when a backup target already exists
- `scope=low-risk-a-share` only
- `mode=smoke` or `mode=pilot` only
- `smoke --max-jobs-per-api <= 3`
- `pilot --max-jobs-per-api <= 20` with explicit start/end dates
- token presence without printing the token
- rough local disk free-space information

`/tmp` paths produce warnings because they are suitable for smoke tests but not durable storage. The one-month low-risk pilot is expected to be in the tens-of-MB size class based on the January 2025 pilot; multi-year local mirrors are larger and are not estimated in Phase 3.4.

Recommended durable execution order:

```bash
python3 -m tushare_mirror mirror-preflight \
  --mirror-root "$MIRROR_ROOT" \
  --backup-target "$MIRROR_BACKUP" \
  --scope low-risk-a-share \
  --mode pilot \
  --start-date 20250101 \
  --end-date 20250131 \
  --max-jobs-per-api 20

python3 -m tushare_mirror --root "$MIRROR_ROOT" init-catalog

python3 -m tushare_mirror --root "$MIRROR_ROOT" mirror-plan \
  --scope low-risk-a-share \
  --mode pilot \
  --start-date 20250101 \
  --end-date 20250131 \
  --max-jobs-per-api 20 \
  --json

# Only after user confirmation:
python3 -m tushare_mirror --root "$MIRROR_ROOT" mirror-run \
  --scope low-risk-a-share \
  --mode pilot \
  --start-date 20250101 \
  --end-date 20250131 \
  --max-jobs-per-api 20 \
  --backup-target "$MIRROR_BACKUP" \
  --execute
```

Do not execute `mirror-run --execute` until preflight and mirror-plan have both been reviewed.

## Phase 3.4 Durable Pilot Mirror Execution

Phase 3.4 moves the reviewed one-month pilot workflow from `/tmp` into user-selected durable local storage. This is still a one-month pilot, not a full Tushare mirror. Confirm disk capacity and paths before running it.

Use paths outside `/tmp` for any long-lived mirror:

```bash
# Replace these with durable local paths.
MIRROR_ROOT=/path/to/local/tushare-mirror
MIRROR_BACKUP=/path/to/local/tushare-mirror-backup
```

Keep `MIRROR_BACKUP` outside `MIRROR_ROOT`. The backup target is a separate immutable artifact, not a child directory inside the source lake.

Initialize the catalog if the durable root is new:

```bash
python3 -m tushare_mirror --root "$MIRROR_ROOT" init-catalog
```

Review the pilot plan first:

```bash
python3 -m tushare_mirror --root "$MIRROR_ROOT" mirror-plan \
  --scope low-risk-a-share \
  --mode pilot \
  --start-date 20250101 \
  --end-date 20250131 \
  --max-jobs-per-api 20 \
  --json
```

Only after reviewing the plan and confirming the paths, execute the pilot:

```bash
python3 -m tushare_mirror --root "$MIRROR_ROOT" mirror-run \
  --scope low-risk-a-share \
  --mode pilot \
  --start-date 20250101 \
  --end-date 20250131 \
  --max-jobs-per-api 20 \
  --backup-target "$MIRROR_BACKUP" \
  --execute
```

Validate and check the backup without mutating backup artifacts:

```bash
python3 -m tushare_mirror --root "$MIRROR_ROOT" validate --snapshot latest --no-record
python3 -m tushare_mirror restore-check --backup "$MIRROR_BACKUP"
python3 -m tushare_mirror backup-inspect --backup "$MIRROR_BACKUP"
```

Operational rules:

- This pilot covers only `low-risk-a-share` for `20250101` through `20250131`.
- It does not run a stock loop and does not cover full history.
- It does not touch minute, tick, order-level, financial statement, PIT, PostgreSQL loader, remote backup, or restore-into workflows.
- Later production backfills should proceed month by month, with explicit `mirror-plan` review before each `mirror-run`.
- Every executed batch should end with `validate --no-record`, `backup`, and `restore-check`.
- Do not use `/tmp` as a durable mirror location.

## Controlled Full Backfill Readiness

The durable January 2025 pilot is a readiness artifact, not permission to start a
full mirror. Before planning any next month, review the current mirror root and
backup artifact with read-only commands:

```bash
MIRROR_ROOT=/mnt/gw/TuShare
MIRROR_BACKUP=/mnt/gw/TuShare-backup

python3 -m tushare_mirror mirror-review \
  --root "$MIRROR_ROOT" \
  --backup "$MIRROR_BACKUP" \
  --scope low-risk-a-share

python3 -m tushare_mirror mirror-readiness \
  --root "$MIRROR_ROOT" \
  --backup "$MIRROR_BACKUP" \
  --scope low-risk-a-share
```

`mirror-review` is an inventory command. It opens the catalog, summarizes latest
snapshots, runs validation in `--no-record` mode, checks coverage for the pilot
range, runs backup inspection and restore-check, reports artifact size, and
reports whether token plaintext was found. It does not fetch, backfill, create
runs, create validation rows, or write the catalog.

`mirror-readiness` is a gate. It returns `ready`, `warning`, or `blocked` and a
`ready_for_controlled_full_backfill` boolean. A good January pilot normally
returns `warning` rather than `ready`, because the mirror is still only a pilot
and known out-of-scope datasets remain intentionally absent.

Readiness must be blocked when any of these are true:

- catalog cannot open or schema is unsupported
- latest snapshots are missing
- `trade_cal` latest snapshot is missing
- backup artifact is missing
- restore-check fails
- backup reports `possible_mutation=true`
- `validate --snapshot latest --no-record` fails
- pilot coverage is incomplete for `daily`, `adj_factor`, `daily_basic`, or `suspend_d`
- token plaintext is found
- backup path is nested inside the mirror root
- CLI max-job guardrails are missing

Expected warnings include:

- only the January 2025 pilot is covered
- this is not a full mirror
- event/company endpoints are not stock-looped
- `weekly` and `monthly` do not use trading-days-only
- financial/PIT/minute/tick/object/PostgreSQL datasets are not covered
- no remote disaster recovery
- no compaction

## Monthly Batch Planning

Use `mirror-batch-plan` to prepare the next bounded month. This command is
read-only. It does not fetch `trade_cal`, does not execute backfill, does not
write raw/lake files, does not create snapshots, and does not create validation
runs.

Recommended dry-run for the next month:

```bash
python3 -m tushare_mirror mirror-batch-plan \
  --root "$MIRROR_ROOT" \
  --scope low-risk-a-share \
  --start-date 20250201 \
  --end-date 20250228 \
  --calendar-exchange SSE \
  --max-jobs-per-api 20 \
  --json
```

Batch planning semantics:

- If local `trade_cal` does not cover the requested range, `trade_cal` is planned
  first as a calendar dependency.
- `daily`, `adj_factor`, `daily_basic`, and `suspend_d` stay blocked until local
  `trade_cal` covers the requested range. They must not fall back to natural-day
  planning.
- `weekly` and `monthly` use bounded explicit date planning only. They do not use
  `--trading-days-only`.
- `stock_basic` and `hs_const` show refresh strategy. They are not blindly
  refetched when latest data already exists.
- `namechange`, `stk_managers`, and `stk_rewards` remain excluded from batch
  execution because Phase 3 does not run stock loops.
- If `--max-jobs-per-api` truncates a plan, the endpoint plan shows
  `truncated=true`.
- The output includes `estimated_request_count`, but execution still requires a
  separate user-confirmed command.

## User-confirmed Monthly Execution

Do not execute the next batch until the user confirms the month, root, backup
target, and plan. The execution command remains deliberately separate:

```bash
python3 -m tushare_mirror --root "$MIRROR_ROOT" mirror-run \
  --scope low-risk-a-share \
  --mode pilot \
  --start-date 20250201 \
  --end-date 20250228 \
  --max-jobs-per-api 20 \
  --backup-target "$MIRROR_BACKUP" \
  --execute
```

This is still a controlled monthly batch. It is not a full mirror and must not be
expanded into all history or all endpoints without another reviewed phase.

## After-batch Validation and Backup

After any user-confirmed monthly execution, run:

```bash
python3 -m tushare_mirror --root "$MIRROR_ROOT" validate --snapshot latest --no-record
python3 -m tushare_mirror backup-inspect --backup "$MIRROR_BACKUP"
python3 -m tushare_mirror restore-check --backup "$MIRROR_BACKUP"
python3 -m tushare_mirror mirror-review --root "$MIRROR_ROOT" --backup "$MIRROR_BACKUP" --scope low-risk-a-share
python3 -m tushare_mirror mirror-readiness --root "$MIRROR_ROOT" --backup "$MIRROR_BACKUP" --scope low-risk-a-share
```

Use `validate --no-record` on backup roots or immutable backup artifacts. Plain
`validate` writes `validation_runs` and can intentionally make restore-check
detect a mutated catalog.

## Failure Recovery

Do not expand scope when a batch fails. Classify the failure first:

- `permission_denied`: record the endpoint status; do not broaden requests.
- `rate_limited`: stop or wait; do not add retries beyond the existing bounded
  logic.
- `invalid_params` or field mismatch: make the smallest config/parser fix and
  add a regression test.
- missing `trade_cal` dependency: fetch only the bounded calendar range after
  user confirmation, then re-plan.
- validation failure: inspect latest snapshot files and quarantine state before
  another execute.
- backup or restore-check failure: repair backup safety before any new data
  request.
- schema quarantine: leave the quarantined data blocked until explicitly
  reviewed.

## Stop Conditions

Stop before executing a next batch when any of these are present:

- readiness is blocked
- restore-check failed
- backup `possible_mutation=true`
- `trade_cal` dependency unresolved
- `max_jobs_per_api > 20`
- token missing
- severe disk-space warning
- unresolved validation failure
- schema quarantine exists
- inconsistent coverage

The next batch must remain user-confirmed. Do not run `mirror-run --execute`
from review, readiness, or batch-plan output alone.

## Bounded Code-list Planning Infrastructure

Code-list planning is infrastructure-only in this phase. It exists so future
stock-code-scoped endpoints can be planned from local data before any execution
path is enabled.

Local code universes come only from local latest snapshots:

- `a_share_listed`, `a_share_active`, `a_share_mainboard`, `a_share_sme`,
  `a_share_chinext`, and `a_share_star` read local `stock_basic`.
- `hs_const_sh` and `hs_const_sz` read local `hs_const`.
- If the required local latest snapshot is missing, the command returns a
  blocked result. It never fetches `stock_basic` or `hs_const` implicitly.

Inspect a local universe:

```bash
python3 -m tushare_mirror --root "$MIRROR_ROOT" code-universe \
  --universe a_share_listed \
  --limit 20

python3 -m tushare_mirror --root "$MIRROR_ROOT" code-universe \
  --universe hs_const_sh \
  --limit 20 \
  --json
```

Generate a bounded code-list plan:

```bash
python3 -m tushare_mirror --root "$MIRROR_ROOT" code-list-plan \
  --api namechange \
  --universe a_share_listed \
  --limit-codes 5 \
  --json
```

Important guardrails:

- `code-list-plan` is dry-run only.
- `--limit-codes` is mandatory.
- Phase code-list planning has a hard max of 20 codes.
- The planner reads local lake/catalog data only.
- It does not fetch, backfill, write raw/lake/snapshot data, create
  `validation_runs`, or enable real execution.
- It does not run a full stock loop.
- Disabled inventory endpoints remain blocked even if a local universe exists.
- Any actual fetch or `mirror-run` path for `code_list` or `code_date_matrix`
  planners remains blocked by execution policy.

Future endpoint enablement should stay narrow and explicit:

1. Choose one endpoint.
2. Promote that endpoint into enabled config explicitly; do not bulk-load
   inventory stubs.
3. Add fake-client tests for params, response fields, writer, validation,
   reader, backup, and restore-check behavior.
4. Plan 1-5 local codes with `code-list-plan`.
5. Run a user-confirmed real smoke only for that endpoint and tiny code set.
6. Add coverage/reporting for the new endpoint shape.
7. Expand only after the small smoke and backup/restore-check path pass.

## Bounded Code-date Matrix Planning Infrastructure

Code/date matrix planning is also infrastructure-only. It lets the system show
what a bounded code-by-date request set would look like before any code-loop
execution path exists.

Generate a small dry-run matrix plan:

```bash
python3 -m tushare_mirror --root "$MIRROR_ROOT" code-date-matrix-plan \
  --api stk_managers \
  --universe a_share_listed \
  --limit-codes 3 \
  --dates 20250102,20250103 \
  --json
```

Generate a bounded date-range plan:

```bash
python3 -m tushare_mirror --root "$MIRROR_ROOT" code-date-matrix-plan \
  --api stk_managers \
  --universe a_share_listed \
  --limit-codes 3 \
  --start-date 20250101 \
  --end-date 20250110 \
  --max-dates 5 \
  --json
```

Use local trading days only when local `trade_cal` already exists:

```bash
python3 -m tushare_mirror --root "$MIRROR_ROOT" code-date-matrix-plan \
  --api stk_managers \
  --universe a_share_listed \
  --limit-codes 3 \
  --start-date 20250101 \
  --end-date 20250110 \
  --trading-days-only \
  --calendar-exchange SSE \
  --max-dates 5 \
  --json
```

Important guardrails:

- `code-date-matrix-plan` is dry-run only.
- The code universe comes only from local `stock_basic` or `hs_const` latest
  snapshots.
- `--trading-days-only` reads only local `trade_cal`; it never fetches
  `trade_cal` implicitly.
- `--limit-codes` is mandatory.
- Phase code/date planning has hard limits: 20 codes, 20 dates, and 100
  candidate jobs.
- `--max-dates` should be provided for date ranges; if omitted, the planner
  still applies the phase max of 20 dates.
- The planner reports truncation flags when code, date, or candidate limits cut
  the matrix down.
- The planner reports local `existing_status` where available:
  `missing`, `active_exists`, `failed_exists`, `staged_exists`,
  `quarantined_exists`, or `unknown`.
- `active_exists` becomes `skip_existing`; failed/staged jobs are shown as
  `retry_failed`; quarantined jobs are `blocked_quarantined`.
- Execution remains blocked. The command does not fetch, write catalog rows,
  create `validation_runs`, create raw/lake files, or run a full stock loop.

Future code/date endpoint enablement must stay incremental:

1. Choose one endpoint.
2. Enable that endpoint config explicitly.
3. Add fake-client tests for params, response fields, writer, validation,
   reader, backup, restore-check, and failure behavior.
4. Plan 1-3 codes and 1-3 dates with `code-date-matrix-plan`.
5. Run a user-confirmed real smoke only for that endpoint and tiny matrix.
6. Add coverage/reporting for the endpoint's code/date shape.
7. Expand only after the small smoke and backup/restore-check path pass.

## Period, Code-period, and PIT Readiness Infrastructure

Period and code/period planning are infrastructure-only. They let the system
describe bounded financial or period-based request sets without enabling
financial execution, PIT loaders, or code/period loops.

Generate a bounded period-only plan:

```bash
python3 -m tushare_mirror period-plan \
  --api income \
  --periods 20240331,20240630 \
  --json

python3 -m tushare_mirror period-plan \
  --api fina_indicator \
  --start-period 2024Q1 \
  --end-period 2024Q4 \
  --period-frequency quarterly \
  --max-periods 4 \
  --json
```

Generate a bounded code/period matrix plan from a local code universe:

```bash
python3 -m tushare_mirror --root "$MIRROR_ROOT" code-period-plan \
  --api income \
  --universe a_share_listed \
  --limit-codes 3 \
  --periods 20240331,20240630 \
  --json
```

Inspect PIT metadata readiness:

```bash
python3 -m tushare_mirror pit-readiness
python3 -m tushare_mirror pit-readiness --json
```

Important guardrails:

- `period-plan` is dry-run only.
- `code-period-plan` is dry-run only.
- Financial statement and financial indicator execution remains blocked.
- Periods are reporting periods, not trading dates. Do not route period
  endpoints through trading-day backfill.
- Supported period forms include `2024Q1`, `2024Q2`, `2024Q3`, `2024Q4`,
  `20240331`, `20240630`, `20240930`, and `20241231`.
- Phase period planning has hard limits: 20 periods, 20 codes, and 100
  candidate code/period jobs.
- Code universes come only from local `stock_basic` or `hs_const` latest
  snapshots. The planner never fetches them implicitly.
- The planners report local `existing_status` where available, but
  `execution_allowed` remains false.
- The commands do not fetch, backfill, write catalog rows, create
  `validation_runs`, create raw/lake files, or execute a full stock loop.

PIT safety is the reason financial execution remains disabled. For any future
financial endpoint, the project must know the `period` field, one or more
announcement or disclosure date fields such as `ann_date`, `f_ann_date`, or
`disclosure_date`, and a safe `usable_after` policy. Without that metadata, a
strategy can accidentally use future disclosures and create lookahead bias.

Future financial endpoint enablement must stay incremental:

1. Choose one financial endpoint.
2. Complete PIT metadata, including period field, announcement/disclosure date
   fields, and usable-after strategy.
3. Add fake-client tests for params, fields, writer, validation, reader,
   backup, restore-check, PIT metadata, and policy behavior.
4. Run `period-plan` only.
5. Run `code-period-plan` for 1-3 codes and 1-3 periods.
6. Run a user-confirmed tiny real smoke only after the plan and tests pass.
7. Validate PIT behavior before any strategy-safe derived layer is considered.
8. Expand only after backup and restore-check pass.

Stop immediately if any of these occur:

- missing PIT metadata
- missing disclosure or announcement date field
- schema incompatibility
- future-data or lookahead risk
- unknown rate-limit behavior
- no current backup
- validation failure
- quarantine or staged data requiring manual review

## Object, Text, Intraday, and Compaction Readiness Roadmap

Object/text, intraday, storage estimate, rate policy, enablement checklist, and
compaction planning commands are infrastructure-only. They are meant to make
future endpoint enablement observable before any high-volume or object-download
execution path exists.

Plan object/text endpoints without fetching or downloading content:

```bash
python3 -m tushare_mirror object-plan \
  --api anns \
  --start-date 20250101 \
  --end-date 20250131 \
  --json

python3 -m tushare_mirror object-plan \
  --api news \
  --start-date 20250101 \
  --end-date 20250131 \
  --json
```

Plan intraday bucket layout without requesting minute/tick/order data:

```bash
python3 -m tushare_mirror intraday-plan \
  --api stk_mins \
  --freq 1min \
  --start-date 20250102 \
  --end-date 20250103 \
  --bucket-count 64 \
  --json
```

Estimate storage and review advisory rate/failure policy:

```bash
python3 -m tushare_mirror storage-estimate \
  --scope low-risk-a-share \
  --start-date 20250101 \
  --end-date 20251231 \
  --json

python3 -m tushare_mirror rate-policy --scope low-risk-a-share --json
python3 -m tushare_mirror rate-policy --category intraday --json
```

Inspect compaction readiness without rewriting files:

```bash
python3 -m tushare_mirror compaction-plan \
  --root "$MIRROR_ROOT" \
  --api daily_basic \
  --json
```

Review endpoint enablement prerequisites:

```bash
python3 -m tushare_mirror endpoint-enable-checklist --api fina_indicator --json
python3 -m tushare_mirror endpoint-enable-checklist --api anns --json
python3 -m tushare_mirror endpoint-enable-checklist --api stk_mins --json
```

Important guardrails:

- `object-plan` is plan-only. It does not fetch indexes, download PDFs, fetch
  news/research content, write catalog rows, or create validation rows.
- `intraday-plan` is plan-only. It does not request minute, tick, order, or
  realtime data.
- `compaction-plan` is plan-only. It reads local catalog metadata and does not
  rewrite files, create snapshots, or modify the catalog.
- `storage-estimate` is approximate. Intraday estimates are low-confidence
  warnings, not capacity guarantees.
- `rate-policy` is advisory and does not execute retries or batches.
- `endpoint-enable-checklist` is required before enabling a disabled inventory
  endpoint.
- Object/text execution remains blocked until object index, object store,
  content-addressed deduplication, validation, retention, and backup semantics
  are designed and tested.
- Intraday execution remains blocked until bucket partitioning, storage
  estimates, query benchmarks, rate limits, and compaction semantics are
  designed and tested.
- Compaction execution remains blocked until a snapshot rewrite protocol,
  backup/restore-check behavior, and query benchmark flow exist.

Future enablement must stay narrow:

1. Choose one endpoint.
2. Complete metadata and execution policy for that endpoint.
3. Add fake-client tests for planner, policy, writer, validation, reader,
   backup, restore-check, and failure behavior.
4. Run only the plan command first.
5. Review `storage-estimate`, `rate-policy`, and
   `endpoint-enable-checklist`.
6. Run a user-confirmed tiny real smoke only after tests and backup path pass.
7. Expand only after the smoke remains bounded, observable, backed up, and
   restore-checkable.

Stop immediately if any of these occur:

- object metadata lacks stable object IDs or source URL fields
- object download would be required before object store exists
- intraday storage estimate is severe or unknown
- bucket or compaction policy is unresolved
- query benchmark is missing
- backup or restore-check is missing or failing
- schema incompatibility or quarantine appears
- rate-limit behavior is unknown
- any command would require a stock loop, full mirror, or unbounded history

## All Tushare API Infrastructure Roadmap

The current executable scope is deliberately narrow: `low-risk-a-share` covers
the already-tested reference, calendar, daily-like, weekly/monthly, and a small
set of event/company-governance endpoints. It is not a full Tushare mirror.

Broader Tushare API families are represented as disabled inventory stubs under
`tushare_mirror/endpoint_configs/inventory/`. Inventory entries classify future
endpoints by `endpoint_kind`, `planner_kind`, `execution_status`,
`required_infra`, and risk notes. They are not copied into the executable
catalog by `init-catalog`, do not appear in mirror scopes, and cannot be fetched
unless a later phase explicitly promotes an endpoint into enabled config with
tests and policy approval.

Use this read-only command to inspect infrastructure readiness:

```bash
python3 -m tushare_mirror api-infra-readiness
python3 -m tushare_mirror api-infra-readiness --json
```

The report summarizes:

- currently supported endpoint kinds and planner kinds
- blocked future planner kinds
- disabled inventory endpoint count
- enabled executable endpoint count
- missing infrastructure by category
- recommended next infrastructure phases

Endpoint enablement has four separate layers:

- Inventory: a disabled classification stub only.
- Enabled config: an endpoint in `_catalog/endpoints/*.yaml` with bounded fields,
  params, partitioning, probe config, and `execution_status=enabled`.
- Planner support: a `planner_kind` registered in the planner registry and able
  to produce a bounded plan.
- Execution policy: final guardrails that decide whether a command may execute
  the endpoint.

An endpoint should not be enabled until all four layers are satisfied and
covered by tests.

Infrastructure still required before broader families can execute:

- Financial PIT: disclosure-date handling, PIT-safe snapshots, code/period
  matrix guardrails, and validation for financial statements and indicators.
- Code loops: explicit code-list sources, max-code limits, retry/checkpoint
  behavior, and observable partial progress.
- Period planners: bounded period generation and reporting for macro, fund, and
  financial-period endpoints.
- Object documents: object index planning, local object store layout, size
  limits, retention policy, and restore-check coverage for PDF/news/research
  artifacts.
- Intraday/minute/tick: bucketed storage, request-volume controls, compaction
  policy, and retention rules.
- Compaction: a reviewed compaction executor and backup/restore-check semantics
  for compacted files.
- Realtime: polling cadence, rate-limit policy, retention policy, and explicit
  user confirmation.

Before enabling any new endpoint:

- classify it with `endpoint_kind` and `planner_kind`
- keep it disabled until tests prove loader, planner, policy, writer,
  validation, reader, backup, and restore-check behavior
- add fake-client contract tests before any real request
- run a small opt-in real smoke only after explicit user confirmation
- keep max-job and scope guardrails in place
- never turn inventory stubs into executable endpoints by bulk loading the
  inventory directory

No real fetch should occur from inventory, readiness, review, or batch-planning
commands. Real execution remains limited to explicit user-confirmed commands
such as bounded `mirror-run --execute` or scoped backfill commands.

## Pre-full-backfill Operational Hardening

This section is for the longer infrastructure-only review before any controlled
full-backfill batch. The commands here are read-only unless explicitly writing a
dry-run bundle to a user-provided output directory outside the mirror and backup
roots. They do not fetch real Tushare data, do not run `mirror-run`, do not
backfill new dates, and do not enable disabled inventory endpoints.

Use the durable paths:

```bash
MIRROR_ROOT=/mnt/gw/TuShare
MIRROR_BACKUP=/mnt/gw/TuShare-backup
```

Run the status dashboard first:

```bash
python3 -m tushare_mirror mirror-status \
  --root "$MIRROR_ROOT" \
  --backup "$MIRROR_BACKUP" \
  --scope low-risk-a-share

python3 -m tushare_mirror mirror-status \
  --root "$MIRROR_ROOT" \
  --backup "$MIRROR_BACKUP" \
  --scope low-risk-a-share \
  --json
```

Use audit to summarize local catalog history, failed jobs, validation failures,
quarantine rows, latest run, and optional backup state:

```bash
python3 -m tushare_mirror mirror-audit \
  --root "$MIRROR_ROOT" \
  --backup "$MIRROR_BACKUP" \
  --scope low-risk-a-share \
  --json
```

Use next-batch recommendation to inspect local `trade_cal` and coverage before
choosing a bounded month. It only recommends and does not execute:

```bash
python3 -m tushare_mirror mirror-next-batch \
  --root "$MIRROR_ROOT" \
  --scope low-risk-a-share \
  --json
```

Generate a dry-run batch bundle only outside the mirror and backup roots:

```bash
python3 -m tushare_mirror mirror-batch-bundle \
  --root "$MIRROR_ROOT" \
  --backup "$MIRROR_BACKUP" \
  --scope low-risk-a-share \
  --start-date 20250201 \
  --end-date 20250228 \
  --max-jobs-per-api 20 \
  --output /tmp/tushare-mirror-batch-bundle-202502 \
  --json
```

The bundle contains `README.md`, `batch_plan.json`, `readiness.json`,
`review.json`, `status.json`, `audit.json`, `stop_policy.json`, and
`commands.sh`. `commands.sh` may include an execution command preview, but it is
commented or guarded with `USER_CONFIRMATION_REQUIRED`. Do not auto-execute it
from scripts, schedulers, shells, or CI. The file exists to make the exact
operator-reviewed command visible before a human decides whether to run it.

Before any user-confirmed batch, run the operator checklist:

```bash
python3 -m tushare_mirror mirror-operator-checklist \
  --root "$MIRROR_ROOT" \
  --backup "$MIRROR_BACKUP" \
  --scope low-risk-a-share \
  --start-date 20250201 \
  --end-date 20250228 \
  --json
```

Review stop policy by scope or category:

```bash
python3 -m tushare_mirror stop-policy --scope low-risk-a-share --json
python3 -m tushare_mirror stop-policy --category financial --json
python3 -m tushare_mirror stop-policy --category intraday --json
python3 -m tushare_mirror stop-policy --category backup --json
```

Inspect schema, quarantine, backup, coverage, and request-volume risk:

```bash
python3 -m tushare_mirror schema-status \
  --root "$MIRROR_ROOT" \
  --json

python3 -m tushare_mirror backup-status \
  --backup "$MIRROR_BACKUP" \
  --json

python3 -m tushare_mirror mirror-coverage-matrix \
  --root "$MIRROR_ROOT" \
  --scope low-risk-a-share \
  --start-date 20250101 \
  --end-date 20250131 \
  --json

python3 -m tushare_mirror request-estimate \
  --scope low-risk-a-share \
  --start-date 20250201 \
  --end-date 20250228 \
  --root "$MIRROR_ROOT" \
  --json
```

Recommended operator workflow:

1. Confirm `mirror-status` has no blocking errors, no token plaintext, and a
   clean restore-check.
2. Review `mirror-audit` for failed jobs, failed validations, and quarantine.
3. Confirm `mirror-next-batch` recommends the intended bounded month.
4. Review `request-estimate` assumptions and risk level.
5. Generate a bundle in `/tmp` or another safe output directory outside the
   mirror and backup roots.
6. Open the bundle JSON files and `commands.sh`; do not execute `commands.sh`.
7. Run `mirror-operator-checklist` and stop if any blocking error remains.
8. If a later human-approved execution happens outside this read-only phase,
   report the run ID, endpoints, date range, planned and executed job counts,
   skipped jobs, failed jobs, validation status, backup ID, restore-check
   status, and next recommended bounded month.

Stop immediately if any of these conditions appear:

- backup is missing, nested under the mirror root, mutated, or fails restore-check
- catalog is missing or cannot be opened
- schema status reports incompatible or pending schema drift
- quarantine rows exist
- validation failures exist
- operator checklist is not ready
- request estimate risk is unacceptable for the intended quota window
- token plaintext appears in any artifact
- an output path is inside the mirror or backup root
- any command would fetch real Tushare data, run `mirror-run`, backfill dates,
  execute a stock loop, enable financial/PIT/object/intraday/compaction
  execution, or implement a loader/scheduler/parallel executor

This hardening flow is still not full mirror automation. It is a local,
read-only review and bundle-generation process around a bounded low-risk batch.
It has no scheduler, no remote backup, no restore-into workflow, no PostgreSQL
loader, no parallel execution, and no automatic transition from recommendation
to execution.

## Batch Execution Safety Suite

This section is for the final infrastructure-only checks before a user-confirmed
February controlled batch. These commands do not fetch real Tushare data, do not
run `mirror-run`, do not backfill dates, do not enable executable endpoints, and
do not run `commands.sh` from a generated bundle. Commands that write files write
only to an explicit user-provided output path outside the mirror and backup
roots.

Use the same durable paths:

```bash
MIRROR_ROOT=/mnt/gw/TuShare
MIRROR_BACKUP=/mnt/gw/TuShare-backup
BUNDLE=/tmp/tushare-mirror-batch-bundle-202502
```

Verify a generated bundle before reviewing it as an execution candidate:

```bash
python3 -m tushare_mirror mirror-batch-bundle-verify \
  --bundle "$BUNDLE" \
  --json
```

The verifier checks `bundle_manifest.json`, required file hashes, required JSON
reports, `README.md`, `commands.sh`, the `USER_CONFIRMATION_REQUIRED` marker,
token hygiene, and whether `commands.sh` is unexpectedly executable. It does not
execute any command in the bundle.

Analyze command previews without running them:

```bash
python3 -m tushare_mirror command-safety-check \
  --file "$BUNDLE/commands.sh" \
  --json
```

The analyzer detects unguarded `mirror-run --execute`, backfill execution,
destructive shell commands, network commands, unsafe output paths, token-like
strings, and other high-risk active commands. A guarded execution preview may be
reported with a warning, but it still requires human confirmation.

Rehearse the batch sequence without executing any step:

```bash
python3 -m tushare_mirror mirror-batch-rehearse \
  --root "$MIRROR_ROOT" \
  --backup "$MIRROR_BACKUP" \
  --bundle "$BUNDLE" \
  --json
```

The rehearsal simulates preflight, review, readiness, batch-plan, operator
checklist, the command that would execute, validation, backup, restore-check, and
post-batch review. It does not call `mirror-run`, fetch, backfill, write catalog
state, or write backup state.

Inspect inferred batch history and the next bounded recommendation:

```bash
python3 -m tushare_mirror mirror-batch-ledger \
  --root "$MIRROR_ROOT" \
  --scope low-risk-a-share \
  --json
```

The ledger is inferred from local catalog runs and coverage. It is not an
execution ledger and does not create ledger rows.

Generate a completion certificate only after a completed bounded batch and clean
backup/restore checks:

```bash
python3 -m tushare_mirror mirror-batch-certificate \
  --root "$MIRROR_ROOT" \
  --backup "$MIRROR_BACKUP" \
  --scope low-risk-a-share \
  --start-date 20250101 \
  --end-date 20250131 \
  --output /tmp/tushare-batch-cert-202501 \
  --json
```

The certificate bundle contains `certificate.json` and `certificate.md`. The
output path must not be inside the mirror root or backup root, and existing
output is refused unless `--overwrite` is provided.

Run failure drills as read-only operator guidance:

```bash
python3 -m tushare_mirror mirror-failure-drill \
  --scenario rate_limited \
  --scope low-risk-a-share \
  --json

python3 -m tushare_mirror mirror-failure-drill \
  --scenario backup_failed \
  --scope low-risk-a-share \
  --json

python3 -m tushare_mirror mirror-failure-drill \
  --scenario schema_incompatible \
  --scope low-risk-a-share \
  --json
```

Supported scenarios include `rate_limited`, `permission_denied`,
`invalid_params`, `schema_incompatible`, `validation_failed`, `backup_failed`,
`restore_check_failed`, `trade_cal_missing`, `token_missing`, and
`disk_space_low`. These drills do not inject failures into the catalog.

Inspect filesystem topology and local capacity:

```bash
python3 -m tushare_mirror path-diagnostics \
  --root "$MIRROR_ROOT" \
  --backup "$MIRROR_BACKUP" \
  --json
```

This reports path existence, local file counts and sizes, parent free bytes,
nested root/backup relationships, and same-device status when available. It does
not write files.

Scan mirror and backup artifacts for accidental token plaintext:

```bash
python3 -m tushare_mirror token-hygiene \
  --path "$MIRROR_ROOT" \
  --json

python3 -m tushare_mirror token-hygiene \
  --path "$MIRROR_BACKUP" \
  --json
```

The scanner reports counts and suspicious paths only. It never prints matched
token-like values. It scans text-like files and SQLite text fields, and skips
binary/raw formats such as Parquet and compressed raw archives.

Decide whether January can promote to the February controlled batch:

```bash
python3 -m tushare_mirror monthly-promotion-checklist \
  --root "$MIRROR_ROOT" \
  --backup "$MIRROR_BACKUP" \
  --scope low-risk-a-share \
  --from-month 202501 \
  --to-month 202502 \
  --json
```

The checklist verifies source-month coverage, backup validity, backup mutation
status, schema/quarantine status, next batch plan availability, request-estimate
risk, operator checklist readiness, optional bundle verification, and explicit
user-confirmation requirements.

Use the aggregate report for a single operator packet:

```bash
python3 -m tushare_mirror mirror-ops-report \
  --root "$MIRROR_ROOT" \
  --backup "$MIRROR_BACKUP" \
  --scope low-risk-a-share \
  --start-date 20250101 \
  --end-date 20250131 \
  --next-start-date 20250201 \
  --next-end-date 20250228 \
  --json
```

The aggregate report includes mirror status, audit, next-batch recommendation,
backup status, schema status, coverage matrix, request estimate, operator
checklist, stop policy, path diagnostics, token hygiene, and monthly promotion
checklist. It does not execute commands.

Recommended operator flow:

1. Run `mirror-status`.
2. Run `mirror-audit`.
3. Run `mirror-next-batch`.
4. Generate or refresh `mirror-batch-bundle`.
5. Run `mirror-batch-bundle-verify`.
6. Run `command-safety-check` on `commands.sh`.
7. Run `mirror-batch-rehearse`.
8. Run `mirror-operator-checklist`.
9. Run `monthly-promotion-checklist`.
10. Obtain explicit user confirmation.
11. Only after confirmation, run the reviewed `mirror-run --execute` command.

Do not run step 11 from `commands.sh` automatically. `commands.sh` is an
inspection artifact, not an executable workflow. It exists so the exact command
can be reviewed, copied deliberately by an operator, and compared against the
bundle manifest and safety reports.

After each user-confirmed batch, report:

- run ID
- batch date range
- endpoints included
- planned, skipped, executed, and failed job counts
- validation status and failed validation IDs
- schema and quarantine status
- backup ID and restore-check status
- token hygiene result
- path diagnostics result
- next recommended bounded month

Stop before user confirmation if any safety report is blocked, if token
plaintext is detected, if the backup is missing or mutated, if restore-check
fails, if schema/quarantine blockers exist, if command safety finds an unguarded
execution command, if the bundle cannot be verified, or if request risk exceeds
the operator's quota window.

This suite is still not full mirror automation. It does not add new executable
endpoints, execute stock loops, enable financial/PIT/object/intraday/compaction
execution, implement PostgreSQL loading, implement remote backup or restore-into,
run a scheduler, or introduce parallel execution. It is a read-only and
file-output safety layer around one bounded low-risk batch.
