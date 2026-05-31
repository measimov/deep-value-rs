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

`--trading-days-only` reads local `trade_cal` lake data. It does not request
Tushare. If no local `trade_cal` latest snapshot exists, the planner fails and
asks you to fetch a small calendar range first:

```bash
python3 -m tushare_mirror --root /tmp/tushare-backfill fetch \
  --api trade_cal \
  --params '{"exchange":"SSE","start_date":"20250101","end_date":"20250131"}'

python3 -m tushare_mirror --root /tmp/tushare-backfill backfill-plan \
  --api daily \
  --start-date 20250101 \
  --end-date 20250110 \
  --trading-days-only \
  --max-jobs 10
```

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
