# Tushare Mirror Phase 1 Runbook

Last reviewed: 2026-05-31

Phase 1 is a minimal file-lake closed loop for one Tushare endpoint: `daily`.
It is not a full Tushare mirror, does not backfill history, and does not support
minute/tick/financial expansion.

## Scope

Implemented:

- SQLite catalog initialization.
- Endpoint config loading for `daily`.
- Permission probe for one endpoint.
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

## Clean Test Root

```bash
rm -rf /tmp/tushare-mirror-real
```

Only clean explicit temporary roots. Do not delete `data/tushare/` unless you
intend to remove local mirror data.

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
