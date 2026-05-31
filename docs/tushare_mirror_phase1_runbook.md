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

Not implemented in Phase 1:

- Full mirror endpoint coverage.
- Historical backfill loops.
- Minute/tick large-volume sync.
- Financial PIT derived loaders.
- PostgreSQL derived loader.
- Compaction executor.
- Backup/restore executor.
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
```

The command lists active lake files for the latest `daily` snapshot.

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

## Real Smoke Test Commands

Run only this single-date sequence for Phase 1 delivery smoke testing:

```bash
python -m tushare_mirror --root /tmp/tushare-mirror-real init-catalog
python -m tushare_mirror --root /tmp/tushare-mirror-real probe --api daily
python -m tushare_mirror --root /tmp/tushare-mirror-real fetch --api daily --params '{"trade_date":"20250102"}'
python -m tushare_mirror --root /tmp/tushare-mirror-real validate --snapshot latest
python -m tushare_mirror --root /tmp/tushare-mirror-real list-files --api daily --snapshot latest
```

Do not loop over dates, add endpoints, or run full mirror jobs during Phase 1.
