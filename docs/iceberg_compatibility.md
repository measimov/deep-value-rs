# Iceberg and Delta Compatibility Notes

Last reviewed: 2026-05-31

This document records compatibility requirements for the Tushare file lake. The
first implementation does not need Apache Iceberg or Delta Lake, but the local
catalog should not block a later migration to either table format.

Related design: `docs/tushare-file-lake-layout.md`.

## Goal

Keep the MVP catalog close enough to open table-format concepts that future
migration is a metadata transformation, not a directory rewrite.

The MVP still writes plain Parquet files plus a local catalog. Compatibility
means reserving the same concepts:

- table identity
- schema versions
- partition spec versions
- manifest-like file records
- immutable snapshots
- snapshot lineage
- active file sets

## Required Stable Identifiers

Every logical table/API needs these identifiers in the catalog:

| Field | Purpose |
| --- | --- |
| `table_id` | Stable logical table id, independent from display name. |
| `namespace` | Logical namespace, such as `tushare.stock` or `tushare.financial`. |
| `api_name` | Tushare endpoint name. Multiple APIs may map to one logical table only if explicitly configured. |
| `schema_id` | Versioned schema identifier. |
| `partition_spec_id` | Versioned partition spec identifier. |
| `snapshot_id` | Immutable committed file set. |
| `parent_snapshot_id` | Snapshot lineage. |
| `sequence_number` | Monotonic ordering for commits. |

Do not use filesystem paths as stable table identity. Paths can change during
compaction, backup restore, or future table-format migration.

## Iceberg Concept Mapping

| Local catalog concept | Iceberg equivalent | Notes |
| --- | --- | --- |
| `endpoints.table_id` | table UUID/name | Use one table per logical API unless explicitly merged. |
| `schemas` | Iceberg schema list | Keep field ids if possible in a later phase. MVP stores names and logical types. |
| `partition_specs` | Iceberg partition specs | Keep spec id and transform metadata, including bucket fields/count. |
| `files` | data file entries | Store path, format, partition values, file size, row count, and checksums. |
| `snapshot_files` | manifest entries | MVP can store directly in SQLite; future export can write manifest lists. |
| `snapshots` | Iceberg snapshots | Keep parent snapshot and operation type. |
| `compaction_runs` | rewrite data files operation | Old files superseded, new files active under a new snapshot. |
| `validation_runs` | post-commit validation metadata | Not an Iceberg primitive, but useful operational metadata. |
| `quarantine_files` | non-table artifacts | Do not include in Iceberg snapshots. |

## Delta Concept Mapping

| Local catalog concept | Delta-like equivalent | Notes |
| --- | --- | --- |
| `snapshots.sequence_number` | Delta log version | Use monotonic sequence numbers to support ordered replay. |
| `files status=active` | add file actions | Active files become add actions. |
| `files status=superseded/compacted` | remove file actions | Physical deletion remains retention-based. |
| `schemas` | metadata schema string | Store schema JSON and normalized logical types. |
| `partition_specs` | partition columns/config | Delta has less partition evolution structure than Iceberg; keep local spec ids anyway. |
| `commit_info` fields on snapshots | commitInfo | Record operation, run id, user/tool, and validation result. |

Delta compatibility primarily needs ordered commits and add/remove file actions.
The local catalog should store enough information to emit such actions later.

## Snapshot Requirements

Every snapshot row should include:

```text
snapshot_id
table_id or global_scope
parent_snapshot_id
sequence_number
schema_id
partition_spec_id
operation
created_at
created_by
run_id
validation_run_id
summary_json
```

Operations:

```text
append
overwrite
replace_partitions
compact
schema_change
restore
```

Global snapshots should reference table snapshots rather than directly listing
all files. Table snapshots list files through `snapshot_files`; global snapshots
use `snapshot_refs` or an equivalent `global_snapshot_tables` table.

## File Manifest Requirements

Every active data file should record at least:

```text
file_id
table_id
added_by_snapshot_id
content_type        # lake | object_index | derived; raw/object blobs are auxiliary
file_format         # parquet | jsonl.zst | pdf | html | ...
relative_path
partition_spec_id
partition_values_json
record_count
file_size_bytes
sha256
schema_id
sort_order_json
created_at
run_id
job_key
status
```

Only Parquet table files such as `lake/`, `derived/`, and object-index
Parquet files become future Iceberg/Delta data files. Raw JSONL.zst files and
physical PDF/HTML objects are auxiliary artifacts managed by the local catalog
and backup manifests; they do not enter Iceberg data manifests directly.

For future Iceberg export, also reserve nullable fields:

```text
column_sizes_json
value_counts_json
null_value_counts_json
lower_bounds_json
upper_bounds_json
split_offsets_json
```

MVP does not have to populate column statistics beyond what the writer provides,
but the schema should leave room for them.

## Partition Spec Requirements

Partition specs must be versioned. Changing bucket count, switching minute bars
from daily buckets to monthly buckets, or changing event-date fallback creates a
new `partition_spec_id`.

Example:

```yaml
partition_spec_id: minute_day_bucket_v1
transforms:
  - source: trade_time
    transform: day
    name: trade_date
  - source: ts_code
    transform: bucket
    num_buckets: 32
    name: bucket
sort_order:
  - trade_date
  - ts_code
  - trade_time
```

Never rewrite old files only to match a new partition spec. New snapshots may
contain files written under multiple partition specs, and readers must resolve
partition values through the catalog.

## Schema Evolution Requirements

Iceberg-style evolution works best when fields have stable ids. Tushare does not
provide field ids, so MVP should at least preserve:

- original field name
- normalized field name
- logical type
- physical type
- nullable flag
- first seen schema id
- deprecated/missing flag

Future migration can assign synthetic field ids based on approved schema history.
Renames must be manually approved before assigning continuity between old and
new names.

## Directory Layout Constraints

The current directory layout is allowed to be human-readable, but readers must
not discover tables by walking directories alone. The catalog is authoritative.

Allowed:

```text
lake/market=a/domain=stock/api=daily/year=2025/month=05/part-000.parquet
```

Required:

- File is active only if listed by the selected snapshot.
- Partition values come from `files.partition_values_json`, not just path
  parsing.
- Compaction can move files without changing logical table identity.

## Migration Path

A later Iceberg migration can follow this sequence:

1. Freeze writes for one table.
2. Validate the latest table snapshot.
3. Export local `schemas` into Iceberg schema metadata.
4. Export local `partition_specs` into Iceberg partition specs.
5. Export active `files` rows into data-file manifest entries.
6. Create one Iceberg snapshot with `parent_snapshot_id` from local lineage where
   applicable.
7. Compare file count, row count, and checksums between local catalog and Iceberg
   metadata.
8. Resume writes through the chosen table-format writer or continue dual-writing
   metadata during migration.

A later Delta migration is similar but emits ordered add/remove actions from
snapshot lineage.

## Non-Goals for MVP

- No Iceberg Java runtime.
- No Delta transaction log writer.
- No object-store locking protocol.
- No delete files or row-level deletes.
- No multi-writer concurrency beyond local catalog transaction locking.

The MVP must still reserve the metadata fields above so these non-goals remain
implementation choices, not blockers.
