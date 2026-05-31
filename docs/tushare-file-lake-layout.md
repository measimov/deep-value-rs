# Tushare File Lake Layout Design

Last reviewed: 2026-05-31

This document designs the file-backed storage layout for a broad Tushare mirror.
It complements the existing PostgreSQL-backed strategy cache. The target is to
store as much permitted Tushare data as possible while keeping storage size,
query efficiency, backup, and resumability under control.

Official references:

- Tushare data index: <https://tushare.pro/document/2?doc_id=209>
- Tushare HTTP protocol: <https://tushare.pro/document/2?doc_id=40>
- Tushare point and frequency table: <https://tushare.pro/document/1?doc_id=290>
- Tushare permission matrix: <https://tushare.pro/document/1?doc_id=108>
- Iceberg/Delta compatibility notes: `docs/iceberg_compatibility.md`

## Storage Decision

Use a file lake as the durable source of truth:

- Primary structured format: Parquet with Zstd compression.
- Raw response archive: JSON or JSONL compressed with Zstd.
- Documents and binaries: original object files plus metadata tables.
- Catalog/checkpoint: `CatalogStore` with SQLite as the MVP backend for
  endpoints, runs, jobs, files, schemas, checkpoints, and snapshots.
- PostgreSQL remains a derived serving store for strategy queries, not the
  canonical full mirror.

This avoids forcing minute bars, documents, news, and long-tail special data into
PostgreSQL tables. It also keeps backups file-oriented and cheap.

## Permission Boundary

A 15000-point token should be treated as a high-tier regular/special-data token,
not as proof that every Tushare product is available. Tushare documents ordinary
point thresholds separately from independent paid permissions. Historical minute
data, realtime data, Hong Kong/US data, news, announcements, and some document
products can require additional permissions.

The implementation should therefore be probe-driven:

1. Keep every candidate endpoint in `_catalog/endpoints/*.yaml`.
2. Run a minimal permission probe per endpoint.
3. Persist probe results in the catalog.
4. Schedule full backfill only for endpoints proven accessible.

## Volume Classes

Every endpoint should be assigned one volume class. The class determines
partitioning, file size target, and default compaction behavior.

| Class | Expected size | Typical shape | Default layout |
| --- | ---: | --- | --- |
| `S0_STATIC` | tiny to small | reference lists, code tables, exchange metadata | snapshot-date partition |
| `S1_EVENT` | small to medium | sparse events keyed by announcement/date/code | year/month by event date |
| `D1_DAILY_NARROW` | medium to large | one row per code per trading day | year/month, sorted by date/code |
| `D2_DAILY_WIDE` | large | dense daily rows with many columns or many markets | year/month plus optional bucket |
| `F1_PERIOD` | medium | statement/report-period rows | period-year/period |
| `F2_PERIOD_EXPANDED` | medium to large | statements with multiple items/segments per company | period-year/period plus optional type |
| `H1_CONSTITUENT` | medium | index/fund/holding constituents by date/period | code or family plus year/month |
| `V1_MINUTE` | very large | 1/5/15/30/60 minute bars | configurable frequency/date/hash-bucket layout |
| `V2_TICK` | extremely large | tick/order/realtime-like data | mandatory trade date + code hash bucket |
| `X1_TEXT` | medium to large | news, research, announcements metadata | publish year/month |
| `X2_OBJECT` | large binary | PDFs, HTML, images, source files | content-addressed object plus publish-date index |
| `M1_MACRO` | small to medium | macro indicators by date | indicator family plus year |

## Global Directory Layout

```text
data/tushare/
  _catalog/
    catalog.sqlite
    endpoints/
      stock.yaml
      financial.yaml
      index.yaml
      text_objects.yaml
    schemas/
    snapshots/

  _tmp/
    run_id=<run_id>/

  _quarantine/
    run_id=<run_id>/

  raw/
    api=daily/ingest_date=20260531/job=<job_key>.jsonl.zst

  lake/
    market=a/domain=stock/api=daily/year=2025/month=05/part-000.parquet

  objects/
    sha256/<first2>/<next2>/<sha256>.<ext>

  derived/
    strategy/
      table=local_daily_prices/snapshot=<snapshot_id>/part-000.parquet
```

Rules:

- `raw/` stores the exact Tushare response envelope where practical.
- `lake/` stores normalized row tables with all requested fields preserved.
- `objects/` stores binaries and large text payloads outside row tables.
- `derived/` stores curated tables for strategies, backtests, and export.
- Files are immutable after commit. Corrections write new files and update the
  catalog snapshot.
- Target Parquet file size after compression: 128-512 MB for high-volume data,
  16-128 MB for low-volume data. Compact tiny files later rather than writing a
  file per row/code/day.

## Standard Row Metadata

Every `lake/` table should add these metadata columns:

| Column | Purpose |
| --- | --- |
| `_api_name` | Tushare endpoint name |
| `_params_hash` | stable hash of request params |
| `_row_hash` | stable hash of returned row |
| `_fetched_at` | ingestion timestamp |
| `_source_fields` | ordered Tushare field list hash or schema id |
| `_job_key` | catalog job id |
| `_run_id` | ingestion run id |

For query columns, keep the original Tushare field names. Do not translate
fields in bronze/lake storage.
`_snapshot_id` is intentionally not a bronze/lake row metadata column. Snapshot
membership belongs to the catalog. Derived files and PostgreSQL serving tables
may record `source_snapshot_id` or `loaded_from_snapshot_id` because they are
materialized from a committed snapshot.


## Hashing and Canonicalization

Stable identifiers must not depend on Python dict ordering, local timezone, or
incidental ingestion metadata.

`params_hash`:

- Build from canonical JSON with sorted keys and compact separators.
- Exclude token and any transport-only values.
- Preserve the difference between missing, null, and empty string unless an
  endpoint config explicitly normalizes them.
- Normalize Tushare dates to `YYYYMMDD` and datetimes to UTC ISO-8601.

`row_hash`:

- Hash source data only, plus API/schema context. Do not include `_fetched_at`,
  `_run_id`, `_job_key`, or other ingestion metadata.
- Use the active schema field order or a documented canonical field order.
- Encode null as a dedicated sentinel.
- Normalize integers, floats, decimals, dates, and datetimes before hashing.
- Include `api_name` and `schema_id` to avoid collisions across endpoints.

`schema_id`:

- Include original field names, normalized field names, logical types, and
  nullable flags.
- Do not rely only on normalized names because different Tushare fields may
  normalize to the same string.

`token_hash`:

- Never store token plaintext.
- Use HMAC-SHA256 with a local secret/salt, not bare SHA256.
- Use `token_hash` only to isolate permission probe results. It is not an
  authentication credential.

`job_key`:

- Derive from `api_name`, canonical params, canonical field list, endpoint
  version, and partition spec id.
- Exclude token and retry attempt counters.

`table_id` and `partition_spec_id`:

- Treat as stable catalog identifiers, preferably configured or generated once
  and stored. Do not derive them from mutable filesystem paths.


## Partition Templates

### Static and Metadata APIs (`S0_STATIC`)

Template:

```text
lake/market=<market>/domain=<domain>/api=<api>/snapshot_date=<YYYYMMDD>/part-000.parquet
```

Use for:

- A-share: `stock_basic`, `stock_company`, `namechange`, `hs_const`,
  concept/classification lists, ST/security metadata, historical name/code
  reference APIs, exchange calendars when stored as full snapshots.
- HK/US: `hk_basic`, `us_basic`, `hk_tradecal`, `us_tradecal`.
- Fund/ETF/index/future/option/bond/forex basics: `fund_basic`,
  `fund_company`, `index_basic`, `fut_basic`, `opt_basic`, `cb_basic`,
  `bond_basic`, `fx_obasic`.

Rationale: these tables are small and are usually read as complete dimensions.
Snapshot-date partitioning preserves historical revisions without rewriting old
files.

### Sparse Event APIs (`S1_EVENT`)

Template:

```text
lake/market=<market>/domain=<domain>/api=<api>/event_year=<YYYY>/event_month=<MM>/part-000.parquet
```

Use the most natural event date in this order:

1. `ann_date`
2. `trade_date`
3. `end_date`
4. `f_ann_date`
5. `_fetched_at` date if the API has no explicit event date

Use for:

- A-share actions/reference: `dividend`, `repurchase`, pledge detail/stat
  endpoints, block trades, shareholder changes, management holdings, IPO/new
  shares, top-list/top-inst style event data.
- Fund events: fund dividends, fund share changes, issue/listing events.
- Convertible bond events: call, issue, conversion, redemption-like events.

Rationale: event queries normally start from a date range. Event partitioning
keeps backups and incremental pulls append-friendly.

### Daily Market Data (`D1_DAILY_NARROW`, `D2_DAILY_WIDE`)

Template:

```text
lake/market=<market>/domain=<domain>/api=<api>/year=<YYYY>/month=<MM>/part-000.parquet
```

Sort order inside each file:

```text
trade_date, ts_code
```

Optional bucket when one month exceeds the target file size:

```text
lake/market=<market>/domain=<domain>/api=<api>/year=<YYYY>/month=<MM>/bucket=<00-31>/part-000.parquet
```

Use for:

- A-share daily bars and daily indicators: `daily`, `daily_basic`,
  `adj_factor`, `stk_limit`, `suspend_d`, `moneyflow`, daily chip/cost
  distribution style APIs, daily top/limit-board style APIs.
- Index daily/weekly/monthly and valuation: `index_daily`, index weight/date
  series when not modeled as constituents, index indicators.
- Fund/ETF daily/weekly/monthly/NAV series.
- Futures/options/bonds/convertible bonds daily bars, settlement, holdings, and
  inventory-like daily series.
- HK/US daily bars, adjusted bars, and adjustment factors.
- Forex daily series.

Rationale: a monthly partition avoids creating one small file per trading day
for ordinary daily data. For point-in-time daily queries, scanning one monthly
partition is cheap with Polars or DuckDB, especially when files are sorted and
column-pruned.

### Financial Statement and Indicator APIs (`F1_PERIOD`)

Template:

```text
lake/market=<market>/domain=financial/api=<api>/period_year=<YYYY>/period=<YYYYMMDD>/part-000.parquet
```

Use for:

- A-share: `income`, `income_vip`, `balancesheet`, `balancesheet_vip`,
  `cashflow`, `cashflow_vip`, `fina_indicator`, `fina_indicator_vip`,
  `fina_audit`, `disclosure_date`, `forecast`, `forecast_vip`, `express`,
  `express_vip`.
- HK/US: `hk_income`, `hk_balancesheet`, `hk_cashflow`,
  `hk_fina_indicator`, `us_income`, `us_balancesheet`, `us_cashflow`,
  `us_fina_indicator`.

Rationale: most financial queries are period-based or point-in-time joins from a
reporting period. `period=<YYYYMMDD>` maps directly to Tushare financial params
and keeps checkpointing simple.

### Expanded Period APIs (`F2_PERIOD_EXPANDED`)

Template:

```text
lake/market=<market>/domain=financial/api=<api>/period_year=<YYYY>/period=<YYYYMMDD>/type=<type>/part-000.parquet
```

Use for:

- `fina_mainbz` / `fina_mainbz_vip`
- fund portfolio holdings
- segment, business-line, or item-expanded report APIs

Rationale: these APIs can have many rows per company and period. Adding `type`
keeps common queries from scanning unrelated rows.

### Constituents and Holdings (`H1_CONSTITUENT`)

Template for index/fund constituents:

```text
lake/market=<market>/domain=<domain>/api=<api>/family=<family_or_code>/year=<YYYY>/month=<MM>/part-000.parquet
```

Examples:

```text
lake/market=a/domain=index/api=index_weight/family=000300.SH/year=2025/month=05/part-000.parquet
lake/market=a/domain=fund/api=fund_portfolio/family=<fund_code>/period_year=2025/period=20250331/part-000.parquet
```

Use for:

- `index_weight`, index constituents/members.
- fund portfolio, fund holdings, ETF constituents.
- northbound/southbound holding and constituent APIs where the natural query key
  is a market/index/fund plus a date.

Rationale: these data sets are larger than metadata but much smaller than
minute data. Partition by the portfolio/index identity only when that identity
is a common query key and cardinality is moderate.

### Minute Bars (`V1_MINUTE`)

Minute bars need a configurable partition strategy because the two dominant
queries pull in opposite directions:

- single-stock multi-year history wants fewer files and code-pruned buckets;
- one-day all-market intraday cross sections want date-local files.

Supported strategies:

Strategy A: day partition, all-market file.

```text
lake/market=<market>/domain=minute/api=<api>/freq=<freq>/year=<YYYY>/month=<MM>/trade_date=<YYYYMMDD>/part-000.parquet
```

Strategy B: day partition plus code hash bucket.

```text
lake/market=<market>/domain=minute/api=<api>/freq=<freq>/year=<YYYY>/month=<MM>/trade_date=<YYYYMMDD>/bucket=<00-NN>/part-000.parquet
```

Strategy C: month partition plus code hash bucket.

```text
lake/market=<market>/domain=minute/api=<api>/freq=<freq>/year=<YYYY>/month=<MM>/bucket=<00-NN>/part-000.parquet
```

Default for MVP full-mirror backfill: Strategy B with `bucket_count=32` for
`1min`, `bucket_count=16` for lower frequencies. It matches date-window
backfill, makes failed date jobs easy to retry, and reduces single-stock scans
by bucket pruning. If single-stock multi-year research becomes dominant,
Strategy C should be added as a derived compacted table, not by changing the raw
bronze layout in place.

Bucket key:

```text
bucket = stable_hash(ts_code) % bucket_count
```

Sort order inside each file:

```text
trade_date, ts_code, trade_time
```

Forbidden default:

```text
market=a/api=stk_mins/ts_code=000001.SZ/trade_date=20250520.parquet
```

One-stock-one-day minute files create millions of tiny files and are allowed
only as a temporary export format, never as canonical lake storage.

### Tick and Order-Level Data (`V2_TICK`)

Tick and order-level endpoints must use hash buckets. This is not optional.

Template:

```text
lake/market=<market>/domain=tick/api=<api>/year=<YYYY>/month=<MM>/trade_date=<YYYYMMDD>/bucket=<000-127>/part-000.parquet
```

Bucket key:

```text
bucket = stable_hash(ts_code) % bucket_count
```

Defaults:

- `bucket_count=128` for first implementation.
- Increase to `256` or higher when any compressed file exceeds 1 GB or the p95
  bucket file exceeds 512 MB.
- Target compressed file size: 128-512 MB.
- Files below 16 MB become compaction candidates when a partition has more than
  32 tiny files.
- One-code-one-day canonical storage is forbidden.

Rationale: tick-level data is too large for monthly partitions and too expensive
to store as one file per code/day. Date plus hash bucket gives bounded file
sizes, stable backup behavior, and predictable validation work.

### News, Announcements, and Research Metadata (`X1_TEXT`)

Template:

```text
lake/market=<market>/domain=text/api=<api>/publish_year=<YYYY>/publish_month=<MM>/part-000.parquet
```

Use for:

- news APIs, CCTV/newswire-like feeds, research metadata, announcement metadata,
  PDF index rows, and LLM/corpus metadata.

Large article bodies can be stored either as compressed JSONL rows or object
files if they are large enough to harm Parquet row groups.

### Documents and Binary Objects (`X2_OBJECT`)

Physical object template:

```text
objects/sha256/<first2>/<next2>/<sha256>.<ext>
```

Logical object index template:

```text
lake/market=<market>/domain=object_index/api=<api>/publish_year=<YYYY>/publish_month=<MM>/part-000.parquet
```

The object index table should include:

- `source_id`
- `title`
- `publish_time`
- `ts_code` when available
- `source_url`
- `object_path`
- `content_type`
- `sha256`
- `size_bytes`
- `fetched_at`
- original Tushare fields

Rationale: PostgreSQL and Parquet are both poor places to store large PDFs. Keep
binary objects as content-addressed files and query their metadata through the
object index.

### Macro and Alternative Data (`M1_MACRO`)

Template:

```text
lake/market=global/domain=macro/api=<api>/indicator_family=<family>/year=<YYYY>/part-000.parquet
```

Use for:

- China macro: GDP, CPI, PPI, PMI, money supply, social financing, industry
  output, trade, credit, rates.
- Global rates and FX/macro series.
- Low-frequency economic tables.

Rationale: macro APIs are usually small but have heterogeneous schemas. A
family partition prevents unrelated indicators from being packed into one wide
table.

## Endpoint Family Matrix

This matrix maps the official Tushare data index families to default volume
classes and layouts. Exact endpoint names should be maintained in
`_catalog/endpoints/*.yaml`; this table is the storage policy.

| Official family | Representative APIs | Volume class | Layout |
| --- | --- | --- | --- |
| A-share basic/reference | `stock_basic`, company/profile/name/code/history/reference lists | `S0_STATIC` | snapshot-date |
| A-share trading calendar | `trade_cal` | `S0_STATIC` or `D1_DAILY_NARROW` | snapshot-date or year |
| A-share daily prices | `daily`, weekly/monthly variants | `D1_DAILY_NARROW` | year/month |
| A-share daily indicators | `daily_basic`, money flow, limit/suspend/chip style daily APIs | `D2_DAILY_WIDE` | year/month, optional bucket |
| A-share minute bars | 1/5/15/30/60 minute endpoints | `V1_MINUTE` | freq + trade_date |
| A-share financial statements | income/balance/cashflow/indicator/audit/disclosure | `F1_PERIOD` | period |
| A-share financial forecasts and reports | forecast/express/main-business composition | `F1_PERIOD` or `F2_PERIOD_EXPANDED` | period, optional type |
| A-share corporate actions/events | dividend, repurchase, pledge, IPO/new shares, block trades | `S1_EVENT` | event year/month |
| A-share shareholders/holdings | top holders, float holders, holder number/trades | `S1_EVENT` or `F1_PERIOD` | event or period |
| A-share money/margin/connect | margin, north/southbound, HSGT, financing data | `D1_DAILY_NARROW` | year/month |
| A-share special data | limit board, top list, themes, concepts, hot data | `S1_EVENT` or `D1_DAILY_NARROW` | event or year/month |
| Index | index basics, daily series, weights, constituents | `S0_STATIC`, `D1_DAILY_NARROW`, `H1_CONSTITUENT` | by api shape |
| Fund/ETF | basics, NAV, daily bars, portfolios, dividends, shares | `S0_STATIC`, `D1_DAILY_NARROW`, `H1_CONSTITUENT`, `S1_EVENT` | by api shape |
| Futures | basics, trade calendar, daily bars, settlement, holdings, warehouse receipts | `S0_STATIC`, `D1_DAILY_NARROW` | year/month |
| Options | basics, daily bars, settlement, minute bars | `S0_STATIC`, `D1_DAILY_NARROW`, `V1_MINUTE` | by api shape |
| Bonds/convertibles | basics, daily bars, issues, calls/redemptions | `S0_STATIC`, `D1_DAILY_NARROW`, `S1_EVENT` | by api shape |
| Forex | forex basics and daily series | `S0_STATIC`, `D1_DAILY_NARROW` | year/month |
| Hong Kong stocks | `hk_basic`, `hk_daily`, `hk_daily_adj`, `hk_adjfactor`, HK financials | `S0_STATIC`, `D1_DAILY_NARROW`, `F1_PERIOD` | by api shape |
| US stocks | `us_basic`, `us_daily`, `us_daily_adj`, `us_adjfactor`, US financials | `S0_STATIC`, `D1_DAILY_NARROW`, `F1_PERIOD` | by api shape |
| Macro | GDP/CPI/PPI/PMI/rates/credit/money/global macro | `M1_MACRO` | family + year |
| News/research/announcements | news, announcements, research, report/document metadata | `X1_TEXT`, `X2_OBJECT` | publish year/month + objects |
| Realtime/tick-like data | realtime quotes, tick/order-style feeds | `V2_TICK` | trade_date + bucket |

## File Size and Compaction Policy

Initial write policy:

- Never write a separate file for each code/day unless the endpoint is a binary
  object.
- Write one Parquet file per partition when the result is below 512 MB.
- Split into `part-000`, `part-001`, ... when one partition exceeds 512 MB.
- Use Zstd compression. Start with compression level 3; tune only after
  measuring.
- Use stable sort keys so row-group statistics help date/code filters.

Compaction policy:

- If a partition has more than 32 files below 16 MB, compact it.
- If a file exceeds 1 GB compressed, split it by bucket or part number.
- Do not rewrite old files during normal sync; write compacted files under a new
  snapshot and mark the old files superseded or compacted in the catalog.

## Catalog Requirements

The catalog is part of the storage system, not a convenience log. It must keep
run, job, file, snapshot, and checkpoint as separate concepts.

MVP decision: implement a `CatalogStore` abstraction with SQLite as the first
backend. PostgreSQL must not become the canonical file-lake checkpoint. Existing
`deep_value.tushare_sync_jobs` can be imported as legacy run/job evidence for
already-fetched PostgreSQL data, but new mirror jobs use the file-lake catalog.

### Catalog Transaction and SQLite Locking

SQLite MVP semantics:

- Local MVP uses a single writer. Multiple readers are allowed through SQLite
  WAL mode.
- Every catalog mutation runs inside an explicit SQLite transaction.
- Snapshot commit, file activation, and checkpoint advancement are one catalog
  transaction.
- Rollback leaves staged files non-active and leaves
  `checkpoint_state.last_committed_cursor` unchanged.
- Catalog schema has `catalog_schema_version`; migrations are ordered and
  idempotent.
- Do not copy a live SQLite file directly while a writer is active. Backups must
  either pause writers and checkpoint WAL, or use the SQLite backup API.
- A catalog backup includes the main database, WAL checkpoint state, endpoint
  YAML revision, and the active snapshot id being backed up.

Minimum catalog tables:

| Table | Purpose |
| --- | --- |
| `endpoints` | API name, family, domain, market, permission class, volume class, default fields, probe config, PIT policy, partition strategy. |
| `partition_specs` | Named partition spec with `partition_spec_id`, template, bucket fields, bucket count, sort keys, and evolution metadata. |
| `permission_probes` | Token-hash scoped probe results, status, error class, probe params, `valid_until`, retry policy. |
| `ingestion_runs` | One user/system invocation; owns many jobs and staged files. |
| `jobs` | One fetch unit for an API plus params/fields; owns raw/lake/object files. |
| `checkpoint_state` | Durable resume cursor per endpoint/partition/window/page. |
| `schemas` | API schema versions, ordered fields, logical types, physical types, schema hash. |
| `schema_changes` | Schema diff decisions between old and new schema versions. |
| `files` | Manifest-like table for raw/lake/object/derived files, status, checksum, counts, partition values, and `created_snapshot_id` / `added_by_snapshot_id`. |
| `snapshots` | Immutable active file-set view, scoped globally or to a table/API. |
| `snapshot_files` | Many-to-many mapping between table snapshots and files. Historical reads use this table, not `files.created_snapshot_id`. |
| `snapshot_refs` | Mapping from a global snapshot to table/API snapshots: `global_snapshot_id`, `table_id`, `table_snapshot_id`. |
| `compaction_runs` | Planned/executed compactions, old files, new files, record-count reconciliation. |
| `validation_runs` | Validation command result and per-file failures. |
| `backup_manifests` | Backup manifest metadata, target, file set, catalog checkpoint, validation result. |
| `quarantine_files` | Files rejected by schema/validation/parse checks with reason and source job. |
| `postgres_loads` | Derived PostgreSQL loads by snapshot/table/schema. |

Concept boundaries:

- `ingestion_run`: a command execution. It can fail without changing active data.
- `job`: one deterministic fetch unit, keyed by API, params, fields, and schema
  policy. A run owns many jobs.
- `file`: a physical artifact with checksum and status.
- `snapshot`: a committed logical view of active files. Readers only use
  snapshots.
- `checkpoint_state`: resume state, not proof that data is queryable.

File/snapshot relationship:

- `files.created_snapshot_id` or `files.added_by_snapshot_id` records the first
  snapshot that introduced a file.
- `snapshot_files(snapshot_id, file_id)` is the authoritative membership rule
  for historical reads.
- One physical file can be referenced by multiple snapshots.
- A file status describes lifecycle relative to latest/retention, not ownership
  by exactly one snapshot.

File statuses:

```text
staged | current | superseded | compacted | quarantined | deleted_pending | deleted | missing
```

Snapshot scopes:

- Table/API-level snapshots are the default commit unit for backfill.
- A global snapshot pins a consistent collection of table snapshots for
  cross-table backtests and derived PostgreSQL loads.
- Global snapshots use `snapshot_refs` to point to table snapshots instead of
  directly listing every file.
- `latest` resolves to the newest successful global snapshot when present;
  otherwise it resolves to the newest table snapshot for single-API reads.

## Staged Write and Snapshot Commit

All writes use staged commit. Direct writes into active lake paths are forbidden.

Commit protocol:

1. Create `ingestion_runs` row with status `running` and a `run_id`.
2. Write files under `_tmp/run_id=<run_id>/...`.
3. Close files and fsync the containing directory where the filesystem supports
   it.
4. Compute `sha256`, `size_bytes`, `record_count`, `source_item_count`,
   `raw_event_count`, and `error_event_count` where applicable.
5. Validate that Parquet footer is readable and schema id exists.
6. Insert `files` rows with `status='staged'` and `active=false`.
7. Insert or update `jobs` and in-run attempt progress. Do not advance
   `checkpoint_state.last_committed_cursor` yet.
8. Run validation for the staged file set.
9. If validation passes, move files into final relative paths or atomically
   rename the staging directory into place.
10. Create a table/API snapshot with `snapshot_files` rows and mark files
    `active`.
11. Optionally create a global snapshot that references table snapshots through
    `snapshot_refs`.
12. Advance `checkpoint_state.last_committed_cursor` inside the same successful
    catalog transaction.
13. Mark the run `succeeded`.

Failure behavior:

- If any validation fails, do not create an active snapshot.
- If validation fails, `checkpoint_state.last_committed_cursor` must not move.
- Staged files remain under `_quarantine/run_id=<run_id>/...` or are deleted
  according to retention policy.
- Catalog-active files must always exist. `validate --all-active` enforces this.
- Query code must only read active snapshots, never `_tmp` or `staged` files.
- Compaction creates a new snapshot. Old files become `superseded` or `compacted`
  but are not physically deleted until retention cleanup.

## Schema Governance

Schema handling is governance, not passive logging.

Schema id:

```text
schema_id = stable_hash(api_name, normalized field names, logical types)
```

Rules:

| Change | Default behavior |
| --- | --- |
| Column added | Allow automatically. New column is nullable for old files. |
| Field order changed | Allow. Readers union by name. |
| Column missing from a non-empty response | Write warning in `schema_changes`; allow if endpoint config marks it optional. |
| Empty response | Use endpoint-configured schema. If unknown, record empty raw response and mark job `empty_schema_pending`. |
| Type widening | Allow only for configured safe changes, such as int to float or int to string. |
| Type narrowing | Reject active commit; write staged output to quarantine. |
| Semantic type change | Reject active commit. Example: date string becomes free text. |
| Rename | Never auto-approve. Create `schema_changes` row with `change_type='possible_rename'` and wait for manual approval. |
| Unknown incompatible change | Reject active commit and quarantine. |

`schema_changes` fields:

```text
change_id
api_name
old_schema_id
new_schema_id
change_type
details_json
approved
approved_by
approved_at
detected_at
```

Readers must support union-by-name across schema versions. Missing columns are
returned as null. Incompatible schema files are excluded from active snapshots.

## Permission Probe Design

Every endpoint in the endpoint catalog must include minimal probe params and
probe fields.

CLI shape:

```bash
python -m tushare_mirror probe --all
python -m tushare_mirror probe --api daily
python -m tushare_mirror probe --family stock
```

Probe statuses:

```text
accessible
empty_but_accessible
permission_denied
rate_limited
invalid_endpoint
invalid_params
network_error
server_error
unknown_error
```

Rules:

- Store only `token_hash`, never the token value.
- Probe results have `valid_until`. Default validity: 7 days for successful
  probes, 1 day for rate-limited/network/server errors, 30 days for
  permission-denied unless manually retried.
- Backfill can schedule only `accessible` endpoints by default.
- `empty_but_accessible` can be scheduled only when endpoint config has
  `allow_empty_probe: true`.
- `permission_denied`, `invalid_endpoint`, and `invalid_params` do not retry in
  a tight loop.
- `rate_limited` uses exponential backoff and does not downgrade endpoint
  permission.
- If a previously accessible endpoint becomes inaccessible, existing active
  data remains valid, new jobs are blocked, and the endpoint is marked
  `access_stale` until a later successful probe.

Endpoint config example:

```yaml
api_name: daily
family: stock
market: a
volume_class: D1_DAILY_NARROW
probe:
  params:
    trade_date: '20250102'
  fields:
    - ts_code
    - trade_date
    - close
  allow_empty_probe: false
permission:
  point_threshold: 120
  independent_permission: false
partition_spec_id: daily_month_v1
```

## Raw Response Archive

Raw archive should preserve enough information to debug Tushare behavior without
leaking credentials.

Default for paginated endpoints: one raw JSONL.zst file per job. Each line is an
event envelope for one request page or one structured error.

Raw event fields:

```json
{
  "run_id": "...",
  "job_key": "...",
  "api_name": "daily",
  "params": {"trade_date": "20250102", "limit": "5000", "offset": "0"},
  "fields": "ts_code,trade_date,close",
  "token_hash": "...",
  "fetched_at": "2026-05-31T12:00:00Z",
  "http_status": 200,
  "tushare_code": 0,
  "tushare_msg": null,
  "response_fields": ["ts_code", "trade_date", "close"],
  "items": [["000001.SZ", "20250102", 11.1]],
  "has_more": false,
  "page_index": 0
}
```

Rules:

- Save successful responses and structured error responses.
- Raw counts are separate from lake counts: `raw_event_count` counts JSONL
  envelopes, `source_item_count` counts Tushare `items`, `error_event_count`
  counts structured errors, and `record_count` counts normalized lake/object
  index rows.
- Do not store token plaintext in raw files, params, logs, or catalog.
- Raw files link to lake files through `run_id`, `job_key`, and `raw_file_id`.
- Very large APIs may split raw into `part-000.jsonl.zst`, `part-001.jsonl.zst`,
  but the catalog still treats them as one job raw group.
- A lake file must be traceable back to raw or to an explicit raw-disabled
  endpoint policy.

## Point-in-Time Financial Design

Financial `period` is not a usable date. Backtests must not treat a report period
as if it were available on the period end date.

PIT requirements:

- Financial derived tables must join on `ann_date`, `f_ann_date`,
  `disclosure_date`, or an endpoint-specific usable-after field.
- `disclosure_date` is the preferred source when available.
- `ann_date` is the fallback usable-after field for most financial APIs.
- Derived PostgreSQL loaders and backtest snapshots must filter rows by
  `usable_after <= trade_date`.
- The current Rust strategy path uses `safe_financial_year()` as a conservative
  heuristic. It avoids many future-data leaks but is not a complete PIT model
  because it does not use actual per-company disclosure dates.

Endpoint catalog should include PIT metadata:

```yaml
pit_safety:
  requires_disclosure_date: true
  disclosure_fields:
    - ann_date
    - f_ann_date
  usable_after_field: ann_date
  period_field: end_date
```

Any endpoint with `pit_safety.requires_disclosure_date=true` cannot feed derived
strategy tables until the usable-after field is populated or an explicit fallback
policy is configured.

Derived financial layers must materialize a normalized `usable_after` column.
`usable_after` is derived from the endpoint config's `usable_after_field`. If it
is missing, the record cannot enter strategy-critical derived tables. Backtest,
snapshot, and PostgreSQL loaders use one rule: `usable_after <= trade_date`.

## Object Storage and Deduplication

Binary and large text objects use content-addressed storage.

Object path:

```text
objects/sha256/<first2>/<next2>/<sha256>.<ext>
```

Logical index path:

```text
lake/market=<market>/domain=object_index/api=<api>/publish_year=<YYYY>/publish_month=<MM>/part-000.parquet
```

Object index schema:

```text
source_id
title
publish_time
ts_code
api_name
source_url
object_path
content_type
size_bytes
sha256
fetched_at
run_id
job_key
payload_json
```

Rules:

- If the same announcement is fetched multiple times with identical `sha256`,
  store one physical object and multiple logical index rows only when metadata
  differs.
- `validate` checks that every active `object_path` exists and matches checksum
  and size.
- Missing object files fail validation and cannot be part of an active snapshot.

## Quarantine Policy

Quarantine path template:

```text
_quarantine/
  run_id=<run_id>/
    api=<api>/
      job=<job_key>/
        reason=<reason>/
```

Rules:

- Quarantined files never enter active snapshots.
- Quarantined files are cataloged with checksum, size, reason, run id, and job
  key.
- Raw responses should be retained when possible so the job can be reprocessed
  after schema approval or bug fixes.
- Manual schema approval should reprocess raw into new staged lake files. Do not
  directly promote lake files that were written under an incompatible schema.
- Backup of quarantine is optional by policy, but the catalog records their
  existence and retention deadline.
- Quarantine retention defaults to 30 days unless a file is pinned for manual
  investigation.

## Validation

Validation is part of MVP, not a later enhancement.

CLI shape:

```bash
python -m tushare_mirror validate --snapshot latest
python -m tushare_mirror validate --api daily --year 2025 --month 05
python -m tushare_mirror validate --all-active
```

Validation checks:

- File exists.
- `sha256` matches catalog.
- `size_bytes` matches catalog.
- `record_count` matches catalog.
- Parquet footer is readable.
- `schema_id` exists and is compatible with the active schema policy.
- Object path exists for object-index rows.
- Raw and lake files are traceable through `job_key` and `run_id`.
- No quarantined file is referenced by an active snapshot.

Validation writes `validation_runs` and per-file failure records. A failed
validation blocks snapshot commit.

## PostgreSQL Derived Loader

PostgreSQL is a serving layer for strategy-critical data only.

Rules:

- Loader reads only from an active snapshot.
- Loader records `snapshot_id`, `schema_id`, and `loaded_at` in every derived
  table or in a companion load metadata table.
- Loader supports rebuilding from a specific snapshot.
- Loader supports rebuilding only selected strategy-critical tables.
- PostgreSQL `tushare_sync_jobs` must not drive file-lake checkpointing.

CLI shape:

```bash
python -m tushare_mirror load-postgres --snapshot latest --table local_daily_prices
python -m tushare_mirror rebuild-derived --snapshot <snapshot_id>
```

Restore checks should be able to validate the lake and then rebuild PostgreSQL
derived tables from active snapshots.

## Backup Policy

Backup must produce a manifest and be verifiable after restore.

CLI shape:

```bash
python -m tushare_mirror backup --dry-run
python -m tushare_mirror backup --target local
python -m tushare_mirror restore-check --snapshot latest
```

Backup order:

1. Stop or pause writers for the snapshot being backed up.
2. Commit/close any catalog transaction.
3. Copy active `lake/` files for the selected snapshot.
4. Copy active `objects/` referenced by the selected snapshot.
5. Copy required `raw/` files according to retention policy. Raw can have a
   different retention class from active lake files.
6. Checkpoint and copy `_catalog/`.
7. Write `backup_manifest.json` with snapshot id, file ids, sha256 values,
   catalog checksum, started/finished timestamps, and target URI.
8. Run `restore-check` against the backup target when requested.

Raw backup classes:

- `required`: keep forever for regulatory/debug-critical APIs.
- `retained`: keep for a fixed retention window after lake validation.
- `disabled`: allowed only for endpoints explicitly marked raw-disabled.

Restore acceptance:

- Catalog opens.
- Active files exist.
- Checksums match.
- Object files exist.
- Latest or requested snapshot can be read through `LakeReader`.
- PostgreSQL derived tables can be rebuilt, or `restore-check` reports exactly
  which derived loaders are missing.

## Query Policy and LakeReader

Recommended query engines:

- PyArrow/Polars for file writing and application-local scans.
- DuckDB for ad hoc SQL over Parquet.
- PostgreSQL only for curated, indexed serving tables.

MVP writer decision: use PyArrow directly for Parquet writing. Avoid pandas as a
required layer. Existing Rust code can continue to read derived PostgreSQL tables
and may later read Parquet through Polars.

LakeReader interface:

```python
class LakeReader:
    def list_active_files(self, api_name, snapshot_id=None):
        ...

    def scan_api(self, api_name, snapshot_id=None, filters=None, columns=None):
        ...

    def scan_partition(self, api_name, partition_values, snapshot_id=None):
        ...
```

Requirements:

- Default to latest active snapshot.
- Accept explicit `snapshot_id`.
- Support column pruning.
- Support schema union by name.
- Support filters by api, partition values, and date ranges.
- Latest snapshot reads ignore superseded/compacted files unless they are
  members of the selected snapshot.
- Explicit historical snapshot reads may read superseded or compacted files if
  `snapshot_files` references them, they still exist, they are not quarantined
  or deleted, and checksum validation still passes.
- Quarantined, missing, and deleted files are never returned by normal
  LakeReader methods.
- Retention/vacuum must not physically delete files referenced by retained
  snapshots.

Common access paths:

- Single date daily market query: scan one monthly partition for the API.
- Multi-year factor research: scan selected monthly partitions with column
  pruning.
- Single stock minute query: use bucket pruning; for heavy workloads build a
  Strategy C derived minute table.
- Point-in-time financial query: filter on `usable_after <= trade_date`.
- Announcement/document search: scan object index metadata, then open object
  paths.

## Compaction Policy

Compaction is a planned catalog operation.

Rules:

- Dry-run mode must produce a compaction plan without changing files or catalog.
- Small-file compaction: more than 32 files under 16 MB in one partition.
- Large-file split: any compressed file over 1 GB, target 128-512 MB.
- New compacted files are staged, validated, and committed as a new snapshot.
- Old files become `compacted` or `superseded`; physical deletion is a separate
  retention cleanup. Historical snapshots can still read those files through
  `snapshot_files` while they are retained and valid.
- Row count and optional row hash aggregates must match before commit.

## Endpoint Catalog Organization

Do not keep all endpoints in one huge YAML file once coverage expands.

Recommended structure:

```text
_catalog/endpoints/
  stock.yaml
  financial.yaml
  index.yaml
  fund.yaml
  futures.yaml
  options.yaml
  bond.yaml
  forex.yaml
  hk.yaml
  us.yaml
  macro.yaml
  text_objects.yaml
```

A generated `endpoints.sqlite` or merged runtime view can be built from these
files during `init-catalog`.

## Testing and Acceptance

Catalog tests:

- Initialize catalog.
- Insert endpoint and partition spec.
- Write permission probe.
- Create run, job, file, schema, and snapshot.
- Switch active/superseded files.
- Preserve snapshot parent-child lineage.

FileLakeStore tests:

- Write raw JSONL.zst.
- Write Parquet.
- Write object file and object index.
- Compute `sha256` correctly.
- Compute `record_count` correctly.
- Failed staged write does not affect active snapshot.
- Same job rerun is idempotent.

Schema tests:

- Added column is accepted.
- Missing column is warned or blocked according to endpoint config.
- Field order change is accepted.
- Type widening is accepted only for configured rules.
- Incompatible type change blocks active commit or writes quarantine.
- Multiple schema versions read by union-by-name.

Permission probe tests:

- `accessible`.
- `empty_but_accessible`.
- `permission_denied`.
- `rate_limited`.
- `network_error`.
- Expired probe result requires re-probe.

Partition tests:

- Every volume class path resolver.
- Event date fallback order.
- Minute bucket path.
- Tick bucket path.
- Macro family path.
- Object publish date path.

Validation tests:

- Missing file.
- Checksum mismatch.
- Parquet footer unreadable.
- Row-count mismatch.
- Missing schema id.
- Missing object path.
- Raw/lake traceability failure.

Compaction tests:

- Small-file merge plan.
- Large-file split plan.
- Row count before and after matches.
- Old files superseded.
- New snapshot created.
- Dry-run does not modify catalog.

PostgreSQL loader tests:

- Load from snapshot.
- Record `snapshot_id` and `schema_id`.
- Rebuild selected table.
- Rebuild all strategy-critical tables.
- Verify PostgreSQL is not used as canonical checkpoint.

## Implementation Order

1. Endpoint YAML files and endpoint catalog loader.
2. Catalog schema and migrations.
3. Permission probe command and probe status machine.
4. FileLakeStore staged write protocol.
5. Raw JSONL.zst writer.
6. Lake Parquet writer.
7. Checksum and footer validation.
8. Active table/API snapshot commit.
9. Schema registry and schema evolution rules.
10. Partition resolver for all volume classes.
11. Selected endpoint MVP backfill.
12. Compaction planner.
13. PostgreSQL derived loader.
14. Backup and restore-check commands.
15. Iceberg/Delta compatibility documentation.

Do not start broad Tushare full-mirror backfill before steps 1-10 are working.

## First Phase MVP

Scope:

- Initialize catalog.
- Load endpoint config.
- Probe one endpoint.
- Staged write.
- Raw JSONL.zst.
- Lake Parquet.
- Basic schema registry.
- Active table/API snapshot.
- Checksum validation.
- LakeReader reading latest snapshot.
- One `daily` endpoint demo over a tiny date window.
- Tests for catalog, probe, writer, schema, snapshot, and validation.

Minimum CLI:

```bash
python -m tushare_mirror init-catalog
python -m tushare_mirror probe --api daily
python -m tushare_mirror fetch --api daily --params '{"trade_date":"20250102"}'
python -m tushare_mirror validate --snapshot latest
python -m tushare_mirror list-files --api daily --snapshot latest
```

Acceptance:

- Probe writes a token-hash-scoped result.
- Raw JSONL.zst is written and traceable to the job.
- Parquet is written with row metadata.
- `schema_id` is generated.
- An active snapshot is created only after validation.
- `validate --snapshot latest` passes.
- LakeReader reads latest snapshot.
- Staged write failure leaves active snapshot unchanged.
- Added schema column is compatible.
- Incompatible schema is blocked or quarantined.

## Open Decisions Resolved

1. Catalog backend: MVP uses SQLite behind `CatalogStore`. PostgreSQL is not the
   canonical catalog.
2. Active snapshot scope: support table/API snapshots and global snapshots.
3. Raw pagination: default one job one JSONL.zst; split into parts only for very
   large jobs.
4. Incompatible schema: fail active commit and write staged artifacts to
   `_quarantine/`.
5. Quarantine catalog: yes, quarantine files are cataloged and validated as
   non-active artifacts.
6. Minute default: Strategy B, day partition plus hash bucket.
7. V1 minute bucket count: default 32 for 1min, 16 for lower frequencies,
   configurable per endpoint.
8. V2 tick bucket count: default 128, increase based on file-size thresholds.
9. Parquet writer: PyArrow direct for the mirror MVP; avoid pandas dependency.
10. Row group and compression: configurable per endpoint/volume class; default
    Zstd level 3 and target row groups around 128 MB uncompressed where writer
    support allows it.
11. Object dedup: yes, content-addressed by sha256.
12. Backup manifest: yes, every backup writes a manifest.
13. Restore-check: yes, it must validate files and support rebuilding PostgreSQL
    derived tables or report missing loaders.
14. Existing `tushare_sync_jobs`: import only as legacy evidence/checkpoints;
    do not use as the file-lake source of truth.
15. Current backtest PIT risk: reduced by `safe_financial_year()` but not fully
    solved because actual disclosure dates are not used.
16. Endpoint PIT marking: yes, add `pit_safety` to endpoint catalog.
17. Endpoint YAML split: yes, split by family once beyond MVP.
18. Permission regression: keep existing active data, block new jobs, mark probe
    `access_stale`, and require re-probe/manual override.
19. Undocumented Tushare field changes: additive/reorder allowed, incompatible
    changes quarantined, suspected rename requires manual approval.
20. Iceberg compatibility: yes, reserve `table_id`, `partition_spec_id`, schema
    versions, manifest-like `files`, and snapshot lineage.
