# PostgreSQL Migration Plan

This document tracks the staged migration from local Parquet cache files to PostgreSQL-backed Tushare storage.

## Goals

- Store all data fetched from Tushare in PostgreSQL.
- Keep the current Polars `DataFrame` API stable while migrating internals.
- Preserve raw Tushare responses before adding typed tables, so schema mistakes do not lose source data.
- Move in small, tested steps. Each completed step must update this document, pass its verification, and be committed and pushed separately.

## Database Scope

- Database credentials are loaded from local `.env` via `DATABASE_URL`.
- Real credentials must not be committed.
- Application tables live under the `deep_value` PostgreSQL schema, not `public`.
- Tests that mutate PostgreSQL should clean their own data and avoid touching other schemas.

## Tushare APIs In Scope

Current code paths use these APIs:

- `trade_cal`
- `daily_basic`
- `stock_basic`
- `income`
- `dividend`
- `balancesheet`
- `fina_audit`
- `daily`
- `adj_factor`
- `index_daily`

## Target Storage Layers

### Raw Layer

`deep_value.tushare_raw_responses` stores the original response shape:

- `cache_key`
- `api_name`
- `params`
- `requested_fields`
- `response_fields`
- `response_items`
- `row_count`
- `fetched_at`
- `updated_at`

This layer is the durable source-of-truth for fetched Tushare data.

### Typed Layer

Typed tables will be added incrementally after the raw layer works:

- `deep_value.tushare_trade_cal`
- `deep_value.tushare_stock_basic`
- `deep_value.tushare_daily_basic`
- `deep_value.tushare_income`
- `deep_value.tushare_dividend`
- `deep_value.tushare_balancesheet`
- `deep_value.tushare_fina_audit`
- `deep_value.tushare_daily`
- `deep_value.tushare_adj_factor`
- `deep_value.tushare_index_daily`

## Execution Stages

### Stage 0: Plan and Local Environment

Status: completed

Tasks:

- [x] Install Rust toolchain and verify current project builds.
- [x] Add `.env.example` placeholders for required local configuration.
- [x] Commit and push this plan document.

Verification:

- `cargo check`
- `cargo test --lib`

Completion notes:

- Added this migration plan document and documented local `DATABASE_URL` configuration placeholder.
- Verification passed: `cargo check`; `cargo test --lib` with 37 tests.

### Stage 1: Database Configuration and Health Check

Status: completed

Tasks:

- [x] Add `DATABASE_URL` to `AppConfig`.
- [x] Add PostgreSQL async dependency.
- [x] Add a small database health-check path that runs `select 1`.
- [x] Add tests for config loading and DB connectivity.

Verification:

- `cargo check`
- `cargo test --lib`
- PostgreSQL integration health check with real `.env`.

Completion notes:

- Added `sqlx` PostgreSQL support and `src/db.rs` connection helpers.
- Added `deep-value db ping` CLI health check.
- Added config unit tests and a PostgreSQL integration health-check test.
- Verification passed: `cargo check`; `cargo test --lib` with 39 tests; `cargo run -- db ping`; `cargo test --test integration_test test_postgres_health_check`.

### Stage 2: Schema Migration and Raw Table

Status: completed

Tasks:

- [x] Add idempotent migration code for `deep_value` schema.
- [x] Create `deep_value.tushare_raw_responses`.
- [x] Add indexes and upsert constraints.
- [x] Add raw cache roundtrip tests.

Verification:

- `cargo check`
- `cargo test --lib`
- PostgreSQL raw table roundtrip integration test.

Completion notes:

- Added `db::init_schema` with a transaction-scoped PostgreSQL advisory lock so concurrent tests can initialize safely.
- Added `tushare::pg_cache::PgCache` for raw response save/load/delete operations.
- Added PostgreSQL integration tests for idempotent schema initialization and raw cache roundtrip.
- Verification passed: `cargo check`; `cargo test --lib` with 39 tests; `cargo test --test postgres_cache_test` with 2 tests.

### Stage 3: Raw PostgreSQL Write Path

Status: completed

Tasks:

- [x] Write every successful `TushareClient::query` response to the raw table.
- [x] Read matching queries from PostgreSQL before calling Tushare.
- [x] Keep `query_no_cache` bypassing the store.
- [x] Convert `cache clear` to clear PostgreSQL raw cache.

Verification:

- `cargo check`
- `cargo test --lib`
- `cargo run -- ping`
- `cargo test --test integration_test`

Completion notes:

- Added PostgreSQL-backed `TushareClient::new_with_pg` and `with_pg_cache`.
- `query()` now reads/writes `deep_value.tushare_raw_responses` when a PostgreSQL store is configured, while `new()` retains the legacy Parquet fallback path.
- `query_no_cache()` still bypasses all storage.
- `cache clear` now clears PostgreSQL raw cache records.
- Verification passed: `cargo check`; `cargo test --lib` with 39 tests; `cargo test --test postgres_cache_test` with 2 tests; `cargo run -- ping`; `cargo test --test integration_test` with 6 tests; `cargo run -- cache clear`.

### Stage 4: Typed Tables Batch 1

Status: completed

Tables:

- `tushare_trade_cal`
- `tushare_stock_basic`
- `tushare_daily_basic`

Tasks:

- [x] Add schemas and upsert logic.
- [x] Add read paths that return DataFrames from typed tables.
- [x] Verify output equivalence against Tushare/raw responses.

Verification:

- `cargo check`
- `cargo test --lib`
- Targeted integration tests for the three APIs.

Completion notes:

- Added typed PostgreSQL tables for `trade_cal`, `stock_basic`, and `daily_basic`.
- Added typed upsert and DataFrame read paths in `PgCache`.
- `TushareClient::query()` now writes supported Batch 1 API responses to typed tables after raw persistence.
- Verification passed: `cargo check`; `cargo test --lib` with 39 tests; `cargo test --test postgres_typed_tables_test` with 3 tests; `cargo test --test postgres_cache_test` with 2 tests; `cargo test --test integration_test` with 6 tests.

### Stage 5: Typed Tables Batch 2

Status: pending

Tables:

- `tushare_income`
- `tushare_dividend`
- `tushare_balancesheet`
- `tushare_fina_audit`

Tasks:

- [ ] Add schemas and conservative uniqueness/upsert rules.
- [ ] Preserve duplicate-sensitive data such as dividend records.
- [ ] Verify current financial aggregation functions produce unchanged results.

Verification:

- `cargo check`
- `cargo test --lib`
- Targeted integration tests for financial APIs.

Completion notes:

- Pending.

### Stage 6: Typed Tables Batch 3

Status: pending

Tables:

- `tushare_daily`
- `tushare_adj_factor`
- `tushare_index_daily`

Tasks:

- [ ] Add schemas and upsert logic for daily prices, adjustment factors, and benchmark prices.
- [ ] Verify `fetch_ashare_prices` and `fetch_benchmark` results.

Verification:

- `cargo check`
- `cargo test --lib`
- Targeted integration tests for backtest price APIs.

Completion notes:

- Pending.

### Stage 7: Parquet Cache Retirement

Status: pending

Tasks:

- [ ] Remove Parquet cache from the primary path.
- [ ] Keep or add a migration utility only if old cache data is needed.
- [ ] Update README and CLI help.

Verification:

- `cargo check`
- `cargo test --lib`
- `cargo test --test integration_test`
- Delete local `data/cache` and verify the app still runs.

Completion notes:

- Pending.

## Commit Log

- Stage 0: plan document and environment placeholder.
- Stage 1: database configuration and PostgreSQL health check.
- Stage 2: PostgreSQL schema migration and raw Tushare response table.
- Stage 3: PostgreSQL raw read/write path for Tushare queries.
- Stage 4: typed PostgreSQL tables for trade calendar, stock basic, and daily basic.
