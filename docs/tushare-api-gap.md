# Tushare API Gap Analysis

Last reviewed: 2026-05-24

This document compares the local Tushare integration in this repository with the
official Tushare Pro HTTP/API documentation. It focuses on APIs actually used by
the current A-share snapshot and backtest code paths.

Official references:

- HTTP protocol: <https://tushare.pro/document/2?doc_id=40>
- Permission matrix: <https://tushare.pro/document/1?doc_id=108>
- Point/frequency table: <https://tushare.pro/document/1?doc_id=290>
- Stock basic: <https://tushare.pro/document/2?doc_id=25>
- Trade calendar: <https://tushare.pro/document/2?doc_id=26>
- A-share daily: <https://tushare.pro/document/2?doc_id=27>
- Adjustment factor: <https://tushare.pro/document/2?doc_id=28>
- Daily basic: <https://tushare.pro/document/2?doc_id=32>
- Income statement: <https://tushare.pro/document/2?doc_id=33>
- Balance sheet: <https://tushare.pro/document/2?doc_id=36>
- Financial audit: <https://tushare.pro/document/2?doc_id=80>
- Index daily: <https://tushare.pro/document/1?doc_id=95>
- Dividend: <https://tushare.pro/document/2?doc_id=103>
- Pro bar / adjusted price rules: <https://tushare.pro/document/2?doc_id=146>

## Current Local Implementation

The local client is a generic HTTP wrapper over Tushare Pro:

- `src/tushare/client.rs` posts JSON to `http://api.tushare.pro`.
- Request shape matches official HTTP docs: `api_name`, `token`, `params`, and
  optional comma-separated `fields`.
- Response handling expects `code`, `msg`, and `data.fields` / `data.items`.
- `query()` reads PostgreSQL raw cache first, then calls Tushare on a miss.
- Successful `query()` responses are saved into raw PostgreSQL storage.
- Supported APIs are additionally saved into typed PostgreSQL tables.
- `query_no_cache()` bypasses all local persistence.

The current typed PostgreSQL layer supports these APIs:

- `trade_cal`
- `stock_basic`
- `daily_basic`
- `income` / `income_vip`
- `dividend`
- `balancesheet` / `balancesheet_vip`
- `fina_audit`
- `fina_indicator` / `fina_indicator_vip`
- `daily`
- `adj_factor`
- `index_daily`

Two data access paths exist:
- **Online**: `TushareClient::query()` — raw cache → API fallback → typed write.
- **Offline**: `PgCache::load_typed()` via `src/data/local.rs` readers, used by
  `snapshot --local`. Typed tables are populated by `sync` commands.

This is not a full Tushare SDK. It is a targeted implementation for the current
Deep Value A-share snapshot, backtest, and sync workflows.

## API Coverage Matrix

| Tushare API | Local status | Local use | Gap |
| --- | --- | --- | --- |
| `trade_cal` | Implemented raw + typed | Trading calendar helper and ping | Typed table stores `exchange`, `cal_date`, `is_open`; it omits official `pretrade_date`. |
| `stock_basic` | Implemented raw + typed | Joins stock name, industry, list_date into A-share cross section | Typed table: `ts_code`, `name`, `industry`, `list_status`, `list_date`. Omits: `symbol`, `area`, `market`, `delist_date`, `is_hs`, `act_name`, `act_ent_type`. |
| `daily_basic` | Implemented raw + typed | Market PB median, cross-section PB/PE/dividend yield, 10-year PB check | Typed table stores only selected valuation fields. No pagination or completeness guard if one request exceeds official row limits. |
| `daily` | Implemented raw + typed | Backtest stock close price | Typed table stores only `close`; it omits OHLC, previous close, change, pct change, volume, and amount. |
| `adj_factor` | Implemented raw + typed | Backtest adjusted stock price calculation | Field coverage is enough for adjustment factors, but current backtest labels the result as forward-adjusted while using `close * adj_factor`, which matches the official back-adjusted formula. Forward adjustment should be `close * adj_factor / latest_adj_factor`. |
| `index_daily` | Implemented raw + typed | Backtest benchmark close price | Typed table stores only `close`; it omits index OHLC, previous close, change, pct change, volume, and amount. |
| `income` | Implemented (per-stock) | Snapshot anomaly checks (online path) | Per-stock calls in `main.rs` match the official `ts_code` requirement. |
| `balancesheet` | Implemented (per-stock) | Snapshot net equity checks (online path) | Per-stock calls in `main.rs` match the official `ts_code` requirement. |
| `dividend` | Fixed | Snapshot dividend anomaly checks | Fixed in 2026-05: calls now use documented `ts_code` param; `end_date` filtering is done in Rust via `sum_div_for_period()`. |
| `fina_audit` | Implemented (per-stock) | Snapshot Big Four audit check | Per-stock calls in `main.rs` match the official `ts_code` requirement. Broken `get_audit_info()` removed from `data/audit.rs`. Typed table stores `audit_agency`; omits `ann_date`, `end_date`, `audit_result`, `audit_fees`, `audit_sign`. |
| `pro_bar` | Not implemented | Not used | Official docs state adjusted行情 via `pro_bar` is a Python SDK dynamic calculation and cannot be called directly over HTTP. |
| `income_vip` | Implemented | `sync` command bulk financial pull | Requires 5000+ points. Pulls all companies for one period in a single call. Routed through `save_typed`/`load_typed` to `tushare_income`. |
| `balancesheet_vip` | Implemented | `sync` command bulk financial pull | Requires 5000+ points. Pulls all companies for one period. Routed through `save_typed`/`load_typed` to `tushare_balancesheet`. |
| `cashflow` / `cashflow_vip` | Implemented | Dividend sustainability: `n_cashflow_act` covers dividend payout | Typed table: `tushare_cashflow` (ts_code, end_date, report_type, n_cashflow_act). |
| `fina_indicator` / `fina_indicator_vip` | Implemented | `sync` command + local reader | Stores ROE, ROA, margins, leverage (13 fields). `fina_indicator_vip` pulls all stocks per period. Typed table: `tushare_fina_indicator`. Local reader: `get_fina_indicator()`. |
| `disclosure_date` | Cached, not yet integrated | Point-in-time financial availability | Typed table: `tushare_disclosure_date` (ts_code, end_date, ann_date, actual_date). Data is synced but snapshot pipeline still uses `safe_financial_year()` — integration pending. |
| `index_weight` | Not implemented | Not used | Needed for benchmark constituent analysis and index-aware portfolio comparison. |
| ST / suspension / limit-up-limit-down APIs | Not implemented | Not used | Current snapshot does not explicitly remove ST stocks, suspended stocks, or limit-up/limit-down liquidity traps through dedicated Tushare endpoints. |

## Behavioral Gaps

### Pagination and Row Limits

The client now auto-paginates via `limit`/`offset` in `execute_and_cache()`
for 10 known-supported endpoints with endpoint-specific page sizes:
`stock_basic`, `daily_basic`, `daily`, `income_vip`, `balancesheet_vip`,
`fina_indicator_vip`, `cashflow_vip`, `adj_factor`, `index_daily` (PAGE=5000),
and `disclosure_date` (PAGE=3000). Safety guards: max 20 pages and a progress
check that stops if rows don't advance between pages. Endpoints outside the
allowlist get a `warn!` if `has_more` is true but are not auto-paginated.

`query_no_cache()` still does single-page only with a `warn!` on `has_more`.

Remaining: date-window segmentation for range APIs that exceed page limits
even with offset pagination.

### Rate Limiting and Retries

Implemented:
- `RateLimiter` struct in `src/tushare/client.rs` with configurable min interval
  (via `--delay-ms` flag on the `sync` command).
- `sync` command errors fail the CLI command when `stats.errors > 0`.

Not yet implemented:
- retry with exponential backoff for transient HTTP failures;
- automatic handling for Tushare frequency-limit errors (code -2001 etc.);
- request accounting for cold-cache `snapshot` workflows.

The `sync` commands use VIP endpoints to drastically reduce API call counts
(from ~6900 per cold snapshot to ~100 for a full sync), making rate limiting
less critical for routine operation.

### Raw Cache vs Typed Cache

`query()` reads from raw cache and writes both raw and typed stores. However, it
does not use typed tables as an API fallback if raw cache is missing.

The typed read paths are tested and available through `PgCache::load_typed()`,
but they are not part of the primary client lookup path.

### Financial API Shape

Official `income`, `balancesheet`, and `fina_audit` are stock-oriented APIs with
`ts_code` required in the normal interface. Broken period-only bulk helpers were
removed from `data/financials.rs` and `data/audit.rs` in 2026-05.

Current implementation:
- `sync` command uses `income_vip`, `balancesheet_vip`, `fina_indicator_vip` for
  all-stock period data (5000+ points required).
- Online `snapshot` path uses per-stock `income`, `balancesheet`, `fina_audit`
  calls with correct `ts_code` + `period` parameters.
- `dividend` calls use only `ts_code` parameter; `end_date` filtering is done in
  Rust.

### Adjusted Price Semantics

The backtest code currently calculates:

```text
close_adj = close * adj_factor
```

Official Tushare docs define:

```text
back adjusted = close * adj_factor
forward adjusted = close * adj_factor / latest_adj_factor
```

So the current implementation is effectively back-adjusted, despite comments
that call it forward-adjusted.

## Permissions and Usage Limits

Tushare points are permission thresholds. The official docs describe them as a
gate for access, not as points consumed per request.

Important thresholds for this repository:

- `daily`: 120 points and up.
- `trade_cal`: 2000 points.
- `stock_basic`: 2000 points, 50 requests per minute.
- `daily_basic`: 2000 points and up.
- `income`: 2000 points.
- `balancesheet`: 2000 points.
- `dividend`: 2000 points.
- `fina_audit`: 2000 points.
- `adj_factor`: 2000 points and up.
- `index_daily`: 2000 points and up.
- `income_vip`: 5000 points.
- `balancesheet_vip`: 5000 points.
- `fina_indicator`: 2000 points.
- `fina_indicator_vip`: 5000 points.

Official point/frequency table:

| Points | Frequency | Daily total | Notes |
| --- | ---: | ---: | --- |
| 120 | 50 requests/min | 8000 requests/day | Only stock non-adjusted daily行情 is available. |
| 2000+ | 200 requests/min | 100000 requests/day per API | Can access APIs whose own docs require no more than this threshold. |
| 5000+ | 500 requests/min | No limit for regular data | Better fit for full strategy runs and VIP financial endpoints. |
| 10000+ | 500 requests/min | No regular-data total limit; special data 300 requests/min | Broader special-data permissions. |
| 15000+ | 500 requests/min | No total limit for special data | Special-data dedicated permission tier. |

### Observed Token Permission Probe

A user-supplied token was probed on 2026-05-23. The token value is intentionally
not stored in this repository.

Observed results:

- `daily` returned `code = 0` with data, proving at least the 120-point tier.
- `stock_basic` returned `code = 0` with data, proving at least the 2000-point
  tier.
- `income_vip` returned `code = 0` with data (6,726 rows), proving at least the
  5000-point tier.
- `balancesheet_vip` returned `code = 0` with data (7,000 rows).
- `fina_indicator_vip` returned `code = 0` with data (7,399 rows).
- `dividend` with `ts_code`-only param returned 52 records for 000001.SZ.
- `us_income` returned `code = 0` with an empty result. Not enough to prove or
  disprove separate US-data permission.

All 10 endpoints in the sync pipeline were verified on 2026-05-23. The token is
a confirmed 5000+ point token. These low-frequency
probes do not reliably distinguish 5000, 10000, and 15000+ tiers because the
higher tiers mostly differ by request frequency, daily totals, and special-data
permissions. Do not use rate-limit stress tests by default because they consume
quota and can trigger throttling.

Minute data, real-time data, Hong Kong daily/minute data, and US daily data are
listed by Tushare as separate paid permissions and are outside the ordinary
points table.

## Snapshot Request Budget

Cold-cache `snapshot --top 30` (online path, without prior sync) can be
request-heavy:

- market PB and cross-section: about 3 requests;
- 10-year PB check: about 10 `daily_basic` requests;
- financial pre-pool: up to `top_n * 10`, capped by current candidate count;
- for a 300-stock pre-pool:
  - net equity: 300 `balancesheet` requests;
  - audit: 300 `fina_audit` requests;
  - current income: 300 `income` requests;
  - current dividend: 300 `dividend` requests;
  - 10-year income: 3000 `income` requests;
  - 10-year dividend: 3000 `dividend` requests.

With the `sync` command, the full backfill breaks down as:

- Bulk (VIP / single-call): ~200 requests total
  - `stock_basic`: 1
  - `daily_basic` (anniversary dates): ~20
  - `income_vip` (per period): ~18 (all stocks each)
  - `balancesheet_vip` (per period): ~18
  - `fina_indicator_vip` (per period): ~18
  - `daily` / `adj_factor` / `index_daily`: ~120
- Per-stock (no VIP equivalents): ~11,000 requests total
  - `fina_audit` (per stock): ~5,500
  - `dividend` (per stock): ~5,500

VIP endpoints eliminate the old 3,000+ per-stock `income` and `balancesheet`
calls, and the fixed `dividend` call pattern removes 10-year nested loops. But
`fina_audit` and `dividend` remain per-stock because no VIP interfaces exist for
them, so they dominate the full-sync request budget (~11k of ~11.2k total).

The incremental `financial` mode avoids re-pulling most stocks — it only fetches
newly listed stocks' audit/dividend records, cutting per-stock calls from ~11k
to a few dozen per run.

After sync, `snapshot --local` makes zero API calls.

## Implemented (2026-05-24)

1. ✅ Rate limiter: `RateLimiter` struct with configurable min interval in
   `src/tushare/client.rs`. Used by the `sync` command (`--delay-ms` flag).
2. ✅ `dividend` calls fixed: online path now uses documented `ts_code` param
   only, filters `end_date` in Rust via `sum_div_for_period()`.
3. ✅ Broken period-only helpers removed: `get_current_year_dividend`,
   `get_10y_dividend`, and `get_audit_info` deleted from `data/` layer.
   `get_current_year_income`, `get_10y_income`, `get_net_equity` switched to
   `income_vip` / `balancesheet_vip`.
4. ✅ VIP endpoints implemented: `income_vip`, `balancesheet_vip`, and
   `fina_indicator_vip` are routed through `PgCache::save_typed()` /
   `load_typed()`, and used by the `sync` command for bulk all-stock queries.
5. ✅ Typed tables as primary local path: `snapshot --local` reads exclusively
   from typed tables via `src/data/local.rs` readers. Typed tables are not yet
   a fallback within `TushareClient::query()` (raw cache remains the sole
   read-through path there).
6. ✅ `fina_indicator` typed table: stores ROE, ROA, margins, leverage, and
   other quality metrics. Synced via `fina_indicator_vip`.
7. ✅ Pagination: auto-paginate 8 known-supported endpoints via limit/offset
   (PAGE_SIZE=5000, max 20 pages, progress guard). `query_no_cache()` warns on
   `has_more`. Endpoints not in the allowlist get a warning and single-page.
8. ✅ Sync errors fail the command: `cmd_sync` exits non-zero when
   `stats.errors > 0`.
9. ✅ `cashflow_vip`: typed table `tushare_cashflow` with `n_cashflow_act`,
   synced in full + incremental pipelines, local reader `get_cashflow()`.
10. ✅ `disclosure_date`: typed table `tushare_disclosure_date` with `ann_date`
    and `actual_date`, synced for the latest period.
11. ✅ `stock_basic.list_date`: added to typed table, sync, and local reader.

## Remaining

1. Retry/backoff for transient HTTP failures and Tushare frequency errors.
2. Date-window segmentation for range APIs that exceed page limits.
3. Correct adjusted-price naming and implement explicit `qfq` / `hfq` helpers.
4. Typed table fallback inside `TushareClient::query()` when raw cache misses.
5. ST/suspension/limit-up-limit-down filtering via dedicated Tushare endpoints.
6. Integrate `disclosure_date` into snapshot pipeline to replace conservative
   `safe_financial_year()` with actual disclosure dates.
7. `index_weight` for benchmark constituent analysis.
8. Integrate `cashflow` into anomaly detection (e.g. operating cash flow <
   dividend payout → flag as unsustainable).
