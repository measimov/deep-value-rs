# Tushare API Gap Analysis

Last reviewed: 2026-05-23

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
- `income`
- `dividend`
- `balancesheet`
- `fina_audit`
- `daily`
- `adj_factor`
- `index_daily`

This is not a full Tushare SDK. It is a targeted implementation for the current
Deep Value A-share snapshot and early backtest workflows.

## API Coverage Matrix

| Tushare API | Local status | Local use | Gap |
| --- | --- | --- | --- |
| `trade_cal` | Implemented raw + typed | Trading calendar helper and ping | Typed table stores `exchange`, `cal_date`, `is_open`; it omits official `pretrade_date`. |
| `stock_basic` | Implemented raw + typed | Joins stock name and industry into the A-share cross section | Typed table stores only `ts_code`, `name`, `industry`, `list_status`; it omits many official fields such as `symbol`, `area`, `market`, `list_date`, `delist_date`, `is_hs`, `act_name`, and `act_ent_type`. |
| `daily_basic` | Implemented raw + typed | Market PB median, cross-section PB/PE/dividend yield, 10-year PB check | Typed table stores only selected valuation fields. No pagination or completeness guard if one request exceeds official row limits. |
| `daily` | Implemented raw + typed | Backtest stock close price | Typed table stores only `close`; it omits OHLC, previous close, change, pct change, volume, and amount. |
| `adj_factor` | Implemented raw + typed | Backtest adjusted stock price calculation | Field coverage is enough for adjustment factors, but current backtest labels the result as forward-adjusted while using `close * adj_factor`, which matches the official back-adjusted formula. Forward adjustment should be `close * adj_factor / latest_adj_factor`. |
| `index_daily` | Implemented raw + typed | Backtest benchmark close price | Typed table stores only `close`; it omits index OHLC, previous close, change, pct change, volume, and amount. |
| `income` | Partially implemented | Snapshot anomaly checks and data helpers | The per-stock calls in `main.rs` match the official `ts_code` requirement. Some batch helper functions call by `period` without `ts_code`, which does not match the official `income` contract. The official full-quarter all-stock interface is `income_vip`, which is not implemented. |
| `balancesheet` | Partially implemented | Snapshot net equity checks and data helpers | The per-stock calls in `main.rs` match the official `ts_code` requirement. Some batch helper functions call by `period` without `ts_code`, which does not match the official `balancesheet` contract. The official full-quarter all-stock interface is `balancesheet_vip`, which is not implemented. |
| `dividend` | Risky / likely mismatched | Snapshot dividend anomaly checks | Local code frequently passes `end_date` as an input parameter. Official docs list `ts_code`, `ann_date`, `record_date`, `ex_date`, and `imp_ann_date`, and require at least one of those. `end_date` is an output field, not a documented input. |
| `fina_audit` | Partially implemented | Snapshot Big Four audit check | The per-stock calls in `main.rs` match the official `ts_code` requirement. `data/audit.rs` calls by `period` without `ts_code`, which does not match the official contract. Typed table stores only `audit_agency`; it omits `ann_date`, `end_date`, `audit_result`, `audit_fees`, and `audit_sign`. |
| `pro_bar` | Not implemented | Not used | Official docs state adjusted行情 via `pro_bar` is a Python SDK dynamic calculation and cannot be called directly over HTTP. Local code should implement the equivalent adjustment formula if SDK parity is needed. |
| `income_vip` | Not implemented | Not used | Needed to pull all companies for one period without per-stock loops; official docs state it requires 5000 points. |
| `balancesheet_vip` | Not implemented | Not used | Needed to pull all companies for one period without per-stock loops; official docs state it requires 5000 points. |
| `cashflow` / `cashflow_vip` | Not implemented | Not used | Useful for quality and dividend sustainability checks, but absent from current strategy. |
| `fina_indicator` | Not implemented | Not used | Useful for ROE, margins, leverage, and dividend payout checks, but absent from current strategy. |
| `disclosure_date` | Not implemented | Not used | Could improve point-in-time financial availability instead of the current conservative `safe_financial_year()` rule. |
| `index_weight` | Not implemented | Not used | Needed for benchmark constituent analysis and index-aware portfolio comparison. |
| ST / suspension / limit-up-limit-down APIs | Not implemented | Not used | Current snapshot does not explicitly remove ST stocks, suspended stocks, or limit-up/limit-down liquidity traps through dedicated Tushare endpoints. |

## Behavioral Gaps

### Pagination and Row Limits

The client parses `has_more` in the response type, but `query()` does not act on
it. There is no generic pagination strategy using `limit` / `offset`, nor a
date-window strategy for APIs that need segmented retrieval.

This matters because several official docs specify single-request limits:

- `stock_basic`: up to 6000 rows per request.
- `daily_basic`: up to 6000 rows per request.
- `daily`: 6000 rows per request.
- `index_daily`: up to 8000 rows per request.

Current behavior may silently operate on incomplete data if Tushare truncates a
result set without the caller noticing.

### Rate Limiting and Retries

The local client does not implement:

- per-token request throttling;
- per-API request throttling;
- retry with exponential backoff;
- handling for permission/frequency errors beyond returning the Tushare error;
- request accounting for cold-cache workflows.

This is a practical issue for `snapshot` because cold-cache execution can issue
thousands of requests when financial and audit data are pulled stock by stock.

### Raw Cache vs Typed Cache

`query()` reads from raw cache and writes both raw and typed stores. However, it
does not use typed tables as an API fallback if raw cache is missing.

The typed read paths are tested and available through `PgCache::load_typed()`,
but they are not part of the primary client lookup path.

### Financial API Shape

Official `income`, `balancesheet`, and `fina_audit` are stock-oriented APIs with
`ts_code` required in the normal interface. The repository has some helper
functions that attempt period-only calls. These are not aligned with the docs.

The better implementation path is:

- use `income_vip` and `balancesheet_vip` for all-stock period data when the
  token has 5000+ points;
- otherwise keep per-stock calls but add rate limiting and request budgeting;
- avoid undocumented parameters such as `dividend(end_date=...)`.

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
- `income_vip` returned `code = 0` with data, proving at least the 5000-point
  tier.
- `us_income` returned `code = 0` with an empty result in the tested query. This
  is not enough to prove or disprove higher-tier or separate US-data permission.

Conclusion: the tested token is at least a 5000-point token. These low-frequency
probes do not reliably distinguish 5000, 10000, and 15000+ tiers because the
higher tiers mostly differ by request frequency, daily totals, and special-data
permissions. Do not use rate-limit stress tests by default because they consume
quota and can trigger throttling.

Minute data, real-time data, Hong Kong daily/minute data, and US daily data are
listed by Tushare as separate paid permissions and are outside the ordinary
points table.

## Snapshot Request Budget

Cold-cache `snapshot --top 30` can be request-heavy:

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

That is roughly 6900+ requests before cache hits. At the 2000-point tier this can
exceed the 200 requests/minute limit unless throttled. At the 5000-point tier it
is still slow and should be optimized.

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
7. ✅ `has_more` warning: `query()` and `query_force()` log a `warn!` when
   Tushare returns `has_more=true`.
8. ✅ Sync errors fail the command: `cmd_sync` exits non-zero when
   `stats.errors > 0`.

## Remaining

1. Retry/backoff for transient HTTP failures and Tushare frequency errors.
2. Full pagination support — currently `has_more` only warns; should implement
   `limit`/`offset` or date-window segmentation.
3. Correct adjusted-price naming and implement explicit `qfq` / `hfq` helpers.
4. Typed table fallback inside `TushareClient::query()` when raw cache misses.
5. ST/suspension/limit-up-limit-down filtering via dedicated Tushare endpoints.
6. `disclosure_date` integration for point-in-time financial availability
   instead of the conservative `safe_financial_year()` rule.
7. `index_weight` for benchmark constituent analysis.
8. `cashflow` / `cashflow_vip` for dividend sustainability checks.
