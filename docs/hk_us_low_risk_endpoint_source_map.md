# HK/US Low-risk Endpoint Source Map

This document records the Phase 1 source-discovery baseline for HK/US
low-risk mirror enablement. The machine-readable source map lives at:

- `tushare_mirror/endpoint_configs/source_maps/hk_us_low_risk.yaml`

The source map is documentation-derived metadata only. It does not enable
endpoint execution and does not add any HK/US mirror-run path.

## Official Documentation Reviewed

- HK/US menu: https://tushare.pro/document/2?doc_id=199
- HK basic: https://tushare.pro/document/2?doc_id=191
- HK trade calendar: https://tushare.pro/document/2?doc_id=250
- HK daily: https://tushare.pro/document/2?doc_id=192
- HK daily adjusted: https://tushare.pro/document/2?doc_id=339
- HK adjustment factor: https://tushare.pro/document/2?doc_id=401
- HK minute bars: https://tushare.pro/document/2?doc_id=304
- HK realtime daily K-line: https://tushare.pro/document/2?doc_id=383
- HK financial statements/indicators: https://tushare.pro/document/2?doc_id=389, https://tushare.pro/document/2?doc_id=390, https://tushare.pro/document/2?doc_id=391, https://tushare.pro/document/2?doc_id=388
- US basic: https://tushare.pro/document/2?doc_id=252
- US trade calendar: https://tushare.pro/document/2?doc_id=253
- US daily: https://tushare.pro/document/2?doc_id=254
- US daily adjusted: https://tushare.pro/document/2?doc_id=338
- US adjustment factor: https://tushare.pro/document/2?doc_id=402
- US financial statements/indicators: https://tushare.pro/document/2?doc_id=394, https://tushare.pro/document/2?doc_id=395, https://tushare.pro/document/2?doc_id=396, https://tushare.pro/document/2?doc_id=393

## Classification

Executable candidates pending bounded real probes:

- `hk_basic`
- `hk_tradecal`
- `hk_daily`
- `hk_daily_adj`
- `hk_adjfactor`
- `us_basic`
- `us_tradecal`
- `us_daily`
- `us_daily_adj`
- `us_adjfactor`

Plan-only or disabled:

- `hk_mins`: disabled, intraday/minute.
- `rt_hk_k`: disabled, realtime.
- `hk_income`, `hk_balancesheet`, `hk_cashflow`, `hk_fina_indicator`: plan-only, financial/PIT boundary.
- `us_income`, `us_balancesheet`, `us_cashflow`, `us_fina_indicator`: plan-only, financial/PIT boundary.

Pagination risks that must be resolved by bounded probes before enablement:

- `hk_daily_adj`: docs say pagination is supported, but the current input table
  does not list `offset`/`limit`.
- `us_daily`: docs mention formal-permission pagination, but the current input
  table does not list `offset`/`limit`.

## Bounded Probe Findings

The bounded probe was run on 2026-06-06T18:25:57Z and wrote the redacted
diagnostic artifact to `/tmp/tushare-hk-us-low-risk-probe.json`. The artifact is
not committed.

All executable candidates returned accessible responses and
`token_plaintext_found=false`.

Observed row counts:

- `hk_basic`: 2743 rows, 1 request.
- `hk_tradecal`: 7 rows, 1 request.
- `hk_daily`: 1 row, 1 request.
- `hk_daily_adj`: 4 rows, 2 requests; offset/limit pagination observed.
- `hk_adjfactor`: 1 row, 1 request.
- `us_basic`: 4 rows, 2 requests; offset/limit pagination observed.
- `us_tradecal`: 6 rows, 1 request.
- `us_daily`: 3 rows, 2 requests; offset/limit pagination observed.
- `us_daily_adj`: 4 rows, 2 requests; offset/limit pagination observed.
- `us_adjfactor`: 1 row, 1 request.

This probe is an interface characterization only. It is not a full HK/US pull,
does not write to the durable mirror, and does not by itself authorize
mirror-run execution.
