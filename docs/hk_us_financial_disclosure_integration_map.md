# HK/US Financial Disclosure Integration Map

This map freezes the intended integration points before adding the disclosure
calendar layer. It is documentation only; it does not authorize execution,
backfill, or durable disclosure storage.

## Modules

- `tushare_mirror/disclosure.py`: disclosure dataclasses, PIT strength enum,
  match status enum, matching helpers, and validation helpers.
- `tushare_mirror/endpoint_configs/source_maps/disclosure_event_schema.yaml`:
  schema-level source of truth for disclosure event fields.
- `tushare_mirror/endpoint_configs/source_maps/financial_disclosure_sources.yaml`:
  SEC, HKEX, and future vendor source inventory.
- `tushare_mirror/pit.py`: existing PIT metadata validation. The disclosure
  layer may extend report fields, but must not weaken current raw/PIT statuses.
- `tushare_mirror/financial_reports.py`: financial readiness summaries should
  expose disclosure availability state without changing raw readiness.
- `tushare_mirror/code_period_planner.py`: raw execution gates stay separate
  from PIT feature gates.
- `scripts/tushare_real_smoke.py`: bounded SEC and Tushare cross-validation
  probes. Probe artifacts must be token-redacted and written only under `/tmp`.

## Boundaries

- HK/US disclosure work must not change `a-share-low-risk`, `hk-low-risk`, or
  `us-low-risk` executable endpoint baselines.
- HK/US financial raw planning must remain possible for raw-ready endpoints
  without requiring a disclosure match.
- `raw_only` financial data must remain blocked from feature use unless a
  disclosure event upgrades it to `availability_only` or `as_filed_verified`.
- No disclosure events are written to durable mirror or backup roots in this
  goal.
